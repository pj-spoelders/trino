package io.trino.plugin.exasol;

import com.exasol.jdbc.EXAConnection;
import com.exasol.jdbc.EXAResultSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.BaseJdbcConnectorTableHandle;
import io.trino.plugin.jdbc.BooleanReadFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DoubleReadFunction;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcErrorCode;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ReadFunction;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.Math.max;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public final class ExasolParallelPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(ExasolParallelPageSource.class);

    private static final int MAX_PARALLEL_REQUEST = 1024;
    private static final int DEFAULT_BUFFER_PAGES = 16;
    private static final CompletableFuture<Void> UNINITIALIZED_STARTUP_FUTURE = CompletableFuture.completedFuture(null);

    private final JdbcClient jdbcClient;
    private final ExecutorService executor;
    private final String connectionUrl;
    private final ConnectorSession session;
    private final JdbcSplit split;
    private final BaseJdbcConnectorTableHandle table;
    private final List<JdbcColumnHandle> columns;

    private final BlockingQueue<Page> queue;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicLong readTimeNanos = new AtomicLong();
    private final AtomicLong completedPositions = new AtomicLong();
    private final AtomicLong completedBytes = new AtomicLong();
    private final AtomicLong queuedBytes = new AtomicLong();

    private final Object blockedLock = new Object();
    private volatile CompletableFuture<Void> blocked = new CompletableFuture<>();

    private volatile Throwable failure;
    @Nullable
    private volatile CountDownLatch workersDone;

    @Nullable
    private Connection mainConn;
    @Nullable
    private PreparedStatement mainStmt;
    @Nullable
    private EXAResultSet mainResultSet;

    private final Set<Connection> workerConnections = ConcurrentHashMap.newKeySet();
    private final Set<Statement> workerStatements = ConcurrentHashMap.newKeySet();
    private final Set<ResultSet> workerResultSets = ConcurrentHashMap.newKeySet();

    private volatile CompletableFuture<Void> startupFuture = UNINITIALIZED_STARTUP_FUTURE;

    private final ReadFunction[] readFunctions;
    private final BooleanReadFunction[] booleanReadFunctions;
    private final DoubleReadFunction[] doubleReadFunctions;
    private final LongReadFunction[] longReadFunctions;
    private final SliceReadFunction[] sliceReadFunctions;
    private final ObjectReadFunction[] objectReadFunctions;

    public ExasolParallelPageSource(
            JdbcClient jdbcClient,
            ExecutorService executor,
            String connectionUrl,
            ConnectorSession session,
            JdbcSplit split,
            BaseJdbcConnectorTableHandle table,
            List<JdbcColumnHandle> columns)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.session = requireNonNull(session, "session is null");
        this.split = requireNonNull(split, "split is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = List.copyOf(columns);

        this.queue = new ArrayBlockingQueue<>(max(2, DEFAULT_BUFFER_PAGES));

        int size = columns.size();
        this.readFunctions = new ReadFunction[size];
        this.booleanReadFunctions = new BooleanReadFunction[size];
        this.doubleReadFunctions = new DoubleReadFunction[size];
        this.longReadFunctions = new LongReadFunction[size];
        this.sliceReadFunctions = new SliceReadFunction[size];
        this.objectReadFunctions = new ObjectReadFunction[size];
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public boolean isFinished()
    {
        startIfNeeded();
        return isFinishedInternal();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        startIfNeeded();

        if (startupFuture != UNINITIALIZED_STARTUP_FUTURE) {
            getFutureValue(startupFuture);
        }

        Throwable t = failure;
        if (t != null) {
            throw toTrino(t);
        }

        Page page = queue.poll();
        if (page == null) {
            return null;
        }

        queuedBytes.addAndGet(-page.getRetainedSizeInBytes());

        if (queue.isEmpty() && !isFinishedInternal()) {
            resetBlockedIfCompleted();
        }

        return SourcePage.create(page);
    }

    @Override
    public long getMemoryUsage()
    {
        return queuedBytes.get();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes.get();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions.get());
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        startIfNeeded();

        if (startupFuture != UNINITIALIZED_STARTUP_FUTURE && !startupFuture.isDone()) {
            return startupFuture;
        }

        if (!queue.isEmpty() || isFinishedInternal()) {
            return NOT_BLOCKED;
        }

        return blocked;
    }

    @Override
    public void close()
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        completeBlocked();
        queue.clear();
        queuedBytes.set(0);

        try {
            if (mainStmt != null) {
                mainStmt.cancel();
            }
        }
        catch (SQLException ignored) {
        }

        for (ResultSet rs : workerResultSets) {
            tryClose(rs);
        }
        for (Statement statement : workerStatements) {
            try {
                statement.cancel();
            }
            catch (SQLException ignored) {
            }
            tryClose(statement);
        }
        for (Connection connection : workerConnections) {
            tryClose(connection);
        }

        if (mainConn != null && mainResultSet != null) {
            try {
                jdbcClient.abortReadConnection(mainConn, mainResultSet);
            }
//            catch (RuntimeException ignored) {
//            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        tryClose(mainResultSet);
        tryClose(mainStmt);
        tryClose(mainConn);
    }

    private void startIfNeeded()
    {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        startupFuture = supplyAsync(() -> {
            startInternal();
            return null;
        }, executor);
    }

    private void startInternal()
    {
        try {
            mainConn = jdbcClient.getConnection(session, split, (JdbcTableHandle) table);

            for (int i = 0; i < columns.size(); i++) {
                JdbcColumnHandle columnHandle = columns.get(i);
                ColumnMapping columnMapping = jdbcClient.toColumnMapping(session, mainConn, columnHandle.getJdbcTypeHandle())
                        .orElseThrow(() -> new IllegalStateException("Unsupported JDBC type for column: " + columnHandle.getColumnName()));

                verify(
                        columnHandle.getColumnType().equals(columnMapping.getType()),
                        "Type mismatch: column handle has type %s but %s is mapped to %s",
                        columnHandle.getColumnType(),
                        columnHandle.getJdbcTypeHandle(),
                        columnMapping.getType());

                ReadFunction readFunction = columnMapping.getReadFunction();
                readFunctions[i] = readFunction;

                Class<?> javaType = columnMapping.getType().getJavaType();
                if (javaType == boolean.class) {
                    booleanReadFunctions[i] = (BooleanReadFunction) readFunction;
                }
                else if (javaType == double.class) {
                    doubleReadFunctions[i] = (DoubleReadFunction) readFunction;
                }
                else if (javaType == long.class) {
                    longReadFunctions[i] = (LongReadFunction) readFunction;
                }
                else if (javaType == Slice.class) {
                    sliceReadFunctions[i] = (SliceReadFunction) readFunction;
                }
                else {
                    objectReadFunctions[i] = (ObjectReadFunction) readFunction;
                }
            }

            mainStmt = jdbcClient.buildSql(session, mainConn, split, (JdbcTableHandle) table, columns);

            long start = nanoTime();
            ResultSet rs = mainStmt.executeQuery();
            readTimeNanos.addAndGet(nanoTime() - start);

            if (!(rs instanceof EXAResultSet exaResultSet)) {
                throw new IllegalStateException("Expected EXAResultSet");
            }
            mainResultSet = exaResultSet;

            EXAConnection exaConnection = (EXAConnection) mainConn;
//            if (!exaConnection.isRequestParallelConnectionsSupported()) {
//                throw new TrinoException(
//                        JdbcErrorCode.JDBC_ERROR,
//                        "Exasol database does not support RequestParallelConnections. Requires Exasol DB 8.32+.");
//            }

            int granted = exaConnection.RequestParallelConnections(MAX_PARALLEL_REQUEST);
            int workers = max(1, granted);

            long workerToken = exaConnection.GetWorkerToken();
            long sessionId = exaConnection.getSessionID();
            String[] workerHosts = exaConnection.GetAvailableWorkerHosts();

            if (workerHosts == null || workerHosts.length == 0) {
                throw new IllegalStateException("GetAvailableWorkerHosts returned no hosts");
            }
            if (workerHosts.length < workers) {
                throw new IllegalStateException("Insufficient worker hosts returned. Required: " + workers + ", available: " + workerHosts.length);
            }

            int handle = exaResultSet.GetHandle();

            log.debug("Exasol granted %s parallel workers", workers);

            workersDone = new CountDownLatch(workers);

            for (int workerId = 0; workerId < workers; workerId++) {
                int id = workerId;
                String workerHost = workerHosts[id];
                executor.execute(() -> runWorker(workerHost, id, workerToken, sessionId, handle));
            }

            resetBlockedIfCompleted();
        }
        catch (Throwable t) {
            failure = t;
            completeBlocked();
            close();
            throw toTrino(t);
        }
    }

    private void runWorker(String workerHost, int workerId, long workerToken, long sessionId, int handle)
    {
        Connection workerConnection = null;
        Statement workerStatement = null;
        ResultSet workerResultSet = null;

        try {
            workerConnection = openWorkerConnection(workerHost, workerId, workerToken, sessionId);
            workerConnections.add(workerConnection);

            workerStatement = workerConnection.createStatement();
            workerStatements.add(workerStatement);

            workerResultSet = openResultSetForHandle(workerStatement, handle);
            workerResultSets.add(workerResultSet);

            PageBuilder pageBuilder = new PageBuilder(columns.stream()
                    .map(JdbcColumnHandle::getColumnType)
                    .collect(toImmutableList()));

            while (!closed.get() && workerResultSet.next()) {
                pageBuilder.declarePosition();

                for (int i = 0; i < columns.size(); i++) {
                    int jdbcIndex = i + 1;
                    BlockBuilder output = pageBuilder.getBlockBuilder(i);
                    Type type = columns.get(i).getColumnType();

                    if (readFunctions[i].isNull(workerResultSet, jdbcIndex)) {
                        output.appendNull();
                    }
                    else if (booleanReadFunctions[i] != null) {
                        type.writeBoolean(output, booleanReadFunctions[i].readBoolean(workerResultSet, jdbcIndex));
                    }
                    else if (doubleReadFunctions[i] != null) {
                        type.writeDouble(output, doubleReadFunctions[i].readDouble(workerResultSet, jdbcIndex));
                    }
                    else if (longReadFunctions[i] != null) {
                        type.writeLong(output, longReadFunctions[i].readLong(workerResultSet, jdbcIndex));
                    }
                    else if (sliceReadFunctions[i] != null) {
                        type.writeSlice(output, sliceReadFunctions[i].readSlice(workerResultSet, jdbcIndex));
                    }
                    else {
                        type.writeObject(output, objectReadFunctions[i].readObject(workerResultSet, jdbcIndex));
                    }
                }

                if (pageBuilder.isFull()) {
                    enqueue(pageBuilder.build());
                    pageBuilder.reset();
                }
            }

            if (!pageBuilder.isEmpty()) {
                enqueue(pageBuilder.build());
            }
        }
        catch (Throwable t) {
            failure = t;
            completeBlocked();
            close();
        }
        finally {
            if (workerResultSet != null) {
                workerResultSets.remove(workerResultSet);
            }
            if (workerStatement != null) {
                workerStatements.remove(workerStatement);
            }
            if (workerConnection != null) {
                workerConnections.remove(workerConnection);
            }

            tryClose(workerResultSet);
            tryClose(workerStatement);
            tryClose(workerConnection);

            CountDownLatch latch = workersDone;
            if (latch != null) {
                latch.countDown();
            }

            if (isFinishedInternal() || failure != null || closed.get()) {
                completeBlocked();
            }
        }
    }

    private void enqueue(Page page)
            throws InterruptedException
    {
        while (!closed.get()) {
            if (queue.offer(page, 50, TimeUnit.MILLISECONDS)) {
                completedPositions.addAndGet(page.getPositionCount());
                completedBytes.addAndGet(page.getSizeInBytes());
                queuedBytes.addAndGet(page.getRetainedSizeInBytes());
                completeBlocked();
                return;
            }
        }
    }

    private boolean isFinishedInternal()
    {
        if (failure != null) {
            return true;
        }
        if (!started.get()) {
            return false;
        }
        CountDownLatch latch = workersDone;
        return latch != null && latch.getCount() == 0 && queue.isEmpty();
    }

    private Connection openWorkerConnection(String workerHost, int workerId, long workerToken, long sessionId)
            throws SQLException
    {
        String workerUrl = buildWorkerUrl(connectionUrl, workerHost, workerId, workerToken, sessionId);
        return DriverManager.getConnection(workerUrl);
    }

    /**
     * Exasol still exposes "read by handle" on driver-specific Statement implementations.
     * Keep a tiny reflective bridge here.
     */
    private static ResultSet openResultSetForHandle(Statement statement, int handle)
            throws Exception
    {
        for (String methodName : List.of(
                "ExecuteQueryByHandle",
                "executeQueryByHandle",
                "ReadHandle",
                "readHandle")) {
            try {
                Method method = statement.getClass().getMethod(methodName, int.class);
                Object result = method.invoke(statement, handle);
                if (result instanceof ResultSet resultSet) {
                    return resultSet;
                }
            }
            catch (NoSuchMethodException ignored) {
            }
        }

        throw new IllegalStateException("Could not open result set for handle");
    }

    private static String buildWorkerUrl(String baseUrl, String workerHost, int workerId, long workerToken, long sessionId)
    {
        requireNonNull(baseUrl, "baseUrl is null");
        requireNonNull(workerHost, "workerHost is null");

        final String prefix = "jdbc:exa:";
        int start = baseUrl.indexOf(prefix);
        if (start != 0) {
            throw new IllegalStateException("Unexpected Exasol JDBC URL: " + baseUrl);
        }

        int hostStart = prefix.length();
        int hostEnd = baseUrl.indexOf(';', hostStart);
        String attributes = "";
        if (hostEnd < 0) {
            hostEnd = baseUrl.length();
        }
        else {
            attributes = baseUrl.substring(hostEnd);
        }

        attributes = removeAttribute(attributes, "worker");
        attributes = removeAttribute(attributes, "workertoken");
        attributes = removeAttribute(attributes, "sessionid");

        StringBuilder url = new StringBuilder(prefix)
                .append(workerHost)
                .append(attributes)
                .append(";worker=").append(workerId)
                .append(";workertoken=").append(workerToken)
                .append(";sessionid=").append(sessionId);

        return url.toString();
    }

    private static String removeAttribute(String attributes, String key)
    {
        if (attributes == null || attributes.isEmpty()) {
            return "";
        }

        String[] parts = attributes.split(";");
        StringBuilder rebuilt = new StringBuilder();

        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }

            int equals = part.indexOf('=');
            String attributeKey = (equals >= 0 ? part.substring(0, equals) : part).trim();
            if (attributeKey.equalsIgnoreCase(key)) {
                continue;
            }

            rebuilt.append(';').append(part);
        }

        return rebuilt.toString();
    }

    private void completeBlocked()
    {
        CompletableFuture<Void> future;
        synchronized (blockedLock) {
            future = blocked;
            if (!future.isDone()) {
                future.complete(null);
            }
        }
    }

    private void resetBlockedIfCompleted()
    {
        synchronized (blockedLock) {
            if (blocked.isDone()) {
                blocked = new CompletableFuture<>();
            }
        }
    }

    private static void tryClose(@Nullable AutoCloseable closeable)
    {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        }
        catch (Exception ignored) {
        }
    }

    private static RuntimeException toTrino(Throwable throwable)
    {
        if (throwable instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new TrinoException(JDBC_ERROR, throwable);
    }
}
