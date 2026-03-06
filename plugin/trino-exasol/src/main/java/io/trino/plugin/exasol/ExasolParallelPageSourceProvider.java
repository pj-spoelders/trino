package io.trino.plugin.exasol;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BaseJdbcConnectorTableHandle;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcClient;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcPageSource;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ExasolParallelPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final JdbcClient jdbcClient;
    private final ExecutorService executor;
    private final String connectionUrl;

    @Inject
    public ExasolParallelPageSourceProvider(
            @ForBaseJdbc JdbcClient jdbcClient,
            @ForJdbcClient ExecutorService executor,
            BaseJdbcConfig config)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.connectionUrl = requireNonNull(config.getConnectionUrl(), "connectionUrl is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        JdbcSplit jdbcSplit = (JdbcSplit) split;
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;

        List<JdbcColumnHandle> jdbcColumns = columns.stream()
                .map(JdbcColumnHandle.class::cast)
                .collect(toImmutableList());

        BaseJdbcConnectorTableHandle constrained =
                tableHandle.intersectedWithConstraint(jdbcSplit.getDynamicFilter().transformKeys(ColumnHandle.class::cast));

        boolean parallel = ExasolSessionProperties.isParallelEnabled(session);

        if (!parallel) {
            return new JdbcPageSource(
                    jdbcClient,
                    executor,
                    session,
                    jdbcSplit,
                    constrained,
                    jdbcColumns);
        }

        return new ExasolParallelPageSource(
                jdbcClient,
                executor,
                connectionUrl,
                session,
                jdbcSplit,
                constrained,
                jdbcColumns);
    }
}
