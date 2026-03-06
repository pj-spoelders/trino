package io.trino.plugin.exasol;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import jakarta.inject.Inject;
import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class ExasolSessionProperties
{
    public static final String PARALLEL = "parallel";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ExasolSessionProperties()
    {
        this.sessionProperties = ImmutableList.of(
                booleanProperty(
                        PARALLEL,
                        "Enable Exasol handle-based parallel fetch (uses maximum granted parallel connections per split)",
                        false,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isParallelEnabled(ConnectorSession session)
    {
        return session.getProperty(PARALLEL, Boolean.class);
    }
}
