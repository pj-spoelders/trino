/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exasol;

import com.exasol.jdbc.EXADriver;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import io.trino.spi.connector.ConnectorPageSourceProvider;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class ExasolClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        //“Whenever someone asks for @ForBaseJdbc JdbcClient, give them a single shared instance of ExasolClient.”
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(ExasolClient.class).in(Scopes.SINGLETON);
        //This wires JdbcStatisticsConfig so Trino can read config properties from catalog config (etc/catalog/exasol.properties) into a strongly typed config object.
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        //wiring for tablefunctions
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);

        // ✅ NEW: bind session properties holder (boolean exasol.parallel)
        //Trino connectors can define session properties (set per query / per session) like:
        //
        //SET SESSION exasol.parallel = true;
        binder.bind(ExasolSessionProperties.class).in(Scopes.SINGLETON);

        // ✅ NEW: override default JDBC page source provider with our parallel provider
        newOptionalBinder(binder, ConnectorPageSourceProvider.class)
                .setBinding()
                .to(ExasolParallelPageSourceProvider.class)
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
    {
        Properties connectionProperties = new Properties();
        // Deactivate SNAPSHOT_MODE (https://docs.exasol.com/db/latest/database_concepts/snapshot_mode.htm)
        // to ensure that {@link Connection#getMetaData()} always returns up-to-date data.
        connectionProperties.setProperty("snapshottransactions", "1");
        return DriverConnectionFactory.builder(
                        new EXADriver(),
                        config.getConnectionUrl(),
                        credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }
}
