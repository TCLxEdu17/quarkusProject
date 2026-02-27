package com.example.multitenant.config;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.quarkus.hibernate.orm.runtime.tenant.TenantConnectionResolver;
import io.quarkus.hibernate.orm.PersistenceUnitExtension;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator;

/**
 * Resolve a conexão JDBC por tenant.
 * Cada tenant aponta para um banco PostgreSQL diferente.
 * Mantém um cache de DataSources para não recriar a cada request.
 */
@PersistenceUnitExtension
public class TenantConnectionResolverImpl implements TenantConnectionResolver {

    private static final Logger LOG = Logger.getLogger(TenantConnectionResolverImpl.class);

    private final Map<String, AgroalDataSource> dataSources = new ConcurrentHashMap<>();

    @ConfigProperty(name = "app.tenant.db.host", defaultValue = "localhost")
    String dbHost;

    @ConfigProperty(name = "app.tenant.db.port", defaultValue = "5432")
    String dbPort;

    @ConfigProperty(name = "app.tenant.db.username", defaultValue = "postgres")
    String dbUsername;

    @ConfigProperty(name = "app.tenant.db.password", defaultValue = "postgres")
    String dbPassword;

    @Override
    public ConnectionProvider resolve(String tenantId) {
        LOG.debugv("Resolving connection for tenant: {0}", tenantId);
        AgroalDataSource ds = dataSources.computeIfAbsent(tenantId, this::createDataSource);
        return new ConnectionProvider() {
            @Override
            public Connection getConnection() throws SQLException {
                return ds.getConnection();
            }

            @Override
            public void closeConnection(Connection connection) throws SQLException {
                connection.close();
            }

            @Override
            public boolean supportsAggressiveRelease() {
                return false;
            }

            @Override
            public boolean isUnwrappableAs(Class<?> unwrapType) {
                return false;
            }

            @Override
            public <T> T unwrap(Class<T> unwrapType) {
                return null;
            }
        };
    }

    private AgroalDataSource createDataSource(String tenantId) {
        try {
            String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", dbHost, dbPort, tenantId);
            LOG.infov("Creating DataSource for tenant: {0}, URL: {1}", tenantId, jdbcUrl);

            AgroalDataSourceConfigurationSupplier config = new AgroalDataSourceConfigurationSupplier()
                    .connectionPoolConfiguration(cp -> cp
                            .maxSize(10)
                            .minSize(2)
                            .initialSize(2)
                            .connectionValidator(defaultValidator())
                            .connectionFactoryConfiguration(cf -> cf
                                    .jdbcUrl(jdbcUrl)
                                    .principal(new NamePrincipal(dbUsername))
                                    .credential(new SimplePassword(dbPassword))
                                    .autoCommit(true)
                            )
                    );

            return AgroalDataSource.from(config);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create DataSource for tenant: " + tenantId, e);
        }
    }

    /**
     * Exposto para testes poderem limpar caches.
     */
    public void clearCache() {
        dataSources.values().forEach(AgroalDataSource::close);
        dataSources.clear();
    }
}
