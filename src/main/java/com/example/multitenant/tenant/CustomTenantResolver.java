package com.example.multitenant.tenant;

import io.quarkus.hibernate.orm.runtime.tenant.TenantResolver;
import io.quarkus.hibernate.orm.PersistenceUnitExtension;
import org.jboss.logging.Logger;

/**
 * Resolve o tenant corrente a partir do TenantContext (ThreadLocal).
 * Utilizado pelo Hibernate ORM para rotear a conexão para o banco correto.
 */
@PersistenceUnitExtension
public class CustomTenantResolver implements TenantResolver {

    private static final Logger LOG = Logger.getLogger(CustomTenantResolver.class);
    private static final String DEFAULT_TENANT = "tenant_default";

    @Override
    public String getDefaultTenantId() {
        return DEFAULT_TENANT;
    }

    @Override
    public String resolveTenantId() {
        String tenant = TenantContext.getCurrentTenant();
        if (tenant == null || tenant.isBlank()) {
            LOG.debugv("No tenant set, using default: {0}", DEFAULT_TENANT);
            return DEFAULT_TENANT;
        }
        LOG.debugv("Resolved tenant: {0}", tenant);
        return tenant;
    }
}
