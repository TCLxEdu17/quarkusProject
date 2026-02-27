package com.example.multitenant.tenant;

/**
 * Armazena o tenant corrente por thread usando ThreadLocal.
 * Esse é o mecanismo central para garantir thread-safety no multi-tenant.
 */
public final class TenantContext {

    private static final ThreadLocal<String> CURRENT_TENANT = new ThreadLocal<>();

    private TenantContext() {
    }

    public static void setCurrentTenant(String tenantId) {
        CURRENT_TENANT.set(tenantId);
    }

    public static String getCurrentTenant() {
        return CURRENT_TENANT.get();
    }

    public static void clear() {
        CURRENT_TENANT.remove();
    }
}

