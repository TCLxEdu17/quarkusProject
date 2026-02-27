package com.example.multitenant.tenant;

import org.eclipse.microprofile.context.spi.ThreadContextProvider;
import org.eclipse.microprofile.context.spi.ThreadContextSnapshot;

import java.util.Map;

/**
 * Provider de Context Propagation para o TenantContext.
 * Registra-se via SPI (META-INF/services) para que o SmallRye Context Propagation
 * capture e restaure automaticamente o tenant ao trocar de thread.
 *
 * Isso garante thread-safety sem precisar passar o tenantId manualmente
 * para todos os métodos.
 */
public class TenantContextProvider implements ThreadContextProvider {

    /**
     * Tipo único que identifica este contexto no MicroProfile Context Propagation.
     */
    public static final String TENANT_CONTEXT_TYPE = "tenant-context";

    @Override
    public ThreadContextSnapshot currentContext(Map<String, String> props) {
        // Captura o tenant da thread atual no momento da criação do snapshot
        String currentTenant = TenantContext.getCurrentTenant();

        return () -> {
            // Quando a tarefa vai rodar em outra thread, salva o tenant anterior
            String previousTenant = TenantContext.getCurrentTenant();

            // Restaura o tenant capturado
            if (currentTenant != null) {
                TenantContext.setCurrentTenant(currentTenant);
            } else {
                TenantContext.clear();
            }

            // Retorna um controller que restaura o estado anterior quando a tarefa termina
            return () -> {
                if (previousTenant != null) {
                    TenantContext.setCurrentTenant(previousTenant);
                } else {
                    TenantContext.clear();
                }
            };
        };
    }

    @Override
    public ThreadContextSnapshot clearedContext(Map<String, String> props) {
        return () -> {
            String previousTenant = TenantContext.getCurrentTenant();
            TenantContext.clear();

            return () -> {
                if (previousTenant != null) {
                    TenantContext.setCurrentTenant(previousTenant);
                } else {
                    TenantContext.clear();
                }
            };
        };
    }

    @Override
    public String getThreadContextType() {
        return TENANT_CONTEXT_TYPE;
    }
}
