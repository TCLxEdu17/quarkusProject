package com.example.multitenant.repository;

import com.example.multitenant.entity.ProcessedMessage;
import com.example.multitenant.tenant.TenantContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Repositório que resolve o tenant automaticamente a partir do TenantContext (ThreadLocal).
 * Graças ao Context Propagation, o TenantContext é propagado entre threads automaticamente,
 * dispensando a necessidade de passar tenantId como parâmetro.
 *
 * Abre sessões com sessionFactory.withOptions().tenantIdentifier() usando o tenant
 * do contexto propagado.
 */
@ApplicationScoped
public class ProcessedMessageRepository {

    private static final Logger LOG = Logger.getLogger(ProcessedMessageRepository.class);

    @Inject
    SessionFactory sessionFactory;

    /**
     * Persiste uma mensagem no banco do tenant corrente (resolvido via TenantContext).
     */
    public ProcessedMessage save(ProcessedMessage message) {
        String tenant = resolveCurrentTenant();
        LOG.infov("Saving message [{0}] for tenant [{1}] on thread [{2}]",
                message.getMessageId(), tenant, Thread.currentThread().getName());

        try (Session session = sessionFactory.withOptions().tenantIdentifier(tenant).openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                session.persist(message);
                session.flush();
                tx.commit();
                return message;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    /**
     * Busca todas as mensagens do tenant corrente filtradas por tenantId.
     */
    public List<ProcessedMessage> findAllByTenant() {
        String tenant = resolveCurrentTenant();
        try (Session session = sessionFactory.withOptions().tenantIdentifier(tenant).openSession()) {
            return session
                    .createQuery("SELECT p FROM ProcessedMessage p WHERE p.tenantId = :tenantId", ProcessedMessage.class)
                    .setParameter("tenantId", tenant)
                    .getResultList();
        }
    }

    /**
     * Busca todas as mensagens do tenant corrente.
     */
    public List<ProcessedMessage> findAll() {
        String tenant = resolveCurrentTenant();
        try (Session session = sessionFactory.withOptions().tenantIdentifier(tenant).openSession()) {
            return session
                    .createQuery("SELECT p FROM ProcessedMessage p", ProcessedMessage.class)
                    .getResultList();
        }
    }

    /**
     * Conta as mensagens do tenant corrente.
     */
    public long count() {
        String tenant = resolveCurrentTenant();
        try (Session session = sessionFactory.withOptions().tenantIdentifier(tenant).openSession()) {
            return session
                    .createQuery("SELECT COUNT(p) FROM ProcessedMessage p", Long.class)
                    .getSingleResult();
        }
    }

    /**
     * Resolve o tenant do TenantContext propagado. Lança exceção se não houver tenant definido.
     */
    private String resolveCurrentTenant() {
        String tenant = TenantContext.getCurrentTenant();
        if (tenant == null || tenant.isBlank()) {
            throw new IllegalStateException(
                    "TenantContext não está definido na thread [" + Thread.currentThread().getName()
                            + "]. Verifique se o tenant foi setado antes de chamar o repositório.");
        }
        return tenant;
    }
}
