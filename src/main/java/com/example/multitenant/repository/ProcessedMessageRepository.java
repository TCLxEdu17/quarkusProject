package com.example.multitenant.repository;

import com.example.multitenant.entity.ProcessedMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Repositório que usa SessionFactory diretamente para abrir sessões
 * com tenant explícito via withOptions().tenantIdentifier().
 * Isso evita depender do ThreadLocal + @Transactional + TenantResolver,
 * que falha em contextos multi-thread com executor pools.
 */
@ApplicationScoped
public class ProcessedMessageRepository {

    private static final Logger LOG = Logger.getLogger(ProcessedMessageRepository.class);

    @Inject
    SessionFactory sessionFactory;

    public ProcessedMessage save(ProcessedMessage message) {
        String tenant = message.getTenantId();
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

    public List<ProcessedMessage> findAllByTenant(String tenantId) {
        try (Session session = sessionFactory.withOptions().tenantIdentifier(tenantId).openSession()) {
            return session
                    .createQuery("SELECT p FROM ProcessedMessage p WHERE p.tenantId = :tenantId", ProcessedMessage.class)
                    .setParameter("tenantId", tenantId)
                    .getResultList();
        }
    }

    public List<ProcessedMessage> findAll(String tenantId) {
        try (Session session = sessionFactory.withOptions().tenantIdentifier(tenantId).openSession()) {
            return session
                    .createQuery("SELECT p FROM ProcessedMessage p", ProcessedMessage.class)
                    .getResultList();
        }
    }

    public long count(String tenantId) {
        try (Session session = sessionFactory.withOptions().tenantIdentifier(tenantId).openSession()) {
            return session
                    .createQuery("SELECT COUNT(p) FROM ProcessedMessage p", Long.class)
                    .getSingleResult();
        }
    }
}
