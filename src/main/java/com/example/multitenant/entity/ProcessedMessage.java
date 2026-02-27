package com.example.multitenant.entity;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * Entidade que representa uma mensagem processada.
 * Cada tenant terá sua própria tabela em seu banco de dados.
 */
@Entity
@Table(name = "processed_message")
public class ProcessedMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id", nullable = false)
    private String messageId;

    @Column(name = "body", nullable = false, columnDefinition = "TEXT")
    private String body;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    @Column(name = "thread_name")
    private String threadName;

    public ProcessedMessage() {
    }

    public ProcessedMessage(String messageId, String body, String tenantId) {
        this.messageId = messageId;
        this.body = body;
        this.tenantId = tenantId;
        this.processedAt = Instant.now();
        this.threadName = Thread.currentThread().getName();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public String toString() {
        return "ProcessedMessage{id=%d, messageId='%s', tenantId='%s', threadName='%s'}"
                .formatted(id, messageId, tenantId, threadName);
    }
}

