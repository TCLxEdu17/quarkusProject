package com.example.multitenant.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO que representa a mensagem trafegada via SQS.
 * Cada mensagem carrega o tenantId para roteamento correto do banco.
 */
public class SqsMessageDTO {

    @JsonProperty("messageId")
    private String messageId;

    @JsonProperty("tenantId")
    private String tenantId;

    @JsonProperty("body")
    private String body;

    public SqsMessageDTO() {
    }

    public SqsMessageDTO(String messageId, String tenantId, String body) {
        this.messageId = messageId;
        this.tenantId = tenantId;
        this.body = body;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "SqsMessageDTO{messageId='%s', tenantId='%s', body='%s'}".formatted(messageId, tenantId, body);
    }
}

