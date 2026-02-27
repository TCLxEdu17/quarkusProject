package com.example.multitenant.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

/**
 * Cliente SQS reativo. Utiliza SqsAsyncClient para enviar e receber mensagens.
 */
@ApplicationScoped
public class SqsClientService {

    private static final Logger LOG = Logger.getLogger(SqsClientService.class);

    private final SqsAsyncClient sqsAsyncClient;
    private final ObjectMapper objectMapper;

    @ConfigProperty(name = "app.sqs.input-queue-url")
    String inputQueueUrl;

    @ConfigProperty(name = "app.sqs.output-queue-url")
    String outputQueueUrl;

    @ConfigProperty(name = "app.sqs.max-messages", defaultValue = "10")
    int maxMessages;

    @ConfigProperty(name = "app.sqs.wait-time-seconds", defaultValue = "5")
    int waitTimeSeconds;

    public SqsClientService(SqsAsyncClient sqsAsyncClient, ObjectMapper objectMapper) {
        this.sqsAsyncClient = sqsAsyncClient;
        this.objectMapper = objectMapper;
    }

    /**
     * Recebe mensagens da fila de entrada (input queue).
     */
    public Uni<List<Message>> receiveMessages() {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(inputQueueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(waitTimeSeconds)
                .build();

        return Uni.createFrom().completionStage(() -> sqsAsyncClient.receiveMessage(request))
                .map(ReceiveMessageResponse::messages)
                .invoke(msgs -> LOG.debugv("Received {0} messages from input queue", msgs.size()));
    }

    /**
     * Publica mensagem processada na fila de saída (output queue).
     */
    public Uni<SendMessageResponse> publishProcessedMessage(SqsMessageDTO dto) {
        try {
            String messageBody = objectMapper.writeValueAsString(dto);

            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(outputQueueUrl)
                    .messageBody(messageBody)
                    .messageAttributes(java.util.Map.of(
                            "tenantId", MessageAttributeValue.builder()
                                    .dataType("String")
                                    .stringValue(dto.getTenantId())
                                    .build()
                    ))
                    .build();

            return Uni.createFrom().completionStage(() -> sqsAsyncClient.sendMessage(request))
                    .invoke(resp -> LOG.infov("Published processed message [{0}] for tenant [{1}] to output queue. MessageId: {2}",
                            dto.getMessageId(), dto.getTenantId(), resp.messageId()));
        } catch (JsonProcessingException e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Deleta uma mensagem da fila de entrada após processamento.
     */
    public Uni<Void> deleteMessage(String receiptHandle) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(inputQueueUrl)
                .receiptHandle(receiptHandle)
                .build();

        return Uni.createFrom().completionStage(() -> sqsAsyncClient.deleteMessage(request))
                .replaceWithVoid();
    }

    /**
     * Envia mensagem para a fila de entrada (usado em testes e no resource).
     */
    public Uni<SendMessageResponse> sendToInputQueue(SqsMessageDTO dto) {
        try {
            String messageBody = objectMapper.writeValueAsString(dto);

            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(inputQueueUrl)
                    .messageBody(messageBody)
                    .messageAttributes(java.util.Map.of(
                            "tenantId", MessageAttributeValue.builder()
                                    .dataType("String")
                                    .stringValue(dto.getTenantId())
                                    .build()
                    ))
                    .build();

            return Uni.createFrom().completionStage(() -> sqsAsyncClient.sendMessage(request));
        } catch (JsonProcessingException e) {
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Recebe mensagens da fila de saída (usado em testes para verificar publicação).
     */
    public Uni<List<Message>> receiveFromOutputQueue() {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(outputQueueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(waitTimeSeconds)
                .messageAttributeNames("All")
                .build();

        return Uni.createFrom().completionStage(() -> sqsAsyncClient.receiveMessage(request))
                .map(ReceiveMessageResponse::messages);
    }

    public String getInputQueueUrl() {
        return inputQueueUrl;
    }

    public String getOutputQueueUrl() {
        return outputQueueUrl;
    }
}

