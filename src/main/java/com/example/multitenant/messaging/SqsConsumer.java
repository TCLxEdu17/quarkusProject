package com.example.multitenant.messaging;

import com.example.multitenant.service.MessageProcessingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer SQS que faz polling da fila de entrada e processa mensagens
 * em paralelo usando transformToUniAndMerge com max concurrency de 20.
 *
 * Fluxo:
 * 1. Poll SQS (receiveMessages)
 * 2. Converte lista de Message em Multi
 * 3. transformToUniAndMerge para processar até MAX_CONCURRENCY mensagens simultâneas
 * 4. Cada mensagem: parse JSON → processMessage (seta tenant, persiste, publica) → delete da fila
 */
@ApplicationScoped
public class SqsConsumer {

    private static final Logger LOG = Logger.getLogger(SqsConsumer.class);

    private final SqsClientService sqsClientService;
    private final MessageProcessingService processingService;
    private final ObjectMapper objectMapper;

    @ConfigProperty(name = "app.sqs.max-concurrency", defaultValue = "20")
    int maxConcurrency;

    private final AtomicBoolean enabled = new AtomicBoolean(true);

    public SqsConsumer(SqsClientService sqsClientService,
                       MessageProcessingService processingService,
                       ObjectMapper objectMapper) {
        this.sqsClientService = sqsClientService;
        this.processingService = processingService;
        this.objectMapper = objectMapper;
    }

    /**
     * Polling periódico da fila SQS de entrada.
     * O Scheduled garante que não há overlap (concurrent=false por padrão).
     */
    @Scheduled(every = "${app.sqs.poll-interval:2s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public Uni<Void> pollAndProcess() {
        if (!enabled.get()) {
            return Uni.createFrom().voidItem();
        }

        return sqsClientService.receiveMessages()
                .onItem().transformToMulti(messages -> processMessages(messages))
                .toUni().replaceWithVoid()
                .onFailure().invoke(err -> LOG.errorv("Error during SQS polling: {0}", err.getMessage()));
    }

    /**
     * Processa uma lista de mensagens em paralelo com transformToUniAndMerge.
     * MAX_CONCURRENCY controla quantas mensagens são processadas simultaneamente.
     */
    public Multi<Void> processMessages(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            LOG.debug("No messages to process");
            return Multi.createFrom().empty();
        }

        LOG.infov("Processing batch of {0} messages with max concurrency {1}",
                messages.size(), maxConcurrency);

        return Multi.createFrom().iterable(messages)
                .onItem().transformToUni(message -> processSingleMessage(message)
                        .onFailure().invoke(err -> LOG.errorv(
                                "Failed to process message [{0}]: {1}",
                                message.messageId(), err.getMessage()))
                        .onFailure().recoverWithItem((Void) null)
                ).merge(maxConcurrency);
    }

    /**
     * Processa uma única mensagem SQS:
     * 1. Deserializa o body em SqsMessageDTO
     * 2. Delega ao MessageProcessingService (seta tenant, persiste, publica)
     * 3. Deleta a mensagem da fila de entrada
     */
    private Uni<Void> processSingleMessage(Message message) {
        return Uni.createFrom().item(() -> parseMessage(message))
                .flatMap(dto -> processingService.processMessage(dto))
                .flatMap(saved -> sqsClientService.deleteMessage(message.receiptHandle()))
                .invoke(() -> LOG.debugv("Deleted message [{0}] from input queue", message.messageId()));
    }

    private SqsMessageDTO parseMessage(Message message) {
        try {
            return objectMapper.readValue(message.body(), SqsMessageDTO.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse SQS message body: " + message.body(), e);
        }
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    public boolean isEnabled() {
        return enabled.get();
    }
}
