package com.example.multitenant.service;

import com.example.multitenant.entity.ProcessedMessage;
import com.example.multitenant.messaging.SqsClientService;
import com.example.multitenant.messaging.SqsMessageDTO;
import com.example.multitenant.repository.ProcessedMessageRepository;
import com.example.multitenant.tenant.TenantContext;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Serviço REATIVO que orquestra o processamento da mensagem:
 * 1. Seta o tenant no TenantContext (propagado automaticamente via Context Propagation)
 * 2. Persiste no banco via repository (que resolve o tenant do TenantContext)
 * 3. Publica a mensagem processada na fila SQS de saída
 *
 * Graças ao Context Propagation, o TenantContext é propagado automaticamente
 * entre threads, sem necessidade de passar o tenant manualmente para cada método.
 */
@ApplicationScoped
public class MessageProcessingService {

    private static final Logger LOG = Logger.getLogger(MessageProcessingService.class);

    private final ProcessedMessageRepository repository;
    private final SqsClientService sqsClientService;

    public MessageProcessingService(ProcessedMessageRepository repository, SqsClientService sqsClientService) {
        this.repository = repository;
        this.sqsClientService = sqsClientService;
    }

    /**
     * Processa uma mensagem SQS de forma reativa.
     * Seta o TenantContext no início — o Context Propagation garante que ele
     * será propagado para todas as threads subsequentes do pipeline.
     */
    public Uni<ProcessedMessage> processMessage(SqsMessageDTO dto) {
        // Seta o tenant ANTES de criar o pipeline reativo.
        // O Context Propagation captura esse valor e o propaga para as threads do executor.
        TenantContext.setCurrentTenant(dto.getTenantId());

        return Uni.createFrom().item(() -> {
                    LOG.infov("Processing message [{0}] for tenant [{1}] on thread [{2}]",
                            dto.getMessageId(), dto.getTenantId(), Thread.currentThread().getName());

                    ProcessedMessage entity = new ProcessedMessage(
                            dto.getMessageId(),
                            dto.getBody(),
                            dto.getTenantId()
                    );

                    // O repository resolve o tenant automaticamente do TenantContext propagado
                    return repository.save(entity);
                })
                .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                .flatMap(savedEntity -> {
                    SqsMessageDTO outputDto = new SqsMessageDTO(
                            savedEntity.getMessageId(),
                            savedEntity.getTenantId(),
                            savedEntity.getBody()
                    );
                    return sqsClientService.publishProcessedMessage(outputDto)
                            .replaceWith(savedEntity);
                })
                .invoke(saved -> LOG.infov("Fully processed message [{0}] for tenant [{1}]",
                        saved.getMessageId(), saved.getTenantId()));
    }
}
