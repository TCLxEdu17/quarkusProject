package com.example.multitenant.service;

import com.example.multitenant.entity.ProcessedMessage;
import com.example.multitenant.messaging.SqsClientService;
import com.example.multitenant.messaging.SqsMessageDTO;
import com.example.multitenant.repository.ProcessedMessageRepository;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Serviço REATIVO que orquestra o processamento da mensagem:
 * 1. Seta o tenant no ThreadLocal
 * 2. Persiste no banco via repository bloqueante (runOnWorkerThread)
 * 3. Publica a mensagem processada na fila SQS de saída
 * 4. Limpa o TenantContext
 *
 * Ajuste: remover criação manual de virtual threads para garantir que o
 * contexto transacional e o TenantContext estejam presentes na mesma thread
 * em que o repositório é executado.
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
     * O bloco bloqueante (Hibernate ORM) roda na própria thread de subscription,
     * garantindo que @Transactional e TenantContext sejam visíveis nessa thread.
     */
    public Uni<ProcessedMessage> processMessage(SqsMessageDTO dto) {
        return Uni.createFrom().item(() -> {
                    LOG.infov("Processing message [{0}] for tenant [{1}] on thread [{2}]",
                            dto.getMessageId(), dto.getTenantId(), Thread.currentThread().getName());

                    ProcessedMessage entity = new ProcessedMessage(
                            dto.getMessageId(),
                            dto.getBody(),
                            dto.getTenantId()
                    );

                    // repository.save() agora usa SessionFactory.withOptions().tenantIdentifier()
                    // diretamente, sem depender de ThreadLocal
                    return repository.save(entity);
                })
                // Executa o supplier em um executor gerenciado pelo Mutiny
                .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                .flatMap(savedEntity -> {
                    // Após persistir, publica na fila de saída (reativo)
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
