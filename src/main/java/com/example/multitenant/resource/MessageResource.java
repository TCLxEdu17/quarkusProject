package com.example.multitenant.resource;

import com.example.multitenant.entity.ProcessedMessage;
import com.example.multitenant.messaging.SqsClientService;
import com.example.multitenant.messaging.SqsConsumer;
import com.example.multitenant.messaging.SqsMessageDTO;
import com.example.multitenant.repository.ProcessedMessageRepository;
import com.example.multitenant.tenant.TenantContext;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Resource REATIVO para interação com a mini-app.
 * Permite enviar mensagens para a fila de entrada, consultar mensagens processadas
 * e disparar manualmente o processamento.
 */
@Path("/api/messages")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MessageResource {

    private static final Logger LOG = Logger.getLogger(MessageResource.class);

    private final SqsClientService sqsClientService;
    private final SqsConsumer sqsConsumer;
    private final ProcessedMessageRepository repository;

    public MessageResource(SqsClientService sqsClientService,
                           SqsConsumer sqsConsumer,
                           ProcessedMessageRepository repository) {
        this.sqsClientService = sqsClientService;
        this.sqsConsumer = sqsConsumer;
        this.repository = repository;
    }

    /**
     * Envia uma mensagem para a fila de entrada SQS.
     */
    @POST
    @Path("/send")
    public Uni<Response> sendMessage(SqsMessageDTO dto) {
        LOG.infov("Sending message to input queue: {0}", dto);
        return sqsClientService.sendToInputQueue(dto)
                .map(resp -> Response.accepted()
                        .entity(new SendResponse(resp.messageId(), dto.getTenantId()))
                        .build());
    }

    /**
     * Envia múltiplas mensagens para a fila de entrada SQS.
     */
    @POST
    @Path("/send-batch")
    public Uni<List<SendResponse>> sendBatch(List<SqsMessageDTO> dtos) {
        LOG.infov("Sending batch of {0} messages to input queue", dtos.size());
        return Multi.createFrom().iterable(dtos)
                .onItem().transformToUniAndMerge(dto ->
                        sqsClientService.sendToInputQueue(dto)
                                .map(resp ->
                                        new SendResponse(resp.messageId(), dto.getTenantId()))
                )
                .collect().asList();
    }

    /**
     * Dispara manualmente o processamento de mensagens da fila.
     */
    @POST
    @Path("/process")
    public Uni<Response> triggerProcessing() {
        LOG.info("Manually triggering message processing");
        return sqsConsumer.pollAndProcess()
                .map(v -> Response.ok().entity("{\"status\":\"processing triggered\"}").build());
    }

    /**
     * Consulta mensagens processadas de um tenant específico.
     */
    @GET
    @Path("/processed/{tenantId}")
    public Uni<List<ProcessedMessage>> getProcessedMessages(@PathParam("tenantId") String tenantId) {
        return Uni.createFrom().item(() -> {
            TenantContext.setCurrentTenant(tenantId);
            try {
                return repository.findAllByTenant(tenantId);
            } finally {
                TenantContext.clear();
            }
        }).runSubscriptionOn(command -> Thread.ofVirtual().start(command));
    }

    /**
     * Health check simples.
     */
    @GET
    @Path("/health")
    public Uni<Response> health() {
        return Uni.createFrom().item(Response.ok("{\"status\":\"UP\"}").build());
    }

    public record SendResponse(String sqsMessageId, String tenantId) {}
}

