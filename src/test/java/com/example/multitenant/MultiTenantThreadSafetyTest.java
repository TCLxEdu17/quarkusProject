package com.example.multitenant;

import com.example.multitenant.entity.ProcessedMessage;
import com.example.multitenant.messaging.SqsClientService;
import com.example.multitenant.messaging.SqsConsumer;
import com.example.multitenant.messaging.SqsMessageDTO;
import com.example.multitenant.repository.ProcessedMessageRepository;
import com.example.multitenant.tenant.TenantContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Teste de integração multi-thread que valida:
 *
 * 1. Thread-safety: múltiplas threads processando mensagens de tenants diferentes
 *    simultaneamente NÃO misturam dados entre bancos.
 * 2. Cada mensagem é persistida no banco CORRETO (do seu tenant).
 * 3. Cada mensagem processada é publicada na fila de saída com o tenant correto.
 * 4. Nenhum tenant é "perdido" entre threads.
 *
 * Usa Testcontainers com:
 * - PostgreSQL (3 bancos: tenant_a, tenant_b, tenant_c)
 * - LocalStack (SQS: input-queue, output-queue)
 */
@QuarkusTest
@QuarkusTestResource(TestContainersResource.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MultiTenantThreadSafetyTest {

    private static final Logger LOG = Logger.getLogger(MultiTenantThreadSafetyTest.class);

    private static final List<String> TENANTS = List.of("tenant_a", "tenant_b", "tenant_c");
    private static final int MESSAGES_PER_TENANT = 30;
    private static final int TOTAL_MESSAGES = TENANTS.size() * MESSAGES_PER_TENANT; // 90

    @Inject
    SqsClientService sqsClientService;

    @Inject
    SqsConsumer sqsConsumer;

    @Inject
    ProcessedMessageRepository repository;

    @Inject
    ObjectMapper objectMapper;

    @ConfigProperty(name = "app.tenant.db.host")
    String tenantDbHost;

    @ConfigProperty(name = "app.tenant.db.port")
    int tenantDbPort;

    @ConfigProperty(name = "app.tenant.db.username")
    String tenantDbUser;

    @ConfigProperty(name = "app.tenant.db.password")
    String tenantDbPass;

    @BeforeEach
    void setup() {
        // Desabilita o scheduler automático para controle manual
        sqsConsumer.setEnabled(false);
    }

    /**
     * TESTE PRINCIPAL: envia 90 mensagens (30 por tenant) para a fila de entrada,
     * processa todas em paralelo com transformToUniAndMerge (max concurrency 20),
     * e valida que cada tenant recebeu suas 30 mensagens no banco correto e na fila de saída.
     */
    @Test
    @Order(1)
    void shouldProcessMessagesForCorrectTenantAcrossMultipleThreads() throws Exception {
        LOG.info("=== STARTING MULTI-TENANT THREAD-SAFETY TEST ===");
        LOG.infov("Tenants: {0}, Messages per tenant: {1}, Total: {2}",
                TENANTS, MESSAGES_PER_TENANT, TOTAL_MESSAGES);

        // ── STEP 1: Envia todas as mensagens para a fila de entrada ──
        LOG.info("STEP 1: Sending messages to input queue...");
        List<SqsMessageDTO> allMessages = new ArrayList<>();

        for (String tenant : TENANTS) {
            for (int i = 0; i < MESSAGES_PER_TENANT; i++) {
                SqsMessageDTO dto = new SqsMessageDTO(
                        "msg-" + tenant + "-" + i,
                        tenant,
                        "Body for " + tenant + " message " + i
                );
                allMessages.add(dto);
            }
        }

        // Envia todas em paralelo
        int sent = 0;
        for (SqsMessageDTO dto : allMessages) {
            try {
                sqsClientService.sendToInputQueue(dto).await().atMost(Duration.ofSeconds(5));
                sent++;
            } catch (Exception e) {
                LOG.errorv("Failed to send message {0}: {1}", dto.getMessageId(), e.getMessage());
            }
        }

        LOG.infov("Sent {0} messages to input queue", sent);
        assertEquals(TOTAL_MESSAGES, sent, "All messages should be sent");

        // ── STEP 2: Processa todas as mensagens via polling ──
        LOG.info("STEP 2: Processing messages from input queue...");
        AtomicInteger totalProcessed = new AtomicInteger(0);
        Set<String> threadsUsed = ConcurrentHashMap.newKeySet();

        // Polling loop até processar tudo (máximo 120s)
        await().atMost(Duration.ofSeconds(120))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    // Recebe batch da fila
                    List<Message> messages = sqsClientService.receiveMessages()
                            .await().atMost(Duration.ofSeconds(10));

                    if (messages != null && !messages.isEmpty()) {
                        LOG.infov("Received batch of {0} messages, processing...", messages.size());

                        // Processa com transformToUniAndMerge
                        sqsConsumer.processMessages(messages)
                                .onItem().invoke(() -> {
                                    totalProcessed.incrementAndGet();
                                    threadsUsed.add(Thread.currentThread().getName());
                                })
                                .collect().asList()
                                .await().atMost(Duration.ofSeconds(30));
                    }

                    // Verifica contagem em cada banco
                    int totalInDbs = 0;
                    for (String tenant : TENANTS) {
                        int count = countMessagesInTenantDb(tenant);
                        totalInDbs += count;
                    }
                    LOG.infov("Total messages in all DBs: {0}/{1}", totalInDbs, TOTAL_MESSAGES);
                    assertEquals(TOTAL_MESSAGES, totalInDbs,
                            "All messages should be persisted across all tenant databases");
                });

        LOG.infov("Threads used during processing: {0}", threadsUsed);

        // ── STEP 3: Valida que cada tenant tem EXATAMENTE suas mensagens no banco correto ──
        LOG.info("STEP 3: Validating per-tenant database integrity...");

        for (String tenant : TENANTS) {
            List<ProcessedMessage> tenantMessages = findMessagesInTenantDb(tenant);

            // Cada tenant deve ter exatamente MESSAGES_PER_TENANT mensagens
            assertEquals(MESSAGES_PER_TENANT, tenantMessages.size(),
                    "Tenant [" + tenant + "] should have exactly " + MESSAGES_PER_TENANT + " messages");

            // Todas as mensagens devem pertencer ao tenant correto
            for (ProcessedMessage pm : tenantMessages) {
                assertEquals(tenant, pm.getTenantId(),
                        "Message [" + pm.getMessageId() + "] should belong to tenant [" + tenant
                                + "] but found tenantId=[" + pm.getTenantId() + "]");

                assertTrue(pm.getMessageId().startsWith("msg-" + tenant),
                        "Message ID [" + pm.getMessageId() + "] should start with 'msg-" + tenant + "'");

                assertNotNull(pm.getProcessedAt(), "processedAt should not be null");
                assertNotNull(pm.getThreadName(), "threadName should not be null");
            }

            // Verifica que não há mensagens de OUTRO tenant neste banco
            Set<String> uniqueTenants = tenantMessages.stream()
                    .map(ProcessedMessage::getTenantId)
                    .collect(Collectors.toSet());
            assertEquals(1, uniqueTenants.size(),
                    "Only one tenant should exist in database [" + tenant + "]: " + uniqueTenants);
            assertTrue(uniqueTenants.contains(tenant),
                    "Database [" + tenant + "] should only contain messages for [" + tenant + "]");

            LOG.infov("✓ Tenant [{0}]: {1} messages validated OK", tenant, tenantMessages.size());
        }

        // ── STEP 4: Valida mensagens publicadas na fila de saída ──
        LOG.info("STEP 4: Validating output queue messages...");
        Map<String, List<SqsMessageDTO>> outputByTenant = new HashMap<>();

        // Drena todas as mensagens da fila de saída
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    List<Message> outputMsgs = sqsClientService.receiveFromOutputQueue()
                            .await().atMost(Duration.ofSeconds(5));

                    for (Message msg : outputMsgs) {
                        SqsMessageDTO dto = objectMapper.readValue(msg.body(), SqsMessageDTO.class);
                        outputByTenant.computeIfAbsent(dto.getTenantId(), k -> new ArrayList<>()).add(dto);
                    }

                    int totalOutput = outputByTenant.values().stream().mapToInt(List::size).sum();
                    LOG.infov("Output queue drained so far: {0}/{1}", totalOutput, TOTAL_MESSAGES);
                    return totalOutput >= TOTAL_MESSAGES;
                });

        // Valida que cada tenant tem suas mensagens na fila de saída
        for (String tenant : TENANTS) {
            List<SqsMessageDTO> outputMsgs = outputByTenant.getOrDefault(tenant, List.of());
            assertEquals(MESSAGES_PER_TENANT, outputMsgs.size(),
                    "Output queue should have " + MESSAGES_PER_TENANT + " messages for tenant [" + tenant + "]");

            for (SqsMessageDTO dto : outputMsgs) {
                assertEquals(tenant, dto.getTenantId(),
                        "Output message should have correct tenantId");
                assertTrue(dto.getMessageId().startsWith("msg-" + tenant),
                        "Output messageId should match tenant");
            }

            LOG.infov("✓ Output queue tenant [{0}]: {1} messages validated OK", tenant, outputMsgs.size());
        }

        // ── STEP 5: Verifica que nenhum tenant estranho apareceu ──
        LOG.info("STEP 5: Verifying no cross-tenant contamination...");
        Set<String> allTenantsInOutput = outputByTenant.keySet();
        assertEquals(new HashSet<>(TENANTS), allTenantsInOutput,
                "Only expected tenants should appear in output");

        LOG.info("=== MULTI-TENANT THREAD-SAFETY TEST PASSED ===");
        LOG.infov("Total messages processed: {0}", TOTAL_MESSAGES);
        LOG.infov("Threads used: {0}", threadsUsed.size());
    }

    /**
     * TESTE DE STRESS: envia mensagens intercaladas de tenants diferentes
     * para maximizar a chance de race conditions.
     */
    @Test
    @Order(2)
    void shouldHandleInterleavedTenantMessagesWithoutMixup() throws Exception {
        LOG.info("=== STARTING INTERLEAVED TENANT TEST ===");

        int messagesPerTenant = 10;
        List<SqsMessageDTO> interleavedMessages = new ArrayList<>();

        // Intercala mensagens: tenant_a, tenant_b, tenant_c, tenant_a, tenant_b, tenant_c, ...
        for (int i = 0; i < messagesPerTenant; i++) {
            for (String tenant : TENANTS) {
                interleavedMessages.add(new SqsMessageDTO(
                        "interleaved-" + tenant + "-" + i,
                        tenant,
                        "Interleaved body " + tenant + " " + i
                ));
            }
        }

        // Envia todas
        for (SqsMessageDTO dto : interleavedMessages) {
            sqsClientService.sendToInputQueue(dto).await().atMost(Duration.ofSeconds(5));
        }

        int expectedTotal = messagesPerTenant * TENANTS.size();

        // Processa
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    List<Message> messages = sqsClientService.receiveMessages()
                            .await().atMost(Duration.ofSeconds(5));

                    if (messages != null && !messages.isEmpty()) {
                        sqsConsumer.processMessages(messages)
                                .collect().asList()
                                .await().atMost(Duration.ofSeconds(15));
                    }

                    // Conta total em todos os bancos (inclui mensagens do teste anterior)
                    for (String tenant : TENANTS) {
                        List<ProcessedMessage> msgs = findMessagesInTenantDb(tenant);
                        // Filtra só as deste teste
                        long interleavedCount = msgs.stream()
                                .filter(m -> m.getMessageId().startsWith("interleaved-" + tenant))
                                .count();
                        assertEquals(messagesPerTenant, (int) interleavedCount,
                                "Tenant [" + tenant + "] should have " + messagesPerTenant + " interleaved messages");
                    }
                });

        // Valida integridade: nenhuma mensagem de tenant errado
        for (String tenant : TENANTS) {
            List<ProcessedMessage> msgs = findMessagesInTenantDb(tenant);
            for (ProcessedMessage pm : msgs) {
                assertEquals(tenant, pm.getTenantId(),
                        "Cross-tenant contamination detected! Message [" + pm.getMessageId()
                                + "] has tenantId [" + pm.getTenantId()
                                + "] but is in database [" + tenant + "]");
            }
        }

        LOG.info("=== INTERLEAVED TENANT TEST PASSED ===");
    }

    /**
     * TESTE DE CONCORRÊNCIA MÁXIMA: dispara processamento de múltiplas threads Java
     * simultaneamente para forçar race conditions no TenantContext.
     */
    @Test
    @Order(3)
    void shouldMaintainTenantIsolationUnderHighConcurrency() throws Exception {
        LOG.info("=== STARTING HIGH CONCURRENCY TEST ===");

        int messagesPerTenant = 20;
        List<SqsMessageDTO> allMessages = new ArrayList<>();

        for (String tenant : TENANTS) {
            for (int i = 0; i < messagesPerTenant; i++) {
                allMessages.add(new SqsMessageDTO(
                        "concurrent-" + tenant + "-" + i,
                        tenant,
                        "Concurrent body " + tenant + " " + i
                ));
            }
        }

        // Shuffle para máxima mistura de tenants
        Collections.shuffle(allMessages);

        // Envia todas
        for (SqsMessageDTO dto : allMessages) {
            sqsClientService.sendToInputQueue(dto).await().atMost(Duration.ofSeconds(5));
        }

        // Processa tudo de uma vez com alta concorrência
        await().atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    List<Message> messages = sqsClientService.receiveMessages()
                            .await().atMost(Duration.ofSeconds(5));

                    if (messages != null && !messages.isEmpty()) {
                        sqsConsumer.processMessages(messages)
                                .collect().asList()
                                .await().atMost(Duration.ofSeconds(20));
                    }

                    for (String tenant : TENANTS) {
                        List<ProcessedMessage> msgs = findMessagesInTenantDb(tenant);
                        long concurrentCount = msgs.stream()
                                .filter(m -> m.getMessageId().startsWith("concurrent-" + tenant))
                                .count();
                        assertEquals(messagesPerTenant, (int) concurrentCount,
                                "Tenant [" + tenant + "] should have " + messagesPerTenant + " concurrent messages");
                    }
                });

        // Valida ZERO cross-tenant contamination
        for (String tenant : TENANTS) {
            List<ProcessedMessage> msgs = findMessagesInTenantDb(tenant);
            Set<String> tenantIds = msgs.stream()
                    .map(ProcessedMessage::getTenantId)
                    .collect(Collectors.toSet());

            assertEquals(Set.of(tenant), tenantIds,
                    "Database [" + tenant + "] should ONLY contain tenant [" + tenant
                            + "] but found: " + tenantIds);

            // Verifica que nenhum messageId de outro tenant acabou neste banco
            for (ProcessedMessage pm : msgs) {
                assertFalse(
                        TENANTS.stream()
                                .filter(t -> !t.equals(tenant))
                                .anyMatch(otherTenant -> pm.getMessageId().contains(otherTenant)),
                        "Message [" + pm.getMessageId() + "] from another tenant found in database [" + tenant + "]");
            }
        }

        LOG.info("=== HIGH CONCURRENCY TEST PASSED ===");
    }

    // ── Helper methods ──

    private int countMessagesInTenantDb(String tenantId) {
        String url = String.format("jdbc:postgresql://%s:%d/%s", tenantDbHost, tenantDbPort, tenantId);
        try (Connection conn = DriverManager.getConnection(url, tenantDbUser, tenantDbPass);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM processed_message")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } catch (Exception e) {
            throw new RuntimeException("Failed to count messages in tenant DB " + tenantId, e);
        }
    }

    private List<ProcessedMessage> findMessagesInTenantDb(String tenantId) {
        String url = String.format("jdbc:postgresql://%s:%d/%s", tenantDbHost, tenantDbPort, tenantId);
        List<ProcessedMessage> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, tenantDbUser, tenantDbPass);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT message_id, body, tenant_id, processed_at, thread_name FROM processed_message")) {
            while (rs.next()) {
                ProcessedMessage pm = new ProcessedMessage(
                        rs.getString("message_id"),
                        rs.getString("body"),
                        rs.getString("tenant_id")
                );
                pm.setProcessedAt(rs.getTimestamp("processed_at").toInstant());
                pm.setThreadName(rs.getString("thread_name"));
                result.add(pm);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read messages from tenant DB " + tenantId, e);
        }
    }
}
