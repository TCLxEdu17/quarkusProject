package com.example.multitenant;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.jboss.logging.Logger;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Testcontainers resource que sobe:
 * - 3 bancos PostgreSQL (tenant_a, tenant_b, tenant_c) + tenant_default
 * - LocalStack com SQS (input-queue e output-queue)
 *
 * Injeta todas as configs necessárias no Quarkus via override de properties.
 */
public class TestContainersResource implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOG = Logger.getLogger(TestContainersResource.class);

    public static final List<String> TENANTS = List.of("tenant_a", "tenant_b", "tenant_c");
    private static final String DEFAULT_TENANT = "tenant_default";

    private PostgreSQLContainer<?> postgres;
    private LocalStackContainer localstack;
    private final Network network = Network.newNetwork();

    @Override
    public Map<String, String> start() {
        LOG.info("Starting test containers...");

        // ── PostgreSQL ──
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName(DEFAULT_TENANT)
                .withUsername("postgres")
                .withPassword("postgres")
                .withNetwork(network);
        postgres.start();

        // Cria bancos para cada tenant e roda migration
        createTenantDatabases();

        // ── LocalStack (SQS) ──
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.4"))
                .withServices(LocalStackContainer.Service.SQS)
                .withNetwork(network);
        localstack.start();

        // Cria filas SQS
        String[] queueUrls = createSqsQueues();
        String inputQueueUrl = queueUrls[0];
        String outputQueueUrl = queueUrls[1];

        LOG.infov("PostgreSQL: {0}", postgres.getJdbcUrl());
        LOG.infov("LocalStack SQS endpoint: {0}", localstack.getEndpointOverride(LocalStackContainer.Service.SQS));
        LOG.infov("Input Queue URL: {0}", inputQueueUrl);
        LOG.infov("Output Queue URL: {0}", outputQueueUrl);

        // ── Properties override ──
        Map<String, String> props = new HashMap<>();

        // DataSource default
        props.put("quarkus.datasource.jdbc.url", postgres.getJdbcUrl());
        props.put("quarkus.datasource.username", "postgres");
        props.put("quarkus.datasource.password", "postgres");

        // Tenant DB config
        props.put("app.tenant.db.host", postgres.getHost());
        props.put("app.tenant.db.port", String.valueOf(postgres.getMappedPort(5432)));
        props.put("app.tenant.db.username", "postgres");
        props.put("app.tenant.db.password", "postgres");

        // SQS config
        String sqsEndpoint = localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString();
        props.put("quarkus.sqs.endpoint-override", sqsEndpoint);
        props.put("quarkus.sqs.aws.region", "us-east-1");
        props.put("quarkus.sqs.aws.credentials.type", "static");
        props.put("quarkus.sqs.aws.credentials.static-provider.access-key-id", "test");
        props.put("quarkus.sqs.aws.credentials.static-provider.secret-access-key", "test");

        props.put("app.sqs.input-queue-url", inputQueueUrl);
        props.put("app.sqs.output-queue-url", outputQueueUrl);

        return props;
    }

    private void createTenantDatabases() {
        // Usa a JDBC completa apenas para conectar no DB default e criar os outros bancos.
        String defaultJdbc = postgres.getJdbcUrl();

        // Cria cada banco de tenant usando a conexão ao DB default
        try (Connection conn = DriverManager.getConnection(
                defaultJdbc, "postgres", "postgres")) {
            try (Statement stmt = conn.createStatement()) {
                for (String tenant : TENANTS) {
                    LOG.infov("Creating database: {0}", tenant);
                    stmt.execute("CREATE DATABASE " + tenant);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create tenant databases", e);
        }

        // Extrai parâmetros de query da JDBC (ex.: ?loggerLevel=OFF) se existirem,
        // e reconstrói a URL na forma: jdbc:postgresql://host:port/<db><query>
        String querySuffix = "";
        int qIdx = defaultJdbc.indexOf('?');
        if (qIdx != -1) {
            querySuffix = defaultJdbc.substring(qIdx);
        }

        String baseHostPort = "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432);

        // Roda a migration (criação da tabela) em cada banco
        List<String> allDbs = new java.util.ArrayList<>(TENANTS);
        allDbs.add(DEFAULT_TENANT);

        for (String db : allDbs) {
            String dbUrl = baseHostPort + "/" + db + querySuffix;
            try (Connection conn = DriverManager.getConnection(dbUrl, "postgres", "postgres")) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("""
                        CREATE TABLE IF NOT EXISTS processed_message (
                            id BIGSERIAL PRIMARY KEY,
                            message_id VARCHAR(255) NOT NULL,
                            body TEXT NOT NULL,
                            tenant_id VARCHAR(100) NOT NULL,
                            processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            thread_name VARCHAR(255)
                        )
                    """);
                    stmt.execute("CREATE INDEX IF NOT EXISTS idx_pm_tenant ON processed_message(tenant_id)");
                    stmt.execute("CREATE INDEX IF NOT EXISTS idx_pm_msgid ON processed_message(message_id)");
                    LOG.infov("Migration applied for database: {0}", db);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to apply migration for: " + db, e);
            }
        }
    }

    private String[] createSqsQueues() {
        try (SqsClient sqsClient = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build()) {

            CreateQueueResponse inputQueue = sqsClient.createQueue(b -> b.queueName("input-queue"));
            CreateQueueResponse outputQueue = sqsClient.createQueue(b -> b.queueName("output-queue"));

            // As URLs retornadas pelo LocalStack podem usar hostname interno (ex.: tc-xxx).
            // Precisamos substituir pelo endpoint acessível do host.
            String sqsEndpoint = localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString();
            String inputUrl = replaceQueueHost(inputQueue.queueUrl(), sqsEndpoint);
            String outputUrl = replaceQueueHost(outputQueue.queueUrl(), sqsEndpoint);

            return new String[]{inputUrl, outputUrl};
        }
    }

    /**
     * Substitui o host:port da URL da fila SQS pelo endpoint acessível do host.
     * Ex.: http://sqs.us-east-1.tc-xxx:4566/000000000000/input-queue
     *   -> http://127.0.0.1:59274/000000000000/input-queue
     */
    private String replaceQueueHost(String queueUrl, String sqsEndpoint) {
        // Extrai o path da queue URL (tudo após host:port)
        try {
            java.net.URI queueUri = java.net.URI.create(queueUrl);
            java.net.URI endpointUri = java.net.URI.create(sqsEndpoint);
            return new java.net.URI(
                    endpointUri.getScheme(),
                    null,
                    endpointUri.getHost(),
                    endpointUri.getPort(),
                    queueUri.getPath(),
                    queueUri.getQuery(),
                    null
            ).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to rewrite queue URL: " + queueUrl, e);
        }
    }

    @Override
    public void stop() {
        LOG.info("Stopping test containers...");
        if (localstack != null) localstack.stop();
        if (postgres != null) postgres.stop();
        network.close();
    }
}
