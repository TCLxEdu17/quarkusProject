CREATE TABLE IF NOT EXISTS processed_message (
    id BIGSERIAL PRIMARY KEY,
    message_id VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    tenant_id VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    thread_name VARCHAR(255)
);

CREATE INDEX idx_processed_message_tenant ON processed_message(tenant_id);
CREATE INDEX idx_processed_message_message_id ON processed_message(message_id);

