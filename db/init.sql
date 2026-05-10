CREATE TABLE IF NOT EXISTS fraud_alerts (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    merchant_category VARCHAR(128) NOT NULL,
    location VARCHAR(128) NOT NULL,
    fraud_reason VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_user_id ON fraud_alerts (user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_timestamp ON fraud_alerts (timestamp);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_category ON fraud_alerts (merchant_category);

CREATE TABLE IF NOT EXISTS validated_transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    merchant_category VARCHAR(128) NOT NULL,
    location VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_validated_transactions_user_id ON validated_transactions (user_id);
CREATE INDEX IF NOT EXISTS idx_validated_transactions_timestamp ON validated_transactions (timestamp);
