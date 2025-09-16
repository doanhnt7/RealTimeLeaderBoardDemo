-- Create table for customer transaction metrics running data
CREATE TABLE IF NOT EXISTS customer_transaction_metrics_running (
    customer_id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(255),
    event_time TIMESTAMPTZ,
    total_amount DECIMAL(15,2),
    transaction_count INTEGER,
    average_amount DECIMAL(15,2),
    min_amount DECIMAL(15,2),
    max_amount DECIMAL(15,2),
    preferred_transaction_type VARCHAR(100),
    preferred_location VARCHAR(255),
    preferred_device VARCHAR(100),
    transaction_velocity DOUBLE PRECISION,
    risk_score DOUBLE PRECISION,
    transaction_type_counts TEXT,   -- Sink sends JSON string
    location_counts TEXT,           -- Sink sends JSON string
    device_counts TEXT,             -- Sink sends JSON string
    recent_transaction_count INTEGER,
    last_transaction_amount DECIMAL(15,2),
    last_transaction_type VARCHAR(100),
    created_at TIMESTAMPTZ,  -- Set by application for sink latency measurement
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP  -- Set by PostgreSQL for database latency measurement
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_customer_transaction_metrics_event_time 
ON customer_transaction_metrics_running(event_time);

CREATE INDEX IF NOT EXISTS idx_customer_transaction_metrics_account_id 
ON customer_transaction_metrics_running(account_id);

-- Create a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
DROP TRIGGER IF EXISTS update_customer_transaction_metrics_updated_at ON customer_transaction_metrics_running;
CREATE TRIGGER update_customer_transaction_metrics_updated_at
    BEFORE UPDATE ON customer_transaction_metrics_running
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
