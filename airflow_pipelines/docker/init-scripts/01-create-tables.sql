-- Sample Source Database Schema
-- ==============================
-- Creates sample tables for testing DAGs locally

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    country VARCHAR(100),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(12, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Activity logs table
CREATE TABLE IF NOT EXISTS activity_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    event_name VARCHAR(255) NOT NULL,
    event_type VARCHAR(100),
    entity_id INTEGER,
    entity_type VARCHAR(100),
    properties JSONB,
    session_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Campaigns table
CREATE TABLE IF NOT EXISTS campaigns (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    name VARCHAR(255),
    status VARCHAR(50) DEFAULT 'draft',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Accounts table
CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    status VARCHAR(50) DEFAULT 'active',
    anonymized_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table (for e-commerce)
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    amount DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- E-commerce shops
CREATE TABLE IF NOT EXISTS ecommerce_shops (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    platform VARCHAR(100),
    enabled BOOLEAN DEFAULT true,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON activity_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_activity_logs_id ON activity_logs(id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);

-- Insert sample data
INSERT INTO users (email, name, country, status) VALUES
    ('user1@example.com', 'John Doe', 'US', 'active'),
    ('user2@example.com', 'Jane Smith', 'UK', 'active'),
    ('user3@example.com', 'Bob Wilson', 'DE', 'inactive')
ON CONFLICT (email) DO NOTHING;

INSERT INTO accounts (name, email, status) VALUES
    ('Acme Corp', 'acme@example.com', 'active'),
    ('Beta Inc', 'beta@example.com', 'active'),
    ('Gamma LLC', 'gamma@example.com', 'forgotten')
ON CONFLICT DO NOTHING;

-- Generate sample transactions
INSERT INTO transactions (user_id, amount, currency, status, created_at)
SELECT 
    (random() * 2 + 1)::int,
    (random() * 1000)::decimal(12,2),
    CASE (random() * 2)::int WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END,
    CASE (random() * 3)::int WHEN 0 THEN 'pending' WHEN 1 THEN 'completed' ELSE 'failed' END,
    NOW() - (random() * 30)::int * INTERVAL '1 day'
FROM generate_series(1, 100);

-- Generate sample activity logs
INSERT INTO activity_logs (user_id, event_name, event_type, properties, created_at)
SELECT
    (random() * 2 + 1)::int,
    CASE (random() * 4)::int 
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'button_click'
        WHEN 2 THEN 'form_submit'
        ELSE 'purchase'
    END,
    'user_action',
    jsonb_build_object('source', 'web', 'browser', 'chrome'),
    NOW() - (random() * 7)::int * INTERVAL '1 day'
FROM generate_series(1, 500);

RAISE NOTICE 'Sample data created successfully!';
