-- Phase 1: Database Schema with Idempotency Keys
-- This file runs automatically when Postgres container starts for the first time

-- ============================================
-- Table: messages
-- Purpose: Store raw email data with deduplication
-- ============================================

CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    idemp_key TEXT UNIQUE NOT NULL,
    mailbox_id TEXT NOT NULL, -- Tracking current email "alice@gmail.com"
    external_id TEXT NOT NULL, -- for tracking back which email <msg_002@netflix.com>
    subject TEXT,
    body_hash TEXT,
    received_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
-- ============================================
-- Indexes for messages table
-- ============================================

-- Create index on idemp_key for fast lookups
CREATE INDEX IF NOT EXISTS idx_messages_idemp_key ON messages(idemp_key);
-- Create index on mailbox_id for filtering by account
CREATE INDEX IF NOT EXISTS idx_messages_mailbox_id ON messages(mailbox_id);

-- ==============================================================================================================

-- ============================================
-- Table: classifications
-- Purpose: Store AI analysis results (subscription info)
-- ============================================

CREATE TABLE IF NOT EXISTS classifications (
    id SERIAL PRIMARY KEY,
    message_id INTEGER UNIQUE NOT NULL,
    class TEXT,                         -- "subscription", "receipt", etc.
    vendor TEXT,                        -- "Netflix", "Spotify", etc.
    amount_cents INTEGER,               -- Store as cents (1999 = $19.99)
    currency TEXT,                      -- "USD", "EUR", etc.
    confidence FLOAT,                   -- 0.0 to 1.0 (AI confidence score)
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
);
-- ============================================
-- Indexes for classifications table
-- ============================================

-- Create index on message_id for fast joins
CREATE INDEX IF NOT EXISTS idx_classifications_message_id ON classifications(message_id);