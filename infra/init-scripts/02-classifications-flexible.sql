-- Migration: Make classifications flexible for different user needs (watchers)
-- Run in Supabase SQL Editor (Project Settings → SQL Editor).
-- MUST columns only: message_id, class, confidence, watcher_id. Everything else in extracted_data.

-- 1) Add new columns
ALTER TABLE classifications ADD COLUMN IF NOT EXISTS watcher_id UUID;
ALTER TABLE classifications ADD COLUMN IF NOT EXISTS extracted_data JSONB DEFAULT '{}';

-- 2) Migrate existing vendor/amount_cents/currency into extracted_data (only if columns exist)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='classifications' AND column_name='vendor') THEN
    UPDATE classifications
    SET extracted_data = COALESCE(extracted_data, '{}'::jsonb) || jsonb_build_object(
      'vendor', COALESCE(vendor, ''),
      'amount_cents', COALESCE(amount_cents, 0),
      'currency', COALESCE(currency, '')
    );
  END IF;
END $$;

-- 3) Drop fixed columns (all data now in extracted_data)
ALTER TABLE classifications DROP COLUMN IF EXISTS vendor;
ALTER TABLE classifications DROP COLUMN IF EXISTS amount_cents;
ALTER TABLE classifications DROP COLUMN IF EXISTS currency;

CREATE INDEX IF NOT EXISTS idx_classifications_watcher_id ON classifications(watcher_id);
CREATE INDEX IF NOT EXISTS idx_classifications_extracted_data ON classifications USING GIN (extracted_data);
