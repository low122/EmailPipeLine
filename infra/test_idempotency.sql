-- Phase 1: Idempotency Test Script
-- Tests that duplicate idemp_key doesn't create duplicate rows

-- ============================================
-- Test 1: Insert first message
-- ============================================
INSERT INTO messages (idemp_key, mailbox_id, external_id, subject, body_hash, received_at)
VALUES (
  'test_key_001',
  'user@gmail.com',
  'msg_12345',
  'Netflix Subscription',
  'abc123def456',
  NOW()
);

-- ============================================
-- Test 2: Insert duplicate (should update, not insert)
-- ============================================
INSERT INTO messages (idemp_key, mailbox_id, external_id, subject, body_hash, received_at)
VALUES (
  'test_key_001',  -- SAME idemp_key as Test 1
  'user@gmail.com',
  'msg_12345',
  'Netflix Subscription',
  'abc123def456',
  NOW()
)
ON CONFLICT (idemp_key) DO UPDATE SET updated_at = NOW();

-- ============================================
-- Test 3: Verify only 1 row exists
-- ============================================
SELECT COUNT(*) as row_count FROM messages WHERE idemp_key = 'test_key_001';
-- Expected output: 1 (not 2!)

-- ============================================
-- Test 4: Insert classification linked to message
-- ============================================
INSERT INTO classifications (message_id, class, vendor, amount_cents, currency, confidence)
VALUES (
  (SELECT id FROM messages WHERE idemp_key = 'test_key_001'),
  'subscription',
  'Netflix',
  1999,  -- $19.99 in cents
  'USD',
  0.95   -- 95% confidence
);

-- ============================================
-- Test 5: Verify foreign key relationship (JOIN query)
-- ============================================
SELECT 
  m.subject,
  m.mailbox_id,
  c.vendor,
  c.amount_cents,
  c.currency,
  c.confidence
FROM messages m
JOIN classifications c ON m.id = c.message_id
WHERE m.idemp_key = 'test_key_001';