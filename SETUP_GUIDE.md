# EmailPipeLine - Complete Setup Guide

## Step-by-Step Instructions to Run Your Application

---

## STEP 1: Clone the Repository

```bash
git clone https://github.com/low122/EmailPipeLine.git
cd EmailPipeLine
```

---

## STEP 2: Create Environment File (.env)

Create a `.env` file in the project root directory with the following content:

```env
# Email Configuration (IMAP)
# For Gmail: use imap.gmail.com
# For other providers: check your email provider's IMAP settings
IMAP_SERVER=imap.gmail.com
EMAIL_USER=your-email@gmail.com
EMAIL_PASSWORD=your-app-password

# Claude AI API Key
# Get your key from: https://console.anthropic.com/
CLAUDE_API_KEY=your-claude-api-key

# Redis Configuration (defaults work for Docker)
REDIS_HOST=redis
REDIS_PORT=6379

# PostgreSQL Configuration (defaults work for Docker)
DB_HOST=postgres
DB_PORT=5433
DB_NAME=email_pipeline
DB_USER=pipeline_user
DB_PASSWORD=pipeline_pass
```

### How to Get Gmail App Password:

1. Go to your Google Account settings
2. Enable 2-Factor Authentication (required)
3. Go to Security → App Passwords
4. Generate a new App Password for "Mail"
5. Copy the 16-character password and use it in `EMAIL_PASSWORD`

### How to Get Claude API Key:

1. Sign up at https://console.anthropic.com/
2. Navigate to API Keys section
3. Create a new API key
4. Copy and paste it into `CLAUDE_API_KEY`

---

## STEP 3: Start All Services with Docker Compose

This command starts all services:
- Redis (message queue for Phase 2+)
- PostgreSQL (database for Phase 1+)
- IMAP Poller (Phase 3: email scanning)
- Normalizer (Phase 4: email processing)
- Classifier (Phase 5: AI classification)
- Persister (Phase 6: database saving)

```bash
docker compose -f infra/compose.yml up -d
```

**What this does:**
- Builds Docker images for all services
- Starts containers in the correct order (waits for health checks)
- Connects all services to the same network
- Loads environment variables from `.env` file

---

## STEP 4: Verify Services Are Running

Check if all containers are running:

```bash
docker compose -f infra/compose.yml ps
```

You should see:
- ✅ redis (healthy)
- ✅ postgres (healthy)
- ✅ imap_poller (running)
- ✅ normalizer (running)
- ✅ classifier (running)
- ✅ persister (running)

---

## STEP 5: View Logs (Optional but Recommended)

Watch all services in real-time:

```bash
# View all logs
docker compose -f infra/compose.yml logs -f
```

Or view specific service logs:

```bash
# IMAP Poller (Phase 3) - see if emails are being fetched
docker compose -f infra/compose.yml logs -f imap_poller

# Normalizer (Phase 4) - see if emails are being processed
docker compose -f infra/compose.yml logs -f normalizer

# Classifier (Phase 5) - see if AI is classifying emails
docker compose -f infra/compose.yml logs -f classifier

# Persister (Phase 6) - see if data is being saved
docker compose -f infra/compose.yml logs -f persister
```

**What to look for:**
- `imap_poller`: "Connected to IMAP", "Published emails"
- `normalizer`: "Normalized email", "Published normalized email"
- `classifier`: "Classified email", "Published classified email"
- `persister`: "Saved message", "Saved classification"

---

## STEP 6: Wait for Processing

The pipeline runs automatically:
- **IMAP Poller** scans every 30 seconds
- Processes latest 100 emails per scan
- Each email goes through: Poller → Normalizer → Classifier → Persister

**Wait 1-2 minutes** for emails to be processed, then check results.

---

## STEP 7: View Results

### Option A: Use the Python Script (Recommended)

```bash
python show_subscriptions.py
```

This displays:
- All subscriptions found
- Amounts and currencies
- Confidence scores
- Total monthly cost
- Pipeline statistics

### Option B: Query Database Directly

```bash
# View all classifications
docker exec -it postgres psql -U pipeline_user -d email_pipeline -c "SELECT * FROM classifications;"

# View all messages
docker exec -it postgres psql -U pipeline_user -d email_pipeline -c "SELECT * FROM messages;"

# View subscriptions with details
docker exec -it postgres psql -U pipeline_user -d email_pipeline -c "SELECT c.vendor, c.amount_cents/100.0 as amount, c.currency, m.subject FROM classifications c JOIN messages m ON c.message_id = m.id WHERE c.vendor != '';"
```

---

## STEP 8: Stop Services (When Done)

```bash
# Stop all services
docker compose -f infra/compose.yml down

# Stop and remove volumes (deletes database data)
docker compose -f infra/compose.yml down -v
```

---

## Troubleshooting Commands

### Check if services are healthy:

```bash
docker compose -f infra/compose.yml ps
```

### Restart a specific service:

```bash
docker compose -f infra/compose.yml restart imap_poller
docker compose -f infra/compose.yml restart classifier
```

### Rebuild after code changes:

```bash
# Rebuild specific service
docker compose -f infra/compose.yml build --no-cache imap_poller
docker compose -f infra/compose.yml up -d imap_poller

# Rebuild all services
docker compose -f infra/compose.yml build --no-cache
docker compose -f infra/compose.yml up -d
```

### Check database connection:

```bash
docker exec -it postgres psql -U pipeline_user -d email_pipeline -c "\dt"
```

### Clear all data and start fresh:

```bash
# Stop and remove everything
docker compose -f infra/compose.yml down -v

# Restart
docker compose -f infra/compose.yml up -d
```

---

## Architecture Overview (From Comments)

### Phase 0-2: Infrastructure
- **Redis**: Message queue using Streams (Phase 2+)
- **PostgreSQL**: Database for storing messages and classifications (Phase 1+)
- **Docker Compose**: Orchestrates all services on a bridge network

### Phase 3: IMAP Poller
- Connects to email server via IMAP
- Polls inbox every 30 seconds
- Fetches latest 100 emails (sorted by UID, takes highest 100)
- Publishes raw emails to `raw_emails.v1` Redis Stream
- Uses base64 encoding for raw email bytes
- Builds idempotency key: `sha256(provider || mailbox_id || external_id)`

### Phase 4: Normalizer
- Consumes from `raw_emails.v1` Redis Stream
- Creates consumer group `normalizer-g`
- Decodes base64 raw email
- Parses MIME format
- Converts HTML to text (strips tags, scripts, styles)
- Strips tracking pixels and UTM parameters
- Computes body hash: `sha256(cleaned_text)` for deduplication
- Publishes to `emails.normalized.v1` Redis Stream

### Phase 5: Classifier
- Consumes from `emails.normalized.v1` Redis Stream
- Creates consumer group `classifier-g`
- Sends email content to Claude AI API
- Extracts structured data: vendor, amount_cents, currency, class, confidence
- Publishes to `emails.classified.v1` Redis Stream

### Phase 6: Persister
- Consumes from `emails.classified.v1` Redis Stream
- Creates consumer group `persister-g`
- Saves to `messages` table (idempotent using `ON CONFLICT DO UPDATE`)
- Saves to `classifications` table (linked via message_id)
- Uses database transactions for atomicity

---

## Important Notes

1. **Idempotency**: The system prevents duplicate processing using SHA256 hashes
   - `idemp_key` = `sha256(provider || mailbox_id || external_id)`
   - `body_hash` = `sha256(cleaned_email_text)`

2. **Polling Frequency**: IMAP Poller checks every 30 seconds by default

3. **Email Processing**: Latest 100 emails are processed per poll cycle

4. **API Costs**: Claude API is called for each normalized email (monitor usage)

5. **Database Updates**: Uses `ON CONFLICT DO UPDATE` to handle duplicates gracefully

6. **Consumer Groups**: Each worker uses consumer groups for parallel processing and reliability

---

## Quick Reference Commands

```bash
# Start everything
docker compose -f infra/compose.yml up -d

# View logs
docker compose -f infra/compose.yml logs -f

# View results
python show_subscriptions.py

# Stop everything
docker compose -f infra/compose.yml down

# Restart everything
docker compose -f infra/compose.yml restart
```

---

**Your application is now running!** The pipeline will automatically process emails and save subscription data to the database.

