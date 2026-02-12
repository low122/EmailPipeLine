# EmailPipeLine

An event-driven email processing pipeline that scans inboxes, normalizes emails, classifies subscriptions using AI, and persists data. Uses Redis Streams and runs **without Docker** — install Redis and run the Python services locally.

## Architecture

- **Phase 3**: IMAP Poller — scans inbox, publishes to `raw_emails.v1`
- **Phase 4**: Normalizer — parses MIME, cleans text, publishes to `emails.normalized.v1`
- **Phase 5**: Classifier — Claude AI extracts subscription details, publishes to `emails.classified.v1`
- **Phase 6**: Persister — saves to database (Supabase Postgres)
- **Watcher** — semantic filter: matches emails to user-defined watchers (Supabase + Voyage embeddings)

Services communicate via **Redis Streams** with consumer groups.

## Prerequisites

- **Redis** (e.g. `brew install redis` then `redis-server`, or [redis.io](https://redis.io))
- **Python 3.10+**
- **Supabase** project (database + optional watchers/embeddings)
- Gmail (or IMAP) App Password, Claude API key, optional Voyage API key for watcher

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/low122/EmailPipeLine.git
cd EmailPipeLine
python -m venv venv
source venv/bin/activate   # or: venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Install per-worker deps if you use them (e.g. watcher: `pip install supabase voyageai`; persister uses `psycopg2-binary` from root requirements).

### 2. Environment

```bash
cp .env.example .env
# Edit .env: IMAP_*, REDIS_HOST=localhost, DB_* (Supabase), SUPABASE_*, CLAUDE_API_KEY, VOYAGE_API_KEY if using watcher
```

### 3. Database (Supabase)

Run the schema in your Supabase project (SQL Editor): use `infra/init.sql` or `infra/init-scripts/01-schema.sql` to create `messages` and `classifications`. Create `watchers` and `email_embeddings` (and `match_watchers` RPC) if you use the watcher.

### 4. Start Redis and services

```bash
# Terminal 1: Redis (if not already running)
redis-server

# Terminal 2: run all pipeline services
chmod +x scripts/run_local.sh
./scripts/run_local.sh
```

Or run each service in its own terminal:

```bash
python services/imap_poller/main.py
python workers/normalizer/main.py
python workers/classifier/main.py
python workers/persister/main.py
python workers/watcher/watcher_semantic.py
```

### 5. View results

```bash
python show_subscriptions.py
```

Add a watcher (preset or custom):

```bash
python scripts/add_watcher.py
```

## Project structure

```
.
├── infra/
│   ├── init.sql              # Schema for messages/classifications (run in Supabase)
│   ├── init-scripts/
│   └── test_idempotency.sql
├── services/
│   └── imap_poller/
├── workers/
│   ├── normalizer/
│   ├── classifier/
│   ├── persister/
│   ├── watcher/              # Semantic watcher (Supabase + Voyage)
│   └── dlq_replayer/
├── scripts/
│   ├── add_watcher.py        # CLI to add watchers
│   └── run_local.sh          # Start all services
├── show_subscriptions.py
└── .env.example
```

## Configuration

- **Redis**: `REDIS_HOST=localhost`, `REDIS_PORT=6379` when running locally.
- **Database**: Use Supabase Postgres; set `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` from Supabase (Project Settings → Database).
- **Supabase API**: `SUPABASE_URL`, `SUPABASE_API_KEY` for watcher and `scripts/add_watcher.py`.

## Troubleshooting

- **Redis**: Ensure Redis is running (`redis-cli ping`).
- **IMAP**: Use an App Password with 2FA enabled for Gmail.
- **No data**: Check each worker’s logs; ensure `.env` has correct keys and Supabase schema is applied.
