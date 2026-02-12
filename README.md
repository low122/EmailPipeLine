# EmailPipeLine

An event-driven email processing pipeline that scans inboxes, normalizes emails, matches them to user-defined watchers, and extracts data via AI. Uses Redis Streams and runs **without Docker** — install Redis and run the Python services locally.

## Architecture

- **Phase 3**: IMAP Poller — scans inbox, publishes to `raw_emails.v1`
- **Phase 4**: Normalizer — parses MIME, cleans text, publishes to `emails.normalized.v1`
- **Watcher** — semantic filter: matches emails to user-defined watchers (Supabase + Voyage embeddings), publishes to `emails.to_classify.v1`
- **Phase 5**: Classifier — Claude AI extracts watcher-specific data (billing, flights, rentals, etc.), publishes to `emails.classified.v1`
- **Phase 6**: Persister — saves to Supabase (messages + classifications)

Classifications are flexible: `class` (watcher name), `confidence`, and `extracted_data` (JSONB) hold all extracted fields per watcher type.

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
# Edit .env: IMAP_*, REDIS_HOST=localhost, SUPABASE_URL, SUPABASE_API_KEY, CLAUDE_API_KEY, VOYAGE_API_KEY (if using watcher)
```

### 3. Database (Supabase)

In Supabase SQL Editor:

- **New project**: Run `infra/init-scripts/01-schema.sql` to create `messages` and `classifications`.
- **Existing project** (with old schema): Run `infra/init-scripts/02-classifications-flexible.sql` to migrate.

For the watcher: create `watchers`, `watcher_queries`, `email_embeddings`, and the `match_watcher_queries` RPC. Use `scripts/create_watcher_bundle.py`.

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

Shows watcher matches with extracted data (vendor, amount, flight_number, etc. from `extracted_data`).

### 6. Add watchers

```bash
python scripts/create_watcher_bundle.py
```

Generates AI-powered semantic prototypes from your description and creates the watcher bundle.

## Project structure

```
.
├── infra/
│   ├── init.sql
│   ├── init-scripts/
│   │   ├── 01-schema.sql           # Base schema (messages, classifications)
│   │   └── 02-classifications-flexible.sql   # Migration: extracted_data, drop vendor/amount/currency
│   └── test_idempotency.sql
├── services/
│   └── imap_poller/
├── workers/
│   ├── normalizer/
│   ├── classifier/                 # Watcher-driven: extracts into extracted_data
│   ├── persister/
│   ├── watcher/                    # Semantic watcher (Supabase + Voyage)
│   └── dlq_replayer/
├── scripts/
│   ├── create_watcher_bundle.py    # AI-powered watcher creation
│   └── run_local.sh          # Start all services
├── show_subscriptions.py     # View watcher results (uses Supabase API)
└── .env.example
```

## Configuration

- **Redis**: `REDIS_HOST=localhost`, `REDIS_PORT=6379` when running locally.
- **Supabase**: `SUPABASE_URL`, `SUPABASE_API_KEY` — used by persister, watcher, show_subscriptions, and scripts.

## Troubleshooting

- **Redis**: Ensure Redis is running (`redis-cli ping`).
- **IMAP**: Use an App Password with 2FA enabled for Gmail.
- **No data**: Check each worker’s logs; ensure `.env` has correct keys and Supabase schema is applied. Run migration `02-classifications-flexible.sql` if you have the old schema.
