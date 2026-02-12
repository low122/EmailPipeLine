# EmailPipeLine — Setup (no Docker)

Run the pipeline locally: Redis + Python services. Database and watcher data live in Supabase.

---

## 1. Clone and Python

```bash
git clone https://github.com/low122/EmailPipeLine.git
cd EmailPipeLine
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

If you use the watcher or `scripts/create_watcher_bundle.py`, install:

```bash
pip install supabase voyageai
```

---

## 2. Redis

Install and start Redis:

- **macOS**: `brew install redis` then `redis-server`
- **Linux**: `sudo apt install redis-server` (or equivalent), then `redis-server`
- **Windows**: Use WSL or [Redis for Windows](https://github.com/microsoftarchive/redis/releases)

Check:

```bash
redis-cli ping
# Should reply: PONG
```

---

## 3. Environment file

```bash
cp .env.example .env
```

Edit `.env`:

| Variable | Where to get it |
|----------|-----------------|
| `IMAP_SERVER`, `EMAIL_USER`, `EMAIL_PASSWORD` | Your email provider (Gmail: imap.gmail.com, use App Password) |
| `REDIS_HOST` | `localhost` when running without Docker |
| `REDIS_PORT` | `6379` |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | Supabase → Project Settings → Database (connection params) |
| `SUPABASE_URL`, `SUPABASE_API_KEY` | Supabase → Project Settings → API |
| `CLAUDE_API_KEY` | [Anthropic Console](https://console.anthropic.com/) |
| `VOYAGE_API_KEY` | For watcher / create_watcher_bundle (Voyage AI) |

---

## 4. Supabase database

1. Create a project at [supabase.com](https://supabase.com).
2. In **SQL Editor**, run the schema that creates `messages` and `classifications`:
   - Copy contents of `infra/init.sql` or `infra/init-scripts/01-schema.sql` and execute.
3. If you use the **watcher**: create `watchers` and `email_embeddings` tables and the `match_watchers` RPC (use your existing schema for these).

---

## 5. Run the pipeline

**Option A — one script (from repo root):**

```bash
chmod +x scripts/run_local.sh
./scripts/run_local.sh
```

**Option B — separate terminals:**

```bash
# Terminal 1
python services/imap_poller/main.py

# Terminal 2
python workers/normalizer/main.py

# Terminal 3
python workers/classifier/main.py

# Terminal 4
python workers/persister/main.py

# Terminal 5 (optional, if using watcher)
python workers/watcher/watcher_semantic.py
```

---

## 6. Verify

- **Subscriptions**: `python show_subscriptions.py`
- **Add a watcher**: `python scripts/create_watcher_bundle.py`
- **Database**: Use Supabase Table Editor or SQL Editor to query `messages` and `classifications`.

---

## Stopping services

If you used `scripts/run_local.sh`, stop all with:

```bash
kill $(cat .run_local.pids)
```

Otherwise stop each Python process (Ctrl+C in each terminal).

---

## Quick reference

| Step | Command |
|------|--------|
| Start Redis | `redis-server` |
| Start all services | `./scripts/run_local.sh` |
| View subscriptions | `python show_subscriptions.py` |
| Add watcher | `python scripts/create_watcher_bundle.py` |
| Stop services | `kill $(cat .run_local.pids)` |
