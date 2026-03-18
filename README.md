# EmailPipeLine

Event-driven email pipeline that:

- pulls emails from your inbox
- normalizes them
- matches them to your semantic “watchers”
- extracts structured data with AI

Runs on **Redis Streams + Python**, no Docker needed.

---

## What you need

- Python 3.10+
- Redis running locally
- Supabase project
- IMAP account (e.g. Gmail App Password)
- Claude API key, optional Voyage API key (for semantic watcher)

---

## Run it

```bash
git clone https://github.com/low122/EmailPipeLine.git
cd EmailPipeLine
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

```bash
cp .env.example .env
# fill IMAP_*, Redis, Supabase, Claude, (Voyage) keys
```

Start Redis and all services:

```bash
redis-server                      # in one terminal

chmod +x scripts/run_local.sh
./scripts/run_local.sh            # in another terminal
```

See the results:

```bash
python show_subscriptions.py
```

Create a watcher (semantic filter):

```bash
python scripts/create_watcher_bundle.py
```

## High-level architecture

flowchart LR
    Inbox["Email Inbox (IMAP)"]

    subgraph Pipeline["Redis Streams + Workers"]
        P1["IMAP Poller"]
        P2["Normalizer"]
        P3["Watcher (semantic)"]
        P4["Classifier (Claude)"]
        P5["Persister"]
    end

    subgraph Supabase["Supabase (Postgres)"]
        DB1["messages + classifications"]
        DB2["watchers + embeddings"]
    end

    Output["show_subscriptions.py\n(terminal report)"]

    Inbox --> P1 --> P2 --> P3 --> P4 --> P5
    P5 --> DB1
    DB1 --> Output

    %% Watcher uses Supabase data + writes embeddings
    P3 <---> DB2