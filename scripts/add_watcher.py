"""
Add Watcher CLI
Prompts for a preset or custom watcher, embeds the query with Voyage, and inserts into Supabase watchers table.
Run from repo root: python scripts/add_watcher.py
"""

import os
import sys
from dotenv import load_dotenv
from supabase import create_client
import voyageai

# Load .env from repo root
load_dotenv()

# Presets: (display_name, query_text used for embedding and matching)
WATCHER_PRESETS = [
    ("Interview follow up", "Interview follow up, scheduling, next steps, thank you after interview"),
    ("Payment / invoice", "Payment, invoice, receipt, billing, amount due, Authorization without credit card present"),
    ("Other", None),  # Custom: user enters name and query text
]

VOYAGE_MODEL = os.getenv("VOYAGE_MODEL", "voyage-3.5-lite")
DEFAULT_THRESHOLD = 0.7


def get_mailbox_id() -> str:
    """Mailbox ID from env or prompt."""
    mailbox_id = os.getenv("MAILBOX_ID", "").strip()
    if mailbox_id:
        return mailbox_id
    return input("Enter mailbox_id (e.g. your@email.com): ").strip()


def get_preset_choice() -> tuple[str, str]:
    """Let user pick preset or Other; return (name, query_text)."""
    print("\nWhat do you want to track?")
    for i, (label, _) in enumerate(WATCHER_PRESETS, 1):
        print(f"  {i}. {label}")
    raw = input("Choice [1-3]: ").strip()
    try:
        idx = int(raw)
        if 1 <= idx <= len(WATCHER_PRESETS):
            name, query_text = WATCHER_PRESETS[idx - 1]
            if name == "Other":
                name = input("Watcher name (e.g. 'Refund requests'): ").strip() or "Custom"
                query_text = input("Query text to match (e.g. 'refund, return, cancellation'): ").strip()
                if not query_text:
                    print("Query text is required.")
                    sys.exit(1)
                return name, query_text
            if not query_text:
                query_text = name
            return name, query_text
    except ValueError:
        pass
    print("Invalid choice.")
    sys.exit(1)


def get_threshold() -> float:
    """Threshold for cosine-similarity match (0–1)."""
    raw = input(f"Similarity threshold [0–1, default {DEFAULT_THRESHOLD}]: ").strip()
    if not raw:
        return DEFAULT_THRESHOLD
    try:
        t = float(raw)
        if 0 <= t <= 1:
            return t
    except ValueError:
        pass
    print(f"Using default threshold {DEFAULT_THRESHOLD}.")
    return DEFAULT_THRESHOLD


def main():
    print("Add watcher — track emails by semantic similarity\n")

    # 1) Resolve watcher name and query text
    name, query_text = get_preset_choice()
    mailbox_id = get_mailbox_id()
    if not mailbox_id:
        print("mailbox_id is required.")
        sys.exit(1)
    threshold = get_threshold()

    # 2) Supabase + Voyage clients
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_API_KEY")
    voyage_key = os.environ.get("VOYAGE_API_KEY")
    if not supabase_url or not supabase_key:
        print("Set SUPABASE_URL and SUPABASE_API_KEY in .env")
        sys.exit(1)
    if not voyage_key:
        print("Set VOYAGE_API_KEY in .env")
        sys.exit(1)

    sb = create_client(supabase_url, supabase_key)
    vo = voyageai.Client(api_key=voyage_key)

    # 3) Embed query text
    print("Embedding query...")
    emb_resp = vo.embed(texts=[query_text], model=VOYAGE_MODEL)
    query_embedding = emb_resp.embeddings[0]

    # 4) Insert watcher (id, created_at are server-generated)
    row = {
        "mailbox_id": mailbox_id,
        "name": name,
        "query_text": query_text,
        "query_embedding": query_embedding,
        "threshold": threshold,
        "is_active": True,
    }
    result = sb.table("watchers").insert(row).execute()
    if result.data and len(result.data) > 0:
        watcher_id = result.data[0].get("id")
        print(f"Created watcher: id={watcher_id}, name={name!r}, mailbox_id={mailbox_id!r}")
    else:
        print("Insert succeeded but no data returned (check Supabase dashboard).")


if __name__ == "__main__":
    main()