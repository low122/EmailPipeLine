import os, json, sys
from dotenv import load_dotenv
from supabase import create_client
import voyageai
from anthropic import Anthropic

load_dotenv()

DEFAULT_THRESHOLD = 0.7


def get_mailbox_id() -> str:
    """Mailbox ID from env or prompt."""
    mailbox_id = os.getenv("MAILBOX_ID", "").strip()
    if mailbox_id:
        return mailbox_id
    return input("Enter mailbox_id (e.g. your@email.com): ").strip()


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


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
VOYAGE_API_KEY = os.environ["VOYAGE_API_KEY"]
ANTHROPIC_API_KEY = os.environ["CLAUDE_API_KEY"]

VOYAGE_MODEL = os.getenv("VOYAGE_MODEL", "voyage-3.5-lite")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
vo = voyageai.Client(api_key=VOYAGE_API_KEY)
cl = Anthropic(api_key=ANTHROPIC_API_KEY)

def claude_expand_intent(intent_name: str, user_seed: str, n: int = 10) -> list[str]:
    """
    Returns a list of semantic prototype sentences (NOT keywords).
    """
    prompt = f"""
You are generating semantic prototypes for an email routing system.
Goal: produce {n} short, distinct prototype sentences that match emails described below.

Constraints:
- Each item is ONE sentence (max ~18 words).
- Avoid comma keyword lists; write natural sentences.
- Include niche/industry phrasing and paraphrases where useful.
- Do NOT include personally identifying info.

Return ONLY valid JSON:
{{"prototypes": ["...", "..."]}}

What emails to match: {user_seed}
"""

    # Choose a Claude model you have access to; keep it simple.
    resp = cl.messages.create(
        model="claude-3-5-haiku-20241022",
        max_tokens=600,
        temperature=0.4,
        messages=[{"role": "user", "content": prompt}],
    )

    # Claude returns text; we asked for JSON only.
    text = resp.content[0].text
    data = json.loads(text)
    protos = data["prototypes"]

    # Basic sanity: unique, non-empty
    clean = []
    seen = set()
    for p in protos:
        p = p.strip()
        if p and p not in seen:
            seen.add(p)
            clean.append(p)
    return clean[:n]

def create_watcher_bundle(mailbox_id: str, watcher_name: str, threshold: float, user_seed: str):
    ## 1) Embed the user_seed to use as the main query_embedding for the watcher
    seed_emb_resp = vo.embed(texts=[user_seed], model=VOYAGE_MODEL)
    seed_embedding = seed_emb_resp.embeddings[0]
    
    # 2) Create watcher row (now with required fields)
    w = sb.table("watchers").insert({
        "mailbox_id": mailbox_id,
        "name": watcher_name,
        "query_text": user_seed,  # Use user_seed as the main query text
        "query_embedding": seed_embedding,  # Embedding of user_seed
        "threshold": threshold,
        "is_active": True,
    }).execute()
    watcher_id = w.data[0]["id"]

    # 3) Claude expansion (for watcher_queries table - if you're using that)
    prototypes = claude_expand_intent(watcher_name, user_seed, n=10)

    # 4) Embed prototypes (Voyage)
    emb_resp = vo.embed(texts=prototypes, model=VOYAGE_MODEL)
    embeddings = emb_resp.embeddings

    # 5) Insert watcher_queries (if this table exists and is used)
    rows = []
    for proto, emb in zip(prototypes, embeddings):
        rows.append({
            "watcher_id": watcher_id,
            "query_text": proto,
            "query_embedding": emb,
        })

    sb.table("watcher_queries").insert(rows).execute()
    

    print("Created watcher bundle:")
    print("watcher_id:", watcher_id)
    print("prototypes:")
    for p in prototypes:
        print("-", p)

if __name__ == "__main__":
    print("AI-powered Watcher Bundle Creator\n")
    
    mailbox_id = get_mailbox_id()
    if not mailbox_id:
        print("mailbox_id is required.")
        sys.exit(1)
    
    watcher_name = input("Watcher name (e.g. 'Billing'): ").strip()
    if not watcher_name:
        print("Watcher name is required.")
        sys.exit(1)
    
    user_seed = input("Describe what emails to match (e.g. 'Emails about payments, invoices, receipts...'): ").strip()
    if not user_seed:
        print("Description is required.")
        sys.exit(1)
    
    threshold = get_threshold()
    
    create_watcher_bundle(
        mailbox_id=mailbox_id,
        watcher_name=watcher_name,
        threshold=threshold,
        user_seed=user_seed
    )