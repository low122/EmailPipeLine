"""
Semantic Watcher Worker
Consumes emails.normalized.v1 -> embeds email text, finds matching watchers in Supabase ->
publishes to notifications.pending.v1
"""

import os, time
import redis
import structlog
from dotenv import load_dotenv
from supabase import create_client
import voyageai

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer()
    ]
)
log = structlog.get_logger(service="semantic_filter")


STREAM_IN = "emails.normalized.v1"
STREAM_OUT = "emails.to_classify.v1"
GROUP = "semantic-filter-g"

VOYAGE_MODEL = os.getenv("VOYAGE_MODEL", "voyage-3.5-lite")
TOP_K = 5
EMAIL_TEXT_LIMIT = 1000
# When true: only match emails already in email_embeddings (no Voyage calls for new emails). Use for testing with 0 cost.
CACHE_ONLY = os.getenv("WATCHER_CACHE_ONLY", "0").strip().lower() in ("1", "true", "yes")


def build_email_text(subject: str, text_content: str) -> str:
    subject = (subject or "").strip()
    text_content = (text_content or "").strip()
    s = f"{subject}\n{text_content}".strip()
    return s[:EMAIL_TEXT_LIMIT]


def similarity_from_cosine_distance(d: float) -> float:
    return 1.0 - float(d)


def main():
    log.info("semantic_filter starting...")

    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True
    )

    sb = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    )

    vo = None if CACHE_ONLY else voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"])
    if CACHE_ONLY:
        log.info("Cache-only mode: skipping Voyage for emails not in email_embeddings")

    try:
        r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
        log.info("Created consumer group", group=GROUP)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    consumer_name = f"semantic-filter-{os.getpid()}"
    log.info("semantic_filter ready", consumer=consumer_name)

    while True:
        try:
            msgs = r.xreadgroup(
                GROUP,
                consumer_name,
                {STREAM_IN: ">"},
                count=1,
                block=1000
            )

            if not msgs:
                continue

            _, data = msgs[0]
            msg_id, fields = data[0]

            mailbox_id = fields.get("mailbox_id", "")
            body_hash = fields.get("body_hash", "")
            subject = fields.get("subject", "")
            text_content = fields.get("text_content", "")

            if not mailbox_id or not body_hash:
                r.xack(STREAM_IN, GROUP, msg_id)
                continue

            email_text = build_email_text(subject, text_content)
            if len(email_text) < 40:
                # too little signal: skip (or route if you want)
                r.xack(STREAM_IN, GROUP, msg_id)
                continue

            # 1) embedding cache lookup in Supabase
            cached = sb.table("email_embeddings") \
                .select("email_embedding") \
                .eq("mailbox_id", mailbox_id) \
                .eq("body_hash", body_hash) \
                .limit(1) \
                .execute().data

            if cached:
                email_emb = cached[0]["email_embedding"]
            else:
                if CACHE_ONLY:
                    log.info("Skipped (not in cache, cache-only mode)", mailbox_id=mailbox_id, body_hash=body_hash[:16])
                    r.xack(STREAM_IN, GROUP, msg_id)
                    continue
                emb_resp = vo.embed(texts=[email_text], model=VOYAGE_MODEL)
                email_emb = emb_resp.embeddings[0]
                sb.table("email_embeddings").upsert({
                    "mailbox_id": mailbox_id,
                    "body_hash": body_hash,
                    "email_embedding": email_emb
                }).execute()

            rpc = sb.rpc("match_watcher_queries", {
                "p_mailbox_id": mailbox_id,
                "p_email_embedding": email_emb,
                "p_limit": TOP_K
            }).execute()

            candidates = rpc.data or []
            if not candidates:
                r.xack(STREAM_IN, GROUP, msg_id)
                continue

            best = candidates[0]

            sim = similarity_from_cosine_distance(float(best["cosine_distance"]))
            threshold = float(best["watcher_threshold"])

            if sim >= threshold:
                out = {
                    # original metadata
                    "trace_id": fields.get("trace_id", ""),
                    "mailbox_id": mailbox_id,
                    "idemp_key": fields.get("idemp_key", ""),
                    "body_hash": body_hash,
                    "text_content": text_content,
                    "subject": subject,
                    "external_id": fields.get("external_id", ""),
                    "received_ts": fields.get("received_ts", ""),

                    # filter metadata
                    "filter_watcher_id": str(best["watcher_id"]),
                    "filter_watcher_name": best["watcher_name"],
                    "filter_query_id": str(best["query_id"]),
                    "filter_query_text": best["query_text"],
                    "filter_similarity": f"{sim:.4f}",
                }

                out_id = r.xadd(STREAM_OUT, out)

                log.info("Routed to classifier",
                        msg_id=msg_id,
                        out_id=out_id,
                        watcher=best["watcher_name"],
                        sim=f"{sim:.3f}",
                        threshold=f"{threshold:.3f}")
            else:
                log.info("Filtered out",
                        msg_id=msg_id,
                        watcher=best["watcher_name"],
                        sim=f"{sim:.3f}",
                        threshold=f"{threshold:.3f}")

            r.xack(STREAM_IN, GROUP, msg_id)

        except Exception as e:
            log.exception("semantic_filter error", error=str(e))
            time.sleep(2)

if __name__ == "__main__":
    main()