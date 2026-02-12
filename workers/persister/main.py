"""
Persister Worker - Phase 6
Consumes from emails.classified.v1, saves messages and classifications via Supabase API
"""

import json
import structlog
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import redis
from supabase import create_client

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer()
    ]
)
log = structlog.get_logger(service="persister")


def get_supabase():
    """Create Supabase client. Uses same API as add_watcher (no direct Postgres)."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_API_KEY")
    if not url or not key:
        raise ValueError("Set SUPABASE_URL and SUPABASE_API_KEY in .env")
    return create_client(url, key)


def save_message(sb, fields: dict) -> int | None:
    """
    Upsert message in messages table via Supabase API.
    Returns message id or None on failure.
    """
    try:
        idemp_key = fields.get("idemp_key", "") or ""
        mailbox_id = fields.get("mailbox_id", "") or ""
        external_id = fields.get("external_id", "") or ""
        subject = fields.get("subject", "") or ""
        body_hash = fields.get("body_hash", "") or ""
        received_ts = fields.get("received_ts", "")

        received_at = None
        if received_ts:
            try:
                received_at = datetime.fromtimestamp(int(received_ts)).isoformat()
            except (ValueError, TypeError):
                pass

        row = {
            "idemp_key": idemp_key,
            "mailbox_id": mailbox_id,
            "external_id": external_id,
            "subject": subject,
            "body_hash": body_hash,
            "received_at": received_at,
        }
        result = sb.table("messages").upsert(row, on_conflict="idemp_key").execute()
        if not result.data or len(result.data) == 0:
            log.error("Message upsert returned no data", idemp_key=idemp_key)
            return None
        message_id = result.data[0]["id"]
        log.info("Saved message", message_id=message_id, idemp_key=idemp_key)
        return message_id
    except Exception as e:
        log.error("Error saving message", error=str(e))
        return None


def save_classification(sb, message_id: int, fields: dict) -> bool:
    """Upsert classification. MUST: message_id, class, confidence. Rest in extracted_data."""
    try:
        class_type = fields.get("class", "") or ""
        confidence_str = fields.get("confidence", "0.0") or "0.0"
        watcher_id = fields.get("watcher_id", "") or None
        extracted_data_raw = fields.get("extracted_data", "{}") or "{}"

        confidence = float(confidence_str) if confidence_str else 0.0
        try:
            extracted_data = json.loads(extracted_data_raw) if isinstance(extracted_data_raw, str) else (extracted_data_raw or {})
        except (json.JSONDecodeError, TypeError):
            extracted_data = {}

        row = {
            "message_id": message_id,
            "class": class_type,
            "confidence": confidence,
            "watcher_id": watcher_id if watcher_id else None,
            "extracted_data": extracted_data,
        }
        sb.table("classifications").upsert(row, on_conflict="message_id").execute()
        log.info("Saved classification", message_id=message_id, class_=class_type, confidence=confidence)
        return True
    except Exception as e:
        log.error("Error saving classification", error=str(e))
        return False


def main():
    """Phase 6: Consumes from emails.classified.v1, saves to Supabase."""
    log.info("persister starting...")

    r = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True,
    )

    try:
        sb = get_supabase()
        log.info("Connected to Supabase")
    except Exception as e:
        log.error("Supabase connection failed", error=str(e))
        return

    try:
        r.xgroup_create("emails.classified.v1", "persister-g", id="0", mkstream=True)
        log.info("Created consumer group persister-g")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    log.info("persister ready")
    consumer_name = f"persister-{os.getpid()}"

    while True:
        try:
            messages = r.xreadgroup(
                "persister-g",
                consumer_name,
                {"emails.classified.v1": ">"},
                count=1,
                block=1000,
            )

            if messages:
                stream, data = messages[0]
                message_id, fields = data[0]

                log.info("Processing classified email", message_id=message_id, class_=fields.get("class"))

                db_message_id = save_message(sb, fields)
                if db_message_id:
                    save_classification(sb, db_message_id, fields)
                    log.info("Persisted email", db_message_id=db_message_id, class_=fields.get("class"))
                else:
                    log.warning("Failed to save message, skipping classification")

                r.xack("emails.classified.v1", "persister-g", message_id)
                log.info("Acknowledged message", message_id=message_id)

            time.sleep(1)
        except Exception as e:
            log.error("Error consuming message", error=str(e))
            time.sleep(5)


if __name__ == "__main__":
    main()
