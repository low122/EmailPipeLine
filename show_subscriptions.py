#!/usr/bin/env python3
"""
Show Watcher Results - Writes watcher matches to a markdown file
Uses Supabase API (same as persister, create_watcher_bundle) - no direct Postgres needed.
"""

import os
from email.header import decode_header
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

OUTPUT_FILE = os.getenv("WATCHER_RESULTS_OUTPUT", "watcher_results.md")

# Fields to skip (noise, redundant, or low-value)
SKIP_KEYS = {"email", "contact_email", "applicant_name", "contact_name", "contact_method", "sender", "survey_link"}
# Max length for values (truncate long URLs, etc.)
MAX_VALUE_LEN = 60


def _decode_subject(s: str) -> str:
    """Decode MIME encoded-word subject (e.g. =?UTF-8?Q?...?=) to readable text."""
    if not s or "=?" not in s:
        return (s or "").strip()
    try:
        parts = decode_header(s)
        decoded = []
        for part, charset in parts:
            if isinstance(part, bytes):
                decoded.append(part.decode(charset or "utf-8", errors="replace"))
            else:
                decoded.append(part or "")
        return " ".join(decoded).replace("\n", " ").strip()
    except Exception:
        return s.strip()


def _cleaned_subject(s: str, max_len: int = 80) -> str:
    """Decode and truncate subject."""
    decoded = _decode_subject(s or "")
    if len(decoded) > max_len:
        return decoded[: max_len - 3] + "..."
    return decoded


def _fmt_extracted_bullets(ext: dict) -> list[str]:
    """Return list of human-readable bullet lines. Only valuable fields, skip long URLs."""
    if not ext:
        return []
    bullets = []
    for k, v in ext.items():
        if k in SKIP_KEYS or v is None or v == "":
            continue
        if isinstance(v, list):
            v = ", ".join(str(x) for x in v[:5])  # First 5 items
        val = str(v)
        if len(val) > MAX_VALUE_LEN:
            val = val[: MAX_VALUE_LEN - 3] + "..."
        if k == "amount_cents" and isinstance(ext[k], (int, float)):
            val = f"${ext[k] / 100:.2f}"
        # Human-readable key (snake_case -> Title Case)
        label = k.replace("_", " ").title()
        bullets.append(f"- **{label}:** {val}")
    return bullets


def get_supabase():
    """Connect via Supabase API (same as persister)."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_API_KEY")
    if not url or not key:
        print("❌ Set SUPABASE_URL and SUPABASE_API_KEY in .env")
        return None
    return create_client(url, key)


def show_subscriptions():
    """Fetch watcher results and write to markdown file."""
    sb = get_supabase()
    if not sb:
        return

    try:
        from datetime import datetime

        # Fetch classifications (MUST: class, confidence; all else in extracted_data)
        try:
            resp = (
                sb.table("classifications")
                .select("message_id, class, confidence, extracted_data")
                .gte("confidence", 0.7)
                .execute()
            )
        except Exception:
            resp = (
                sb.table("classifications")
                .select("message_id, class, confidence")
                .gte("confidence", 0.7)
                .execute()
            )
        rows = resp.data or []
        if not rows:
            results = []
        else:
            msg_ids = [r["message_id"] for r in rows]
            msg_resp = sb.table("messages").select("id, received_at, subject, mailbox_id").in_("id", msg_ids).execute()
            msg_map = {m["id"]: m for m in (msg_resp.data or [])}

            results = []
            for r in rows:
                m = msg_map.get(r["message_id"]) or {}
                received_at = m.get("received_at")
                if received_at and isinstance(received_at, str) and "T" in received_at:
                    try:
                        received_at = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
                    except Exception:
                        pass
                ext = r.get("extracted_data") or {}
                if not isinstance(ext, dict):
                    ext = {}
                results.append((
                    r.get("class"),
                    r.get("confidence"),
                    received_at,
                    m.get("subject"),
                    m.get("mailbox_id"),
                    ext,
                ))

            results.sort(key=lambda x: (x[2] or datetime.min).isoformat() if x[2] else "", reverse=True)
            results.sort(key=lambda x: str(x[0] or ""))

        # Build markdown
        lines = [
            "# Watcher Results",
            "",
            "Emails matched by your watchers (user-defined intent).",
            "",
        ]

        if not results:
            lines.extend([
                "No watcher matches yet.",
                "",
                "Add watchers with: `python scripts/create_watcher_bundle.py`",
                "",
            ])
        else:
            lines.append(f"**{len(results)}** match(es).\n")
            for i, row in enumerate(results, 1):
                class_type, confidence, date, subject, mailbox, ext = row
                date_str = date.strftime("%Y-%m-%d") if date else "N/A"
                subj = _cleaned_subject(subject or "")
                bullets = _fmt_extracted_bullets(ext)
                lines.append(f"### {i}. {subj}")
                lines.append(f"*{date_str}* · {class_type or 'N/A'} ({confidence*100:.0f}%)")
                if bullets:
                    lines.extend(bullets)
                lines.append("")

            # Stats
            msg_resp = sb.table("messages").select("*", count="exact").limit(1).execute()
            total_emails = getattr(msg_resp, "count", None) or len(msg_resp.data or [])
            mailbox_resp = sb.table("messages").select("mailbox_id").execute()
            accounts = len(set(r.get("mailbox_id") for r in (mailbox_resp.data or []) if r.get("mailbox_id")))

            lines.extend([
                "",
                "## Pipeline Statistics",
                "",
                f"- **Emails Processed:** {total_emails}",
                f"- **Accounts Monitored:** {accounts}",
                f"- **Watcher Matches:** {len(results)}",
                "",
            ])

        md_content = "\n".join(lines)

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(md_content)

        print(f"✅ Wrote {len(results)} match(es) to {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    show_subscriptions()
