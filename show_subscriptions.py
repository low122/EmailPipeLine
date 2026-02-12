#!/usr/bin/env python3
"""
Show Watcher Results - Displays emails matched by user-defined watchers
Uses Supabase API (same as persister, create_watcher_bundle) - no direct Postgres needed.
"""

import os
from dotenv import load_dotenv
from supabase import create_client

# Try to import tabulate, fallback to simple formatting
try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

load_dotenv()


def _fmt_extracted(ext: dict) -> str:
    """Format extracted_data for display. Handles amount_cents (e.g. 1999 -> $19.99)."""
    if not ext:
        return "—"
    parts = []
    for k, v in ext.items():
        if v is None or v == "":
            continue
        if k == "amount_cents" and isinstance(v, (int, float)):
            parts.append(f"amount: ${v/100:.2f}")
        else:
            parts.append(f"{k}: {v}")
    s = ", ".join(parts)
    return s[:55] + ("..." if len(s) > 55 else "")


def get_supabase():
    """Connect via Supabase API (same as persister)."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_API_KEY")
    if not url or not key:
        print("❌ Set SUPABASE_URL and SUPABASE_API_KEY in .env")
        return None
    return create_client(url, key)


def show_subscriptions():
    """Show watcher results from the pipeline (all classifications by user intent)"""
    print("\n" + "="*80)
    print(" " * 22 + "📧 WATCHER RESULTS")
    print("="*80)
    print("Emails matched by your watchers (user-defined intent)\n")
    
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
            # Fetch messages for those message_ids
            msg_ids = [r["message_id"] for r in rows]
            msg_resp = sb.table("messages").select("id, received_at, subject, mailbox_id").in_("id", msg_ids).execute()
            msg_map = {m["id"]: m for m in (msg_resp.data or [])}

            # Join and flatten
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

            # Sort by class, then by received_at desc
            results.sort(key=lambda x: (x[2] or datetime.min).isoformat() if x[2] else "", reverse=True)
            results.sort(key=lambda x: str(x[0] or ""))

        if not results:
            print("📭 No watcher matches yet.")
            print("\nThe pipeline is processing your emails...")
            print("Add watchers with: python scripts/create_watcher_bundle.py")
            print("Or: python scripts/create_watcher_bundle.py")
            print("\n💡 Check back after adding watchers and processing emails.\n")
            return
        
        # Display results (group by watcher/class)
        print(f"Found {len(results)} match(es):\n")
        
        if HAS_TABULATE:
            headers = ["Watcher", "Confidence", "Date", "Subject", "Extracted", "Account"]
            table_data = []
            for row in results:
                class_type, confidence, date, subject, mailbox, ext = row
                table_data.append([
                    class_type or "N/A",
                    f"{confidence*100:.0f}%" if confidence else "0%",
                    date.strftime("%Y-%m-%d") if date else "N/A",
                    (subject[:35] + "...") if subject and len(subject) > 35 else (subject or "—"),
                    _fmt_extracted(ext),
                    (mailbox[:15] + "...") if mailbox and len(mailbox) > 15 else (mailbox or "N/A")
                ])
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
        else:
            print(f"{'Watcher':<18} {'Conf':<8} {'Date':<12} {'Subject':<40} {'Extracted':<45} {'Account':<20}")
            print("-" * 150)
            for row in results:
                class_type, confidence, date, subject, mailbox, ext = row
                confidence_str = f"{confidence*100:.0f}%" if confidence else "0%"
                date_str = date.strftime("%Y-%m-%d") if date else "N/A"
                subject_str = (subject[:38] + "..") if subject and len(subject) > 38 else (subject or "—")
                mailbox_str = (mailbox[:18] + "..") if mailbox and len(mailbox) > 18 else (mailbox or "N/A")
                ext_str = _fmt_extracted(ext)
                print(f"{class_type or 'N/A':<18} {confidence_str:<8} {date_str:<12} {subject_str:<40} {ext_str:<45} {mailbox_str:<20}")
        
        # Show pipeline stats
        msg_resp = sb.table("messages").select("*", count="exact").limit(1).execute()
        total_emails = getattr(msg_resp, "count", None) or len(msg_resp.data or [])
        mailbox_resp = sb.table("messages").select("mailbox_id").execute()
        accounts = len(set(r.get("mailbox_id") for r in (mailbox_resp.data or []) if r.get("mailbox_id")))

        print(f"\n📊 Pipeline Statistics:")
        print(f"   • Emails Processed: {total_emails}")
        print(f"   • Accounts Monitored: {accounts}")
        print(f"   • Watcher Matches: {len(results)}")

    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "="*80)
    print("💡 Add watchers: python scripts/create_watcher_bundle.py")
    print("   Or: python scripts/run_local.sh to run the pipeline\n")

if __name__ == "__main__":
    show_subscriptions()

