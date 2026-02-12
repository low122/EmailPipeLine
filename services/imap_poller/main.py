"""
IMAP Poller Service - Phase 3
Polls email servers via IMAP and publishes to raw_emails.v1 stream
"""

import structlog
import time
import os
import redis
import imaplib
import email
import hashlib
import base64
from dotenv import load_dotenv

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer()
    ]
)

log = structlog.get_logger(service="imap_poller")


def build_idempotency_key(provider: str, mailbox_id: str, external_id: str) -> str:
    """
    Build idempotency key: sha256(provider || mailbox_id || external_id)
    
    Args:
        provider: Email provider (e.g., "gmail")
        mailbox_id: Email address (e.g., "user@gmail.com")
        external_id: IMAP UID or Message-ID
    
    Returns:
        SHA256 hash as hex string
    """
    combined = provider + mailbox_id + external_id
    combined_bytes = combined.encode('utf-8')
    idemp_key = hashlib.sha256(combined_bytes).hexdigest()

    return idemp_key


def connect_to_imap():
    """
    Connect to IMAP server
    
    Returns:
        IMAP4_SSL connection or None if failed
    """
    try:
        imap_server = os.getenv('IMAP_SERVER')
        email_user = os.getenv('EMAIL_USER')
        email_password = os.getenv('EMAIL_PASSWORD')
        
        mail = imaplib.IMAP4_SSL(imap_server)
        mail.login(email_user, email_password)
        log.info("Connected to IMAP successfully", server=imap_server, user=email_user)
        return mail
        
    except Exception as e:
        log.error("IMAP connection failed", error=str(e))
        return None

def poll_emails(mail, last_uid=0):
    """
    Poll for new emails since last_uid
    
    Args:
        mail: IMAP connection
        last_uid: Last processed UID (track progress)
    
    Returns:
        List of email dicts with: uid, subject, from_addr, date, raw, message_id
    """
    try:
        status, _ = mail.select("INBOX")
        
        # Search for new UIDs
        status, response = mail.uid('SEARCH', None, 'ALL')
        # Response: ['100 101 102 103'] (list with one string of space-separated UIDs)
        
        # parse UID list; only process UIDs > last_uid (new emails)
        if status == 'OK' and response and response[0]:
            uid_string = response[0].decode('utf-8')
            all_uids = [int(uid) for uid in uid_string.split() if uid]
            sorted_uids = sorted(all_uids)
            # Only fetch new emails (UID > last_uid) so we don't re-publish the same ones
            new_uids = [u for u in sorted_uids if u > last_uid]
            # Cap at 20 (latest 20 new emails per poll)
            uids = new_uids[-20:] if len(new_uids) > 20 else new_uids
            log.info("Found emails", total=len(all_uids), new_since_last=len(new_uids), processing=len(uids))
        else:
            uids = []
        
        # Fetch each email
        emails = []
        for uid in uids:
            status, msg_data = mail.uid('FETCH', str(uid), '(RFC822)')
            if status == 'OK':
                raw_email = msg_data[0][1]  # Email bytes
                msg = email.message_from_bytes(raw_email)
                
                # Extract email data
                email_dict = {
                    'uid': uid,
                    'subject': msg.get('Subject', ''),
                    'from_addr': msg.get('From', ''),
                    'date': msg.get('Date', ''),
                    'message_id': msg.get('Message-ID', ''),
                    'raw': raw_email
                }
                
                emails.append(email_dict)
        
        log.info("Polled emails", count=len(emails), last_uid=last_uid, new_last_uid=max(uids) if uids else last_uid)
        return emails
        
    except Exception as e:
        log.error("Error polling emails", error=str(e))
        return []


def publish_email(r: redis.Redis, email_data: dict, mailbox_id: str):
    """
    Publish email to raw_emails.v1 stream
    
    Args:
        r: Redis connection
        email_data: Email dict with uid, subject, from_addr, date, raw, message_id
        mailbox_id: Email address (e.g., "user@gmail.com")
    """
    try:
        # Extract external_id
        external_id = email_data.get('message_id') or str(email_data.get('uid'))
        
        # Determine provider
        if '@gmail.com' in mailbox_id:
            provider = 'gmail'
        elif '@outlook.com' in mailbox_id or '@hotmail.com' in mailbox_id:
            provider = 'outlook'
        else:
            provider = mailbox_id.split('@')[1].split('.')[0] if '@' in mailbox_id else 'unknown'
        
        # Build idempotency key
        idemp_key = build_idempotency_key(provider, mailbox_id, external_id)
        
        # Generate trace_id
        trace_id = str(int(time.time() * 1000))
        
        # For now, use current timestamp
        received_ts = str(int(time.time()))
        
        # Encode raw email bytes as base64 for Redis (avoids encoding issues)
        raw_email_b64 = base64.b64encode(email_data.get('raw', b'')).decode('utf-8')

        
        # Publish to Redis Stream
        message_id = r.xadd('raw_emails.v1', {
            'trace_id': trace_id,
            'mailbox_id': mailbox_id,
            'received_ts': received_ts,
            'idemp_key': idemp_key,
            'subject': email_data.get('subject', ''),
            'external_id': external_id,
            'raw_email_b64': raw_email_b64  # Base64 encoded raw email
        })
        
        log.info("Published email", message_id=message_id, idemp_key=idemp_key, subject=email_data.get('subject'))
        
    except Exception as e:
        log.error("Error publishing email", error=str(e))


def main():
    """
    Main service loop.
    Phase 3: Polls IMAP and publishes real emails
    """
    log.info("imap_poller starting...")

    r = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

    # Connect to IMAP
    mail = connect_to_imap()
    if not mail:
        log.error("Failed to connect to IMAP. Exiting.")
        return
    
    # Get mailbox_id from environment
    mailbox_id = os.getenv('EMAIL_USER', '')
    if not mailbox_id:
        log.error("EMAIL_USER not set. Exiting.")
        return
    
    log.info("imap_poller ready", mailbox_id=mailbox_id)

    # One shot: fetch latest 20 emails, publish to pipeline, then exit. Pipeline processes only these 20.
    last_uid = 0
    emails = poll_emails(mail, last_uid)
    if emails:
        log.info("Publishing 20 emails to pipeline, then exiting", count=len(emails))
        for email_data in emails:
            publish_email(r, email_data, mailbox_id)
        log.info("Done. Published 20 emails. Exiting.")
    else:
        log.info("No emails to publish. Exiting.")


if __name__ == "__main__":
    main()