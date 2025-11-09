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
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import anthropic
import psycopg2
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

def connect_to_postgres():
    """
    Connect to PostgreSQL
    
    Returns:
        PostgreSQL connection or None if failed
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'postgres'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'email_pipeline'),
            user=os.getenv('DB_USER', 'pipeline_user'),
            password=os.getenv('DB_PASSWORD', 'pipeline_pass')
        )
        log.info("Connected to PostgreSQL successfully")
        return conn
    except Exception as e:
        log.error("PostgreSQL connection failed", error=str(e))
        return None

def poll_emails(mail, last_uid=0, is_initial_scan=False):
    """
    Poll for emails with Gmail
    
    Args:
        mail: IMAP connection
        last_uid: Last processed UID (for incremental scans)
        is_initial_scan: If True, scan past 15 months
        mailbox_id: Email address (to detect Gmail and filter Primary)
    
    Returns:
        List of email dicts with: uid, subject, from_addr, date, raw, message_id
    """
    try:
        status, _ = mail.select("INBOX")
        
        # Build search criteria
        if is_initial_scan:
            # Initial scan: get emails from past 15 months
            fifteen_months_ago = datetime.now() - timedelta(days=450)
            date_str = fifteen_months_ago.strftime("%d-%b-%Y")
            search_criteria = f'(SINCE {date_str})'
        else:
            # Incremental scan: only new emails since last_uid
            if last_uid > 0:
                search_criteria = f'UID {last_uid + 1}:*'
            else:
                search_criteria = 'ALL'
        
        # Search for UIDs
        status, response = mail.uid('SEARCH', None, search_criteria)
        
        # Parse UID list
        if status == 'OK' and response and response[0]:
            uid_string = response[0].decode('utf-8')
            all_uids = [int(uid) for uid in uid_string.split() if uid]

            if not is_initial_scan and last_uid > 0:
                uids = [u for u in all_uids if u > last_uid]
            else:
                uids = all_uids

            uids = sorted(uids)
            log.info("Found emails111", count=len(uids), is_initial=is_initial_scan, search_criteria=search_criteria)
        else:
            uids = []

        MAX_EMAILS = 100
        uids = uids[-MAX_EMAILS:]
        log.info("Limiting to latest emails", limited_count=len(uids))
        
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
                log.info("Email adding...", count=len(emails))
        
        log.info("Polled emails", count=len(emails), last_uid=last_uid, new_last_uid=max(uids) if uids else last_uid)
        return emails
        
    except Exception as e:
        log.error("Error polling emails", error=str(e))
        return []


def publish_email(r: redis.Redis, email_data: dict, mailbox_id: str):
    """
    Publish email to raw_emails.v1 stream (only if subject classification passes)
    
    Args:
        r: Redis connection
        email_data: Email dict with uid, subject, from_addr, date, raw, message_id
        mailbox_id: Email address (e.g., "user@gmail.com")
    """
    try:
        subject = email_data.get('subject', '')
        from_addr = email_data.get('from_addr', '')
        
        subject_classification = classify_subject_with_ai(subject, from_addr)
        confidence = subject_classification.get('confidence', 0.0)
        is_subscription = subject_classification.get('is_subscription', False)

        log.info("Subject classification", 
                subject=subject[:50], 
                confidence=confidence, 
                is_subscription=is_subscription)
        
        # Step 2: Only proceed if confidence >= 70%
        if confidence < 0.7 or not is_subscription:
            log.debug("Subject classification below threshold, skipping", 
                     subject=subject[:50], 
                     confidence=confidence)
            return


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
        
        # Wrap the timeframe
        received_ts = str(int(time.time()))
        email_date_str = email_data.get('date', '')
        if email_date_str:
            try:
                from email.utils import parsedate_to_datetime
                email_date = parsedate_to_datetime(email_date_str)
                if email_date:
                    received_ts = str(int(email_date.timestamp()))
            except (ValueError, TypeError, AttributeError) as e:
                log.warning("Failed to parse email date, using current time")
        
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


def classify_subject_with_ai(subject: str, from_addr: str) -> dict:
    """
    Classify email subject line using AI to determine if it's subscription-related
    
    Args:
        subject: Email subject line
        from_addr: Email sender address
    
    Returns:
        Dict with: confidence (0.0 to 1.0), is_subscription (bool)
        Returns empty dict if classification fails
    """
    try:
        api_key = os.getenv('CLAUDE_API_KEY')
        if not api_key:
            log.error("CLAUDE_API_KEY not set")
            return {}

        log.info("Sending to AI...")
        
        client = anthropic.Anthropic(api_key=api_key)
        
        prompt = f"""
You are an email classifier that analyzes ONLY the subject line to determine if an email is likely a subscription-related email (payment, renewal, receipt, billing).

INPUT:
From: {from_addr}
Subject: {subject}

TASK:
Determine if this email subject line suggests it's a subscription-related email (payment confirmation, renewal notice, receipt, billing statement).

OUTPUT FORMAT:
Return exactly one JSON object, with no text or explanations.
json
{{
  "confidence": <float between 0.0 and 1.0>,
  "is_subscription": <true or false>
}}Only return high confidence (>= 0.7) if the subject clearly indicates subscription/payment content.
"""

        response = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )

        content_text = response.content[0].text if response.content else ""

        import json
        import re
        json_match = re.search(r'\s*(\{[^`]*?\})\s*```', content_text, re.DOTALL)
        if not json_match:
            json_match = re.search(r'\{[^{}]*"confidence"[^{}]*\}', content_text, re.DOTALL)
        
        if json_match:
            json_str = json_match.group(1) if json_match.lastindex else json_match.group(0)
            result = json.loads(json_str.strip())
            
            return {
                'confidence': float(result.get('confidence', 0.0)),
                'is_subscription': bool(result.get('is_subscription', False))
            }
        else:
            log.warning("No JSON found in Claude subject classification response")
            return {}
            
    except Exception as e:
        log.error("Error classifying subject", error=str(e))
        return {}

def get_scan_status(conn, mailbox_id: str) -> dict:
    """
    Get scan status for mailbox
    
    Args:
        conn: PostgreSQL connection
        mailbox_id: Email address
    
    Returns:
        Dict with: initial_scan_completed, last_scan_uid
    """

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT initial_scan_completed, last_scan_uid FROM mailbox_scan_status WHERE mailbox_id = %s",
            (mailbox_id,)
        )
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return {
                'initial_scan_completed': result[0],
                'last_scan_uid': result[1] or 0
            }
        else:
            # First time - create record
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO mailbox_scan_status (mailbox_id, initial_scan_completed, last_scan_uid) VALUES (%s, FALSE, 0) RETURNING initial_scan_completed, last_scan_uid",
                (mailbox_id,)
            )
            conn.commit()
            result = cursor.fetchone()
            cursor.close()
            return {
                'initial_scan_completed': False,
                'last_scan_uid': 0
            }
    except Exception as e:
        log.error("Error getting scan status", error=str(e))
        return {'initial_scan_completed': False, 'last_scan_uid': 0}

def update_scan_status(conn, mailbox_id: str, last_uid: int, initial_completed: bool = False):
    """
    Update scan status after processing
    
    Args:
        conn: PostgreSQL connection
        mailbox_id: Email address
        last_uid: Last processed UID
        initial_completed: Whether initial scan is done
    """
    try:
        cursor = conn.cursor()
        if initial_completed:
            cursor.execute(
                """UPDATE mailbox_scan_status 
                   SET initial_scan_completed = TRUE, 
                       last_scan_uid = %s, 
                       initial_scan_date = NOW(),
                       updated_at = NOW()
                   WHERE mailbox_id = %s""",
                (last_uid, mailbox_id)
            )
        else:
            cursor.execute(
                """UPDATE mailbox_scan_status 
                   SET last_scan_uid = %s, 
                       updated_at = NOW()
                   WHERE mailbox_id = %s""",
                (last_uid, mailbox_id)
            )
        conn.commit()
        cursor.close()
        log.info("Updated scan status", mailbox_id=mailbox_id, last_uid=last_uid)
    except Exception as e:
        log.error("Error updating scan status", error=str(e))
        conn.rollback()

def process_email_batch(email_batch: list, r: redis.Redis, mailbox_id: str) -> dict:
    """
    Process a batch of emails in parallel
    
    Args:
        email_batch: List of email dicts
        r: Redis connection
        mailbox_id: Email address
    
    Returns:
        Dict with published_count and max_uid
    """
    published_count = 0
    max_uid = 0

    def process_single_email(email_data):
        """Process a single email and return result"""
        try:
            # Check if email should be published (AI classification happens inside)
            # We need to check classification first to decide if we publish
            subject = email_data.get('subject', '')
            from_addr = email_data.get('from_addr', '')
            
            subject_classification = classify_subject_with_ai(subject, from_addr)
            confidence = subject_classification.get('confidence', 0.0)
            is_subscription = subject_classification.get('is_subscription', False)

            log.info(f"Checking {subject}:", confidence=confidence, is_subscription=is_subscription)
            
            if confidence >= 0.7 and is_subscription:
                # Publish the email
                publish_email_internal(r, email_data, mailbox_id)
                return {'published': True, 'uid': email_data.get('uid', 0)}
            else:
                return {'published': False, 'uid': email_data.get('uid', 0)}
        except Exception as e:
            log.error("Error processing email in batch", error=str(e), uid=email_data.get('uid'))
            return {'published': False, 'uid': email_data.get('uid', 0)}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_single_email, email_data): email_data 
                  for email_data in email_batch}

        for future in as_completed(futures):
            try:
                log.info("Processed email [process_single_email]")

                result = future.result()
                if result['published']:
                    published_count += 1
                if result['uid'] > max_uid:
                    max_uid = result['uid']

            except Exception as e:
                log.error("Email failed [process_single_email]")
                
    
    return {'published_count': published_count, 'max_uid': max_uid}

def publish_email_internal(r: redis.Redis, email_data: dict, mailbox_id: str):
    """
    Internal publish function (without AI check, assumes already checked)
    This is the publishing logic extracted from publish_email
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
        
        # Wrap the timeframe
        received_ts = str(int(time.time()))
        email_date_str = email_data.get('date', '')
        if email_date_str:
            try:
                from email.utils import parsedate_to_datetime
                email_date = parsedate_to_datetime(email_date_str)
                if email_date:
                    received_ts = str(int(email_date.timestamp()))
            except (ValueError, TypeError, AttributeError) as e:
                log.warning("Failed to parse email date, using current time")
        
        # Encode raw email bytes as base64 for Redis
        raw_email_b64 = base64.b64encode(email_data.get('raw', b'')).decode('utf-8')
        
        # Publish to Redis Stream
        message_id = r.xadd('raw_emails.v1', {
            'trace_id': trace_id,
            'mailbox_id': mailbox_id,
            'received_ts': received_ts,
            'idemp_key': idemp_key,
            'subject': email_data.get('subject', ''),
            'external_id': external_id,
            'raw_email_b64': raw_email_b64
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
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

    # Connect to PostgreSQL for scan status tracking
    pg_conn = connect_to_postgres()
    if not pg_conn:
        log.warning("PostgreSQL connection failed, continuing without scan status tracking")

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

    # Get Scan status
    scan_status = {'initial_scan_completed': False, 'last_scan_uid': 0}
    if pg_conn:
        scan_status = get_scan_status(pg_conn, mailbox_id)

    is_initial_scan = not scan_status['initial_scan_completed']
    last_uid = scan_status['last_scan_uid']

    if is_initial_scan:
        log.info("Starting initial 15-month scan", mailbox_id=mailbox_id)
    else:
        log.info("Starting incremental scan", mailbox_id=mailbox_id, last_uid=last_uid)

    NUM_STREAMS = 3
    TEST_BATCH_SIZE = 100
    
    while True:
        try:
            # Poll for new emails
            emails = poll_emails(mail, last_uid, is_initial_scan)
            
            if emails:
                log.info("Found emails", count=len(emails), is_initial=is_initial_scan)
                
                # For testing: limit to TEST_BATCH_SIZE if initial scan
                if is_initial_scan and len(emails) > TEST_BATCH_SIZE:
                    emails = emails[:TEST_BATCH_SIZE]
                    log.info("Limiting to test batch", batch_size=TEST_BATCH_SIZE)
                
                # Process emails in parallel batches
                published_count = 0
                max_uid = last_uid
                
                # Process emails in chunks of NUM_STREAMS
                for i in range(0, len(emails), NUM_STREAMS):
                    batch = emails[i:i + NUM_STREAMS]
                    
                    # Process this batch in parallel
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        futures = {
                            executor.submit(publish_email, r, email_data, mailbox_id): email_data 
                            for email_data in batch
                        }
                        
                        for future in as_completed(futures):
                            email_data = futures[future]
                            try:
                                future.result()  # Wait for completion
                                published_count += 1
                                if email_data['uid'] > max_uid:
                                    max_uid = email_data['uid']
                            except Exception as e:
                                log.error("Error processing email", 
                                        error=str(e), 
                                        uid=email_data.get('uid'))
                    
                    # Log progress every batch
                    if (i + NUM_STREAMS) % 30 == 0 or i + NUM_STREAMS >= len(emails):
                        log.info("Processing progress", 
                                processed=min(i + NUM_STREAMS, len(emails)),
                                total=len(emails),
                                published=published_count)
                
                # Update last_uid
                if emails:
                    last_uid = max_uid
                    
                    # Update scan status in database
                    if pg_conn:
                        if is_initial_scan:
                            update_scan_status(pg_conn, mailbox_id, last_uid, initial_completed=True)
                            is_initial_scan = False
                            log.info("Initial scan completed", mailbox_id=mailbox_id, last_uid=last_uid)
                        else:
                            update_scan_status(pg_conn, mailbox_id, last_uid)
                    
                    log.info("Published emails", 
                            count=published_count, 
                            total_found=len(emails),
                            last_uid=last_uid,
                            streams=NUM_STREAMS)
            
            # After initial scan, switch to incremental mode
            if is_initial_scan and not emails:
                if pg_conn:
                    update_scan_status(pg_conn, mailbox_id, last_uid, initial_completed=True)
                is_initial_scan = False
                log.info("Initial scan completed (no more emails)", mailbox_id=mailbox_id)
            
            # Sleep longer for initial scan (processing many emails)
            sleep_time = 60 if is_initial_scan else 30
            time.sleep(sleep_time)
            
        except Exception as e:
            log.error("Error in polling loop", error=str(e))
            time.sleep(60)  # Wait longer on error


if __name__ == "__main__":
    main()