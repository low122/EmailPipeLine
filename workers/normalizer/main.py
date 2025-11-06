"""
Normalizer Worker - Phase 4
Consumes from raw_emails.v1, processes, publishes to emails.normalized.v1
"""

import structlog
import time
import os
import redis
import email
import re
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

log = structlog.get_logger(service="normalizer")

def html_to_text(html: str) -> str:
    """Simple HTML to text converter"""
    if not html:
        return ""
    
    # Remove script and style tags
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', html)
    
    # Decode HTML entities (simple)
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '')
    text = text.replace('&quot;', '"')
    
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def strip_trackers(text: str) -> str:
    """Remove tracking URLs and pixels"""
    if not text:
        return ""
    
    # Remove common tracking patterns
    # Remove URLs with utm_ parameters
    text = re.sub(r'https?://[^\s]*utm_[^\s\)]*', '', text)
    
    # Remove 1x1 pixel URLs (common tracking pixels)
    text = re.sub(r'https?://[^\s]*1x1[^\s\)]*', '', text)
    
    # Remove image tracking pixels (data URLs)
    text = re.sub(r'<img[^>]*src=["\'][^"\']*track[^"\']*["\'][^>]*>', '', text, flags=re.IGNORECASE)
    
    return text.strip()


def parse_email(raw_email_bytes: bytes) -> dict:
    """
    Parse MIME email and extract text content
    
    Args:
        raw_email_bytes: Raw email bytes from Redis message
    
    Returns:
        Dict with: text_content, html_content, body_hash
    """
    try:
        msg = email.message_from_bytes(raw_email_bytes)
        
        text_content = ""
        html_content = ""

        if msg.is_multipart():
            # Walk through all parts
            for part in msg.walk():
                content_type = part.get_content_type()
                
                if content_type == "text/plain":
                    text_bytes = part.get_payload(decode=True)
                    if text_bytes:
                        text_content += text_bytes.decode('utf-8', errors='ignore')
                
                elif content_type == "text/html":
                    html_bytes = part.get_payload(decode=True)
                    if html_bytes:
                        html_content += html_bytes.decode('utf-8', errors='ignore')
        else:
            # Single part email
            content_type = msg.get_content_type()
            if content_type == "text/plain":
                text_bytes = msg.get_payload(decode=True)
                text_content = text_bytes.decode('utf-8', errors='ignore') if text_bytes else ""
            elif content_type == "text/html":
                html_bytes = msg.get_payload(decode=True)
                html_content = html_bytes.decode('utf-8', errors='ignore') if html_bytes else ""
        
        # Use the cleaned text content
        final_text = text_content or html_to_text(html_content)
        final_text = strip_trackers(final_text)
        body_hash = hashlib.sha256(final_text.encode('utf-8')).hexdigest()
    
        #   {text_content, html_content, body_hash}
        return {'text_content': final_text, 'html_content': html_content, 'body_hash': body_hash}
        
    except Exception as e:
        log.error("Error parsing email", error=str(e))
        return {'text_content': '', 'html_content': '', 'body_hash': ''}


def publish_normalized(r: redis.Redis, normalized_data: dict, original_fields: dict):
    """
    Publish normalized email to emails.normalized.v1 stream
    
    Args:
        r: Redis connection
        normalized_data: Dict with text_content, html_content, body_hash
        original_fields: Original Redis message fields (trace_id, mailbox_id, idemp_key, etc.)
    """
    try:
        # Combine original metadata with normalized content
        message_data = {
            'trace_id': original_fields.get('trace_id', ''),
            'mailbox_id': original_fields.get('mailbox_id', ''),
            'idemp_key': original_fields.get('idemp_key', ''),
            'body_hash': normalized_data.get('body_hash', ''),
            'text_content': normalized_data.get('text_content', '')[:1000],  # Limit size
            'subject': original_fields.get('subject', ''),
            'external_id': original_fields.get('external_id', ''),
            'received_ts': original_fields.get('received_ts', '')
        }
        
        # Publish to normalized stream
        message_id = r.xadd('emails.normalized.v1', message_data)
        log.info("Published normalized email", 
                stream_message_id=message_id, 
                idemp_key=original_fields.get('idemp_key'),
                body_hash=normalized_data.get('body_hash')[:16])  # First 16 chars for logging
        
    except Exception as e:
        log.error("Error publishing normalized email", error=str(e))


def main():
    """
    Main service loop.
    Phase 4: Consumes from raw_emails.v1, normalizes, publishes to emails.normalized.v1
    """
    log.info("normalizer starting...")

    r = redis.Redis(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

    try:
        r.xgroup_create('raw_emails.v1', 'normalizer-g', id='0', mkstream=True)
        log.info("Created consumer group normalizer-g")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    log.info("normalizer ready")

    consumer_name = f"normalizer-{os.getpid()}"

    while True:
        try:
            # Consume
            messages = r.xreadgroup(
                'normalizer-g',
                consumer_name,
                {'raw_emails.v1': '>'},
                count=1,
                block=1000
            )

            # Process
            if messages:
                stream, data = messages[0]
                message_id, fields = data[0]

                log.info("Processing message", message_id=message_id, subject=fields.get('subject'))

                # Extract raw email bytes from base64
                raw_email_b64 = fields.get('raw_email_b64', '')

                if raw_email_b64:
                    try:
                        raw_email_bytes = base64.b64decode(raw_email_b64)
                        
                        # Parse email
                        normalized_data = parse_email(raw_email_bytes)
                        
                        if normalized_data.get('body_hash'):
                            # Publish normalized email
                            publish_normalized(r, normalized_data, fields)
                            
                            log.info("Normalized email", 
                                    body_hash=normalized_data.get('body_hash')[:16],
                                    text_length=len(normalized_data.get('text_content', '')))
                        else:
                            log.warning("Failed to parse email, skipping", message_id=message_id)
                    
                    except Exception as e:
                        log.error("Error processing email", error=str(e), message_id=message_id)
                
                # ACK the original message
                r.xack('raw_emails.v1', 'normalizer-g', message_id)
                log.info("Acknowledged message", message_id=message_id)

            time.sleep(1)
        except Exception as e:
            log.error("Error consuming message", error=str(e))
            time.sleep(5)


if __name__ == "__main__":
    main()