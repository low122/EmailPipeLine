"""
Classifier Worker - Phase 5
Consumes from emails.normalized.v1, classifies with Claude API, publishes classified emails
"""

import structlog
import time
import os
from dotenv import load_dotenv
import redis
import anthropic
import json, re

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer()
    ]
)

log = structlog.get_logger(service="classifier")

def classify_email_with_claude(text_content: str, subject: str, from_addr: str) -> dict:
    """
    Classify email using Claude API
    
    Args:
        text_content: Cleaned email text content
        subject: Email subject
        from_addr: Email sender
    
    Returns:
        Dict with: vendor, amount_cents, currency, class, confidence
        Returns empty dict if classification fails
    """
    try:
        api_key = os.getenv('CLAUDE_API_KEY')
        if not api_key:
            log.error("CLAUDE_API_KEY not set")
            return {}
        
        client = anthropic.Anthropic(api_key=api_key)
        
        prompt = f"""
You are an email classifier that extracts *only* active, successful subscription or renewal payments.

INPUT:
From: {from_addr}
Subject: {subject}
Body: {text_content[:2000]}

RULES:
1. Identify only **active or successful recurring subscription payments or renewals**.
2. **Ignore**:
   - Failed payments or payment declines
   - Cancelled subscriptions
   - One-time purchases
   - Free trials
   - Marketing emails, promotions, alerts, or receipts with $0 amount
3. Do not infer information not explicitly present.

OUTPUT FORMAT:
Return exactly one JSON object, with no text or explanations.

```json
{{
  "vendor": "<service name or empty string>",
  "amount_cents": <integer amount in cents or 0>,
  "currency": "<currency code or empty string>",
  "class": "subscription" or "",
  "confidence": <float between 0.0 and 1.0>
}}```"""

        
        response = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )

        # Extract response text
        content_text = response.content[0].text if response.content else ""

        if not content_text.strip():
            log.warning("Empty response from Claude")
            return {}

        # Parse JSON from response
        # Try to find JSON in ```json``` code blocks first
        json_match = re.search(r'```json\s*(\{[^`]*?\})\s*```', content_text, re.DOTALL)

        # If not found, try standalone JSON
        if not json_match:
            json_match = re.search(r'\{[^{}]*"vendor"[^{}]*\}', content_text, re.DOTALL)

        if json_match:
            # Extract JSON string
            json_str = json_match.group(1) if json_match.lastindex else json_match.group(0)
            result = json.loads(json_str.strip())
        else:
            log.warning("No JSON found in Claude response")
            return {}
        
        # Return normalized dict
        classification = {
            'vendor': result.get('vendor', ''),
            'amount_cents': int(result.get('amount_cents', 0)) if result.get('amount_cents') else 0,
            'currency': result.get('currency', ''),
            'class': result.get('class', ''),
            'confidence': float(result.get('confidence', 0.0))
        }
        
        log.info("Classified email", 
                vendor=classification['vendor'],
                amount_cents=classification['amount_cents'],
                confidence=classification['confidence'])
        
        return classification
        
    except json.JSONDecodeError as e:
        log.error("Failed to parse JSON from Claude response", error=str(e))
        return {}
    except Exception as e:
        log.error("Error classifying email", error=str(e))
        return {}


def publish_classified(r: redis.Redis, classification: dict, original_fields: dict):
    """
    Publish classified email
    
    Args:
        r: Redis connection
        classification: Dict with vendor, amount_cents, currency, class, confidence
        original_fields: Original Redis message fields from normalized stream
    """
    try:
        message_data = {
            'trace_id': original_fields.get('trace_id', ''),
            'mailbox_id': original_fields.get('mailbox_id', ''),
            'idemp_key': original_fields.get('idemp_key', ''),
            'body_hash': original_fields.get('body_hash', ''),
            'subject': original_fields.get('subject', ''),
            'external_id': original_fields.get('external_id', ''),
            'received_ts': original_fields.get('received_ts', ''),
            'vendor': classification.get('vendor', ''),
            'amount_cents': str(classification.get('amount_cents', 0)),
            'currency': classification.get('currency', ''),
            'class': classification.get('class', ''),
            'confidence': str(classification.get('confidence', 0.0))
        }
        
        message_id = r.xadd('emails.classified.v1', message_data)
        
        log.info("Published classified email",
            stream_message_id=message_id,
            vendor=classification.get('vendor'),
            amount_cents=classification.get('amount_cents')
        )
        
    except Exception as e:
        log.error("Error publishing classified email", error=str(e))



def main():
    """
    Main service loop.
    Phase 5: Consumes from emails.normalized.v1, classifies with Claude, publishes to emails.classified.v1
    """
    log.info("classifier starting...")

    r = redis.Redis(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

    # Create consumer group for emails.normalized.v1 (classifier consumes from this)
    try:
        r.xgroup_create('emails.normalized.v1', 'classifier-g', id='0', mkstream=True)
        log.info("Created consumer group classifier-g")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        
    log.info("classifier ready")

    consumer_name = f"classifier-{os.getpid()}"

    while True:
        try:
            messages = r.xreadgroup(
                'classifier-g',
                consumer_name,
                {'emails.normalized.v1': '>'},
                count=1,
                block=1000
            )

            if messages:
                stream, data = messages[0]
                message_id, fields = data[0]

                log.info("Processing normalized email", message_id=message_id, subject=fields.get('subject'))

                text_content = fields.get('text_content', '')

                if text_content:
                    classification = classify_email_with_claude(
                        text_content=text_content,
                        subject=fields.get('subject', ''),
                        from_addr=fields.get('mailbox_id', '')
                    )

                    # Publish if classification found
                    if classification.get('vendor') or classification.get('confidence', 0) >= 0.7:
                        publish_classified(r, classification, fields)
                    else:
                        log.debug("No classification found, skipping", message_id=message_id)
                else:
                    log.warning("No text content to classify", message_id=message_id)

                # ACK the message
                r.xack('emails.normalized.v1', 'classifier-g', message_id)
                log.info("Acknowledged message", message_id=message_id)

            time.sleep(1)
        except Exception as e:
            log.error("Error consuming message", error=str(e))
            time.sleep(5)


if __name__ == "__main__":
    main()
