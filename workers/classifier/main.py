"""
Classifier Worker - Phase 5
Consumes from emails.to_classify.v1 (watcher output), classifies with Claude API, publishes to emails.classified.v1
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

def classify_email_with_claude(
    text_content: str,
    subject: str,
    from_addr: str,
    watcher_name: str | None = None,
    query_text: str | None = None,
) -> dict:
    """
    Classify email using Claude API, driven by user's watcher intent.

    Args:
        text_content: Cleaned email text content
        subject: Email subject
        from_addr: Email sender
        watcher_name: User-defined watcher name (e.g. "Billing", "Flight Confirmations")
        query_text: User's semantic query describing what to match

    Returns:
        Dict with: vendor, amount_cents, currency, class, confidence
        class is set to watcher_name when provided
    """
    try:
        api_key = os.getenv('CLAUDE_API_KEY')
        if not api_key:
            log.error("CLAUDE_API_KEY not set")
            return {}
        
        client = anthropic.Anthropic(api_key=api_key)

        # Use watcher-driven prompt when user intent is known
        if watcher_name and query_text:
            prompt = f"""
You classify emails based on user-defined intent (watchers).

USER INTENT:
- Watcher name: {watcher_name}
- User query: {query_text}

EMAIL INPUT:
From: {from_addr}
Subject: {subject}
Body: {text_content[:2000]}

TASK:
1. Determine if this email matches the user's intent (confidence 0.0–1.0).
2. Put ALL extracted info into extracted_data. Only include fields that are present.
   Examples by watcher type:
   - Billing: {{"vendor": "Netflix", "amount_cents": 1999, "currency": "USD", "invoice_id": "..."}}
   - Flights: {{"airline": "United", "flight_number": "UA123", "departure": "2025-02-15", "confirmation": "..."}}
   - Rentals: {{"company": "Hertz", "pickup_date": "2025-02-20"}}
   - Use empty {{}} if nothing to extract
3. Set class to exactly: "{watcher_name}"
4. Do not infer information not explicitly present.

OUTPUT FORMAT:
Return exactly one JSON object, with no text or explanations.

```json
{{
  "class": "{watcher_name}",
  "confidence": <float between 0.0 and 1.0>,
  "extracted_data": {{ <all relevant key-value pairs for this watcher type> }}
}}"""
        else:
            # Fallback: no watcher context (e.g. legacy path)
            prompt = f"""
You classify emails. Extract relevant info into extracted_data.

INPUT:
From: {from_addr}
Subject: {subject}
Body: {text_content[:2000]}

OUTPUT FORMAT:
Return exactly one JSON object, with no text or explanations.

```json
{{
  "class": "<category or empty string>",
  "confidence": <float between 0.0 and 1.0>,
  "extracted_data": {{}}
}}"""

        
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

        # Parse JSON from response (may contain nested extracted_data)
        json_match = re.search(r'```json\s*([\s\S]*?)\s*```', content_text)
        if json_match:
            json_str = json_match.group(1).strip()
        else:
            json_match = re.search(r'\{[\s\S]*\}', content_text)
            json_str = json_match.group(0) if json_match else ""
        if json_str:
            result = json.loads(json_str)
        else:
            log.warning("No JSON found in Claude response")
            return {}
        
        # Return normalized dict (MUST: class, confidence; all else in extracted_data)
        extracted = result.get('extracted_data')
        if not isinstance(extracted, dict):
            extracted = {}
        classification = {
            'class': result.get('class', ''),
            'confidence': float(result.get('confidence', 0.0)),
            'extracted_data': extracted,
        }
        
        log.info("Classified email", class_=classification['class'], confidence=classification['confidence'])
        
        return classification
        
    except json.JSONDecodeError as e:
        log.error("Failed to parse JSON from Claude response", error=str(e))
        return {}
    except Exception as e:
        log.error("Error classifying email", error=str(e))
        return {}


def publish_classified(r: redis.Redis, classification: dict, original_fields: dict):
    """
    Publish classified email.

    Args:
        r: Redis connection
        classification: Dict with vendor, amount_cents, currency, class, confidence, extracted_data
        original_fields: Original Redis message fields (includes filter_watcher_id)
    """
    try:
        extracted = classification.get('extracted_data') or {}
        message_data = {
            'trace_id': original_fields.get('trace_id', ''),
            'mailbox_id': original_fields.get('mailbox_id', ''),
            'idemp_key': original_fields.get('idemp_key', ''),
            'body_hash': original_fields.get('body_hash', ''),
            'subject': original_fields.get('subject', ''),
            'external_id': original_fields.get('external_id', ''),
            'received_ts': original_fields.get('received_ts', ''),
            'class': classification.get('class', ''),
            'confidence': str(classification.get('confidence', 0.0)),
            'watcher_id': original_fields.get('filter_watcher_id', ''),
            'extracted_data': json.dumps(extracted) if extracted else '{}',
        }
        
        message_id = r.xadd('emails.classified.v1', message_data)
        
        log.info("Published classified email", stream_message_id=message_id, class_=classification.get('class'))
        
    except Exception as e:
        log.error("Error publishing classified email", error=str(e))



def main():
    """
    Main service loop.
    Phase 5: Consumes from emails.normalized.v1, classifies with Claude, publishes to emails.classified.v1
    """
    log.info("classifier starting...")

    r = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

    # Create consumer group for emails.to_classify.v1 (watcher publishes here)
    try:
        r.xgroup_create('emails.to_classify.v1', 'classifier-g', id='0', mkstream=True)
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
                {'emails.to_classify.v1': '>'},
                count=1,
                block=1000
            )

            if messages:
                stream, data = messages[0]
                message_id, fields = data[0]

                log.info("Processing normalized email", message_id=message_id, subject=fields.get('subject'))

                text_content = fields.get('text_content', '')

                if text_content:
                    watcher_name = fields.get('filter_watcher_name')
                    query_text = fields.get('filter_query_text')

                    classification = classify_email_with_claude(
                        text_content=text_content,
                        subject=fields.get('subject', ''),
                        from_addr=fields.get('mailbox_id', ''),
                        watcher_name=watcher_name or None,
                        query_text=query_text or None,
                    )

                    # Publish when: watcher-matched (we trust the semantic filter) or confidence >= 0.7
                    has_watcher = bool(watcher_name)
                    confident = classification.get('confidence', 0) >= 0.7
                    if has_watcher or confident or classification.get('extracted_data'):
                        # Ensure class is set from watcher when present
                        if has_watcher and not classification.get('class'):
                            classification = {**classification, 'class': watcher_name}
                        publish_classified(r, classification, fields)
                    else:
                        log.debug("No classification found, skipping", message_id=message_id)
                else:
                    log.warning("No text content to classify", message_id=message_id)

                # ACK the message (we read from emails.to_classify.v1)
                r.xack('emails.to_classify.v1', 'classifier-g', message_id)
                log.info("Acknowledged message", message_id=message_id)

            time.sleep(1)
        except Exception as e:
            log.error("Error consuming message", error=str(e))
            time.sleep(5)


if __name__ == "__main__":
    main()
