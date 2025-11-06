"""
Persister Worker - Phase 6
Consumes from emails.classified.v1, saves messages and classifications to PostgreSQL
"""

import structlog
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import redis
import psycopg2
import psycopg2.extras

# Load environment variables from .env file
load_dotenv()

# Configure structured logging for consistent output across all services
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),      # Add ISO timestamp
        structlog.processors.add_log_level,                # Add log level (INFO, ERROR, etc.)
        structlog.dev.ConsoleRenderer()                    # Human-readable format for dev
    ]
)

# Create logger bound to this service's name
log = structlog.get_logger(service="persister")

def connect_to_postgres():
    """
    Connect to PostgreSQL database
    
    Returns:
        psycopg2 connection or None if failed
    """
    try:
        host = os.getenv('DB_HOST', 'postgres')
        port = os.getenv('DB_PORT', '5432')
        dbname = os.getenv('DB_NAME', 'email_pipeline')
        user = os.getenv('DB_USER', 'pipeline_user')
        password = os.getenv('DB_PASSWORD', 'pipeline_pass')
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        
        log.info("Connected to PostgreSQL", host=host, dbname=dbname)
        return conn
        
    except Exception as e:
        log.error("PostgreSQL connection failed", error=str(e))
        return None

def save_message(conn, fields: dict) -> int:
    """
    Save or update message in messages table
    
    Args:
        conn: PostgreSQL connection
        fields: Redis message fields (idemp_key, mailbox_id, external_id, subject, body_hash, received_ts)
    
    Returns:
        message_id (int) or None if failed
    """
    try:
        idemp_key = fields.get('idemp_key', '')
        mailbox_id = fields.get('mailbox_id', '')
        external_id = fields.get('external_id', '')
        subject = fields.get('subject', '')
        body_hash = fields.get('body_hash', '')
        received_ts = fields.get('received_ts', '')

        log.info("Receive timestamp", received_ts=received_ts, has_value=bool(received_ts))

        received_at = None
        if received_ts:
            try:
                received_at = datetime.fromtimestamp(int(received_ts))
            except (ValueError, TypeError):
                received_at = None
        
        sql = """
INSERT INTO messages (idemp_key, mailbox_id, external_id, subject, body_hash, received_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (idemp_key) DO UPDATE SET
    subject = EXCLUDED.subject,
    body_hash = EXCLUDED.body_hash,
    received_at = EXCLUDED.received_at,
    updated_at = NOW()
RETURNING id;
"""
        
        cursor = conn.cursor()
        cursor.execute(sql, (idemp_key, mailbox_id, external_id, subject, body_hash, received_at))
        message_id = cursor.fetchone()[0]  # Get the returned id
        cursor.close()
        
        conn.commit()
        
        log.info("Saved message", message_id=message_id, idemp_key=idemp_key)
        return message_id
        
    except Exception as e:
        log.error("Error saving message", error=str(e))
        conn.rollback()  # Rollback on error
        return None


def save_classification(conn, message_id: int, fields: dict) -> bool:
    """
    Save or update classification in classifications table
    
    Args:
        conn: PostgreSQL connection
        message_id: ID from messages table (foreign key)
        fields: Redis message fields (vendor, amount_cents, currency, class, confidence)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        vendor = fields.get('vendor', '')
        amount_cents_str = fields.get('amount_cents', '0')
        currency = fields.get('currency', '')
        class_type = fields.get('class', '')
        confidence_str = fields.get('confidence', '0.0')

        # Convert to proper types
        amount_cents = int(amount_cents_str) if amount_cents_str else 0
        confidence = float(confidence_str) if confidence_str else 0.0
        
        sql = """
INSERT INTO classifications (message_id, vendor, amount_cents, currency, class, confidence)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (message_id) DO UPDATE SET
    vendor = EXCLUDED.vendor,
    amount_cents = EXCLUDED.amount_cents,
    currency = EXCLUDED.currency,
    class = EXCLUDED.class,
    confidence = EXCLUDED.confidence,
    updated_at = NOW();
"""
        
        cursor = conn.cursor()
        cursor.execute(sql, (message_id, vendor, amount_cents, currency, class_type, confidence))
        cursor.close()
        
        conn.commit()
        
        log.info("Saved classification", 
        message_id=message_id,
        vendor=vendor,
        amount_cents=amount_cents,
        confidence=confidence)
        return True
        
    except Exception as e:
        log.error("Error saving classification", error=str(e))
        conn.rollback()
        return False


def main():
    """
    Main service loop.
    Phase 6: Consumes from emails.classified.v1, saves to PostgreSQL
    """
    log.info("persister starting...")

    r = redis.Redis(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

    conn = connect_to_postgres()
    if not conn:
        log.error("Failed to connect to PostgreSQL. Exiting.")
        return

    try:
        r.xgroup_create('emails.classified.v1', 'persister-g', id='0', mkstream=True)
        log.info("Created consumer group persister-g")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    log.info("persister ready")

    consumer_name = f"persister-{os.getpid()}"

    while True:
        try:
            # Consume
            messages = r.xreadgroup(
                'persister-g',
                consumer_name,
                {'emails.classified.v1': '>'},
                count=1,
                block=1000
            )

            if messages:
                stream, data = messages[0]
                message_id, fields = data[0]

                log.info("Processing classified email", 
                        message_id=message_id,
                        vendor=fields.get('vendor'))

                # Save message first (returns message_id)
                db_message_id = save_message(conn, fields)
                
                if db_message_id:
                    # Save classification (linked to message)
                    save_classification(conn, db_message_id, fields)
                    log.info("Persisted email", 
                            db_message_id=db_message_id,
                            vendor=fields.get('vendor'))
                else:
                    log.warning("Failed to save message, skipping classification")

                # ACK
                r.xack('emails.classified.v1', 'persister-g', message_id)
                log.info("Acknowledged message", message_id=message_id)

            time.sleep(1)
        except Exception as e:
            log.error("Error consuming message", error=str(e))
            time.sleep(5)


if __name__ == "__main__":
    main()