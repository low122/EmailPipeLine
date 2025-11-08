from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
from dotenv import load_dotenv
import structlog

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer()
    ]
)
log = structlog.get_logger(service="api")

app = FastAPI(title="Subscription Tracker API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your iOS app's origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection function
def get_db_connection():
    """Connect to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'postgres'),
            port=int(os.getenv('DB_PORT', '5432')),
            dbname=os.getenv('DB_NAME', 'email_pipeline'),
            user=os.getenv('DB_USER', 'pipeline_user'),
            password=os.getenv('DB_PASSWORD', 'pipeline_pass')
        )
        return conn
    except Exception as e:
        log.error("Database connection failed", error=str(e))
        raise

@app.get("/")
def root():
    return {"message": "Subscription Tracker API"}

@app.get("/api/subscription")
def get_subscription():
    """Get all subscriptions from database"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query="""
        SELECT 
            c.id,
            c.vendor,
            c.amount_cents / 100.0 as amount,
            c.currency,
            c.class,
            c.confidence,
            m.received_at,
            m.subject,
            m.mailbox_id
        FROM classifications c
        JOIN messages m ON c.message_id = m.id
        WHERE c.vendor != '' AND c.vendor IS NOT NULL
        ORDER BY m.received_at DESC;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        subscriptions = []
        for row in results:
            subscriptions.append({
                "id": row[0],
                "vendor": row[1],
                "amount": float(row[2]) if row[2] else 0.0,
                "currency": row[3] or "USD",
                "class": row[4],
                "confidence": float(row[5]) if row[5] else 0.0,
                "received_at": row[6].isoformat() if row[6] else None,
                "subject": row[7],
                "mailbox_id": row[8]
            })

        return {"subscriptions":subscriptions}

    except Exception as e:
        log.error("Error fetching subscriptions", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/api/spending/monthly")
def get_monthly_spending():
    """Get monthly spending breakdown"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        DATE_TRUNC('month', m.received_at) as month,
        SUM(c.amount_cents) / 100.0 as total_amount,
        c.currency
    FROM classifications c
    JOIN messages m ON c.message_id = m.id
    WHERE c.amount_cents > 0
    GROUP BY DATE_TRUNC('month', m.received_at), c.currency
    ORDER BY month DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return results

@app.get("/api/spending/total")
def get_total_spending():
    """Get total monthly subscription cost"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        SUM(c.amount_cents) / 100.0 as total_monthly,
        c.currency
    FROM classifications c
    WHERE c.amount_cents > 0
    GROUP BY c.currency;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return results

@app.get("/api/stats")
def get_stats():
    """Get pipeline statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    stats = {}
    cursor.execute("SELECT COUNT(*) FROM messages")
    stats["total_emails"] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT mailbox_id) FROM messages")
    stats["accounts"] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM classifications WHERE vendor != ''")
    stats["subscriptions"] = cursor.fetchone()[0]
    
    return stats

def detect_price_increases(conn):
    """Detect subscriptions with price increases"""
    cursor = conn.cursor()
    
    query = """
    WITH vendor_history AS (
        SELECT 
            vendor,
            amount_cents,
            received_at,
            ROW_NUMBER() OVER (PARTITION BY vendor ORDER BY received_at DESC) as rn
        FROM subscription_history
        WHERE vendor != '' AND amount_cents > 0
    ),
    current_prices AS (
        SELECT vendor, amount_cents, received_at
        FROM vendor_history
        WHERE rn = 1
    ),
    previous_prices AS (
        SELECT vendor, amount_cents, received_at
        FROM vendor_history
        WHERE rn = 2
    )
    SELECT 
        c.vendor,
        c.amount_cents as current_amount,
        p.amount_cents as previous_amount,
        (c.amount_cents - p.amount_cents) as increase_cents,
        c.received_at as current_date,
        p.received_at as previous_date
    FROM current_prices c
    JOIN previous_prices p ON c.vendor = p.vendor
    WHERE c.amount_cents > p.amount_cents
    ORDER BY increase_cents DESC;
    """

    cursor.execute(query)
    results = cursor.fetechall()
    cursor.close()

    notifications = []
    for row in results:
        vendor, current, previous, increase, current_date, previous_date = row
        increase_dollars = increase / 100.0
        
        # Format the date for the message
        if previous_date:
            month_name = previous_date.strftime("%B")
            notifications.append({
                "type": "price_increase",
                "vendor": vendor,
                "message": f"Your {vendor} plan increased by ${increase_dollars:.2f} since {month_name}",
                "current_amount": current / 100.0,
                "previous_amount": previous / 100.0,
                "increase": increase_dollars,
                "date": current_date.isoformat() if current_date else None
            })
    
    return notifications

def detect_early_renewals(conn):
    """Detect subscriptions that renewed earlier than expected"""
    cursor = conn.cursor()
    
    # Get subscriptions with at least 2 renewals
    query = """
    WITH renewal_dates AS (
        SELECT 
            vendor,
            received_at,
            LAG(received_at) OVER (PARTITION BY vendor ORDER BY received_at) as previous_renewal,
            ROW_NUMBER() OVER (PARTITION BY vendor ORDER BY received_at DESC) as rn
        FROM subscription_history
        WHERE vendor != '' AND amount_cents > 0
    )
    SELECT 
        vendor,
        received_at as current_renewal,
        previous_renewal,
        (previous_renewal - received_at) as days_early
    FROM renewal_dates
    WHERE previous_renewal IS NOT NULL
      AND rn = 1
      AND (previous_renewal - received_at) BETWEEN 1 AND 7  -- 1-7 days early
    ORDER BY days_early DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    
    notifications = []
    for row in results:
        vendor, current, previous, days_early = row
        if days_early and days_early > 0:
            notifications.append({
                "type": "early_renewal",
                "vendor": vendor,
                "message": f"Your {vendor} subscription renewed {int(days_early)} days early",
                "current_renewal": current.isoformat() if current else None,
                "expected_renewal": previous.isoformat() if previous else None,
                "days_early": int(days_early)
            })
    
    return notifications

def detect_overlapping_subscriptions(conn):
    """Detect overlapping subscriptions in same category"""
    # Service category mapping
    categories = {
        "storage": ["Google Drive", "Dropbox", "iCloud", "OneDrive", "Box", "pCloud"],
        "music": ["Spotify", "Apple Music", "YouTube Music", "Amazon Music", "Pandora"],
        "video": ["Netflix", "Disney+", "Hulu", "Prime Video", "HBO Max", "Paramount+"],
        "productivity": ["Microsoft 365", "Google Workspace", "Notion", "Evernote"],
        "news": ["New York Times", "Wall Street Journal", "The Guardian"],
        "fitness": ["Peloton", "Strava", "MyFitnessPal", "Nike Training"]
    }
    
    cursor = conn.cursor()
    
    # Get current active subscriptions
    query = """
    SELECT DISTINCT vendor
    FROM subscription_history sh1
    WHERE received_at = (
        SELECT MAX(received_at)
        FROM subscription_history sh2
        WHERE sh2.vendor = sh1.vendor
    )
    AND vendor != '';
    """
    
    cursor.execute(query)
    active_vendors = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    notifications = []
    
    # Check each category for overlaps
    for category, vendors in categories.items():
        found_vendors = [v for v in active_vendors if any(cat_vendor.lower() in v.lower() for cat_vendor in vendors)]
        
        if len(found_vendors) >= 2:
            vendor_list = ", ".join(found_vendors)
            notifications.append({
                "type": "overlapping",
                "category": category,
                "vendors": found_vendors,
                "message": f"You're paying for {len(found_vendors)} overlapping {category} plans ({vendor_list})"
            })
    
    return notifications

def detect_recent_spending(conn, days=7):
    """Detect recent subscription spending (basic notifications)"""
    cursor = conn.cursor()
    
    query = """
    SELECT 
        vendor,
        amount_cents,
        currency,
        received_at
    FROM subscription_history
    WHERE received_at >= NOW() - INTERVAL '%s days'
      AND vendor != ''
      AND amount_cents > 0
    ORDER BY received_at DESC;
    """
    
    cursor.execute(query, (days,))
    results = cursor.fetchall()
    cursor.close()
    
    notifications = []
    for row in results:
        vendor, amount_cents, currency, received_at = row
        amount = amount_cents / 100.0
        
        # Format currency
        currency_symbol = "$" if currency == "USD" else currency
        
        notifications.append({
            "type": "recent_spending",
            "vendor": vendor,
            "message": f"You spent {currency_symbol}{amount:.2f} on {vendor}",
            "amount": amount,
            "currency": currency or "USD",
            "date": received_at.isoformat() if received_at else None
        })
    
    return notifications

@app.get("/api/notifications/smart")
def get_smart_notifications():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        notifications = []

        # Basic: Recent spending notifications (last 7 days)
        recent_spending = detect_recent_spending(conn, days=7)
        notifications.extend(recent_spending)

        price_increases = detect_price_increases(conn=conn)
        notifications.extend(price_increases)

        early_renewals = detect_early_renewals(conn)
        notifications.extend(early_renewals)
        
        # Detect overlapping subscriptions
        overlapping = detect_overlapping_subscriptions(conn)
        notifications.extend(overlapping)

        return {
            "notifications": notifications,
            "count": len(notifications)
        }
    
    except Exception as e:
        log.error("Error fetching smart notifications", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()