#!/usr/bin/env python3
"""
Show Subscriptions - Displays subscriptions found by the email pipeline
Uses data from Phases 0-6 pipeline (PostgreSQL database)
"""

import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

# Try to import tabulate, fallback to simple formatting
try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

load_dotenv()

def connect_db():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5433')),
            dbname=os.getenv('DB_NAME', 'email_pipeline'),
            user=os.getenv('DB_USER', 'pipeline_user'),
            password=os.getenv('DB_PASSWORD', 'pipeline_pass')
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("\n💡 Make sure PostgreSQL is running:")
        print("   docker compose -f infra/compose.yml up -d postgres")
        return None

def show_subscriptions():
    """Show subscriptions from the pipeline"""
    print("\n" + "="*80)
    print(" " * 25 + "📧 YOUR SUBSCRIPTIONS")
    print("="*80)
    print("Powered by Email Pipeline (Phases 0-6)\n")
    
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        # Get subscriptions
        query = """
        SELECT 
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
        
        if not results:
            print("📭 No subscriptions found yet.")
            print("\nThe pipeline is processing your emails...")
            print("It polls every 30 seconds and processes new emails automatically.")
            print("\n💡 Check back in a few minutes!")
            print("   Or check pipeline logs: docker compose -f infra/compose.yml logs imap_poller\n")
            return
        
        # Calculate total
        total_monthly = sum(row[1] for row in results if row[1])
        
        # Display results
        print(f"Found {len(results)} subscription(s):\n")
        
        if HAS_TABULATE:
            headers = ["Vendor", "Amount", "Currency", "Type", "Confidence", "Date", "Account"]
            table_data = []
            
            for row in results:
                vendor, amount, currency, class_type, confidence, date, subject, mailbox = row
                table_data.append([
                    vendor or "N/A",
                    f"${amount:.2f}" if amount else "$0.00",
                    currency or "USD",
                    class_type or "N/A",
                    f"{confidence*100:.0f}%" if confidence else "0%",
                    date.strftime("%Y-%m-%d") if date else "N/A",
                    (mailbox[:20] + "...") if mailbox and len(mailbox) > 20 else (mailbox or "N/A")
                ])
            
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
        else:
            # Simple formatting
            print(f"{'Vendor':<20} {'Amount':<12} {'Currency':<8} {'Type':<12} {'Confidence':<10} {'Date':<12} {'Account':<25}")
            print("-" * 100)
            for row in results:
                vendor, amount, currency, class_type, confidence, date, subject, mailbox = row
                amount_str = f"${amount:.2f}" if amount else "$0.00"
                confidence_str = f"{confidence*100:.0f}%" if confidence else "0%"
                date_str = date.strftime("%Y-%m-%d") if date else "N/A"
                mailbox_str = (mailbox[:20] + "...") if mailbox and len(mailbox) > 20 else (mailbox or "N/A")
                print(f"{vendor or 'N/A':<20} {amount_str:<12} {currency or 'USD':<8} {class_type or 'N/A':<12} {confidence_str:<10} {date_str:<12} {mailbox_str:<25}")
        
        print(f"\n💰 Total Monthly Cost: ${total_monthly:.2f}")
        
        # Show pipeline stats
        cursor.execute("SELECT COUNT(*) FROM messages")
        total_emails = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT mailbox_id) FROM messages")
        accounts = cursor.fetchone()[0]
        
        print(f"\n📊 Pipeline Statistics:")
        print(f"   • Emails Processed: {total_emails}")
        print(f"   • Accounts Monitored: {accounts}")
        print(f"   • Subscriptions Found: {len(results)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()
    
    print("\n" + "="*80)
    print("💡 The pipeline runs automatically every 30 seconds")
    print("   Check logs: docker compose -f infra/compose.yml logs -f\n")

if __name__ == "__main__":
    show_subscriptions()

