import imaplib
from dotenv import load_dotenv
import os
import email
from tabulate import tabulate
import anthropic
import time


load_dotenv()

def connect_to_email():
    """Connect to email server"""
    try:
        server = os.getenv('IMAP_SERVER')
        user = os.getenv('EMAIL_USER')
        
        print(f"🔄 Attempting to connect to server: {server}")
        print(f"🆔 Using account: {user}")
        
        mail = imaplib.IMAP4_SSL(server)
        mail.login(user, os.getenv('EMAIL_PASSWORD'))
        
        print(f"✅ Successfully connected to {server}")
        return mail

    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
    return None

def fetch_recent_emails(batch_size=50):
    mail = connect_to_email()
    if not mail:
        return []

    try:
        mail.select("INBOX")
        
        # Try to search for Gmail's "Important" emails first
        try:
            # Method 1: Search for Important label
            status, messages = mail.search(None, 'X-GM-LABELS "\\\\Important"')
            if status == 'OK' and messages[0]:
                print("📌 Using Gmail Important emails")
                email_ids = messages[0].split()[-batch_size:]
            else:
                raise Exception("Important search failed")
                
        except:
            # Fallback: Use regular search
            print("📧 Using all emails (Important search not available)")
            status, messages = mail.search(None, "ALL")
            if status != 'OK' or not messages[0]:
                return []
            email_ids = messages[0].split()[-batch_size:]
        
        batch = []
        for e_id in email_ids:
            try:
                status, msg_data = mail.fetch(e_id, "(RFC822)")
                if status == 'OK':
                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)
                    
                    batch.append({
                        "id": e_id,
                        "from": msg.get("From", "Unknown"),
                        "subject": msg.get("Subject", "No Subject"),
                        "date": msg.get("Date", "Unknown"),
                        "raw": raw_email
                    })
            except:
                continue
        
        mail.close()
        print(f"✓ Fetched {len(batch)} emails")
        return batch
        
    except Exception as e:
        print(f"✗ Email fetch failed: {str(e)}")
        return []

def extract_email_content(raw_email):
    """Extract text content from email"""
    try:
        email_message = email.message_from_bytes(raw_email)
        text = ""
        
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    text += part.get_payload(decode=True).decode('utf-8', errors='ignore')
        else:
            text = email_message.get_payload(decode=True).decode('utf-8', errors='ignore')
        
        return {"text": text}
    except:
        return {"text": ""}

def batch_analyze_emails(email_batch):
    import json
    import re
    import os

    api_key = os.getenv("CLAUDE_API_KEY")
    if not api_key:
        print("✗ Missing CLAUDE_API_KEY")
        return []

    client = anthropic.Anthropic(api_key=api_key)
    
    # Build prompt with email data
    prompt = "Analyze these emails for subscription information:\n\n"
    for i, email in enumerate(email_batch):
        content = extract_email_content(email["raw"])
        text_content = content['text'][:500] if content['text'] else "No content"
        
        prompt += f"### Email {i+1} ###\n"
        prompt += f"From: {email['from']}\n"
        prompt += f"Subject: {email['subject']}\n"
        prompt += f"Content: {text_content}\n\n"
    
    prompt += """
    Return ONLY a JSON array with this format for each email:
    {"service": "Service Name", "amount": "Amount", "currency": "Currency", 
     "next_payment_date": "YYYY-MM-DD", "billing_cycle": "Monthly/Yearly", "confidence": 0.8}
    
    Use empty strings and 0 confidence if no subscription info found.
    """

    try:
        response = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        
        content_text = response.content[0].text if response.content else ""
        
        # Extract JSON from markdown code blocks
        json_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', content_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # Try to find JSON array directly
            json_match = re.search(r'\[.*\]', content_text, re.DOTALL)
            json_str = json_match.group(0) if json_match else content_text
        
        results = json.loads(json_str)
        print(f"✓ Analyzed {len(results)} emails")
        return results if isinstance(results, list) else []
        
    except (json.JSONDecodeError, AttributeError, Exception) as e:
        print(f"✗ Analysis failed: {str(e)}")
        return []



def is_duplicate_email(email, processed_hashes):
    import hashlib
    content_hash = hashlib.sha256(email["raw"]).hexdigest()
    is_duplicate = content_hash in processed_hashes
    if not is_duplicate:
        processed_hashes.add(content_hash)
    return is_duplicate

def prioritize_emails(emails):
    """Prioritize emails based on sender and keywords"""
    priority_senders = ["paypal@", "stripe@", "apple@", "netflix@", "spotify@"]
    for email in emails:
        if any(sender in email["from"].lower() for sender in priority_senders):
            email["priority"] = 10
        elif any(kw in email["subject"].lower() for kw in ["invoice", "receipt", "subscription", "payment"]):
            email["priority"] = 5
        else:
            email["priority"] = 1
    return sorted(emails, key=lambda x: x["priority"], reverse=True)

def filter_with_subscription(batch_email):
    keywords = ["payment", "subscription", "renew", "bill", "invoice", "charge", 
                "receipt", "premium", "pro", "plus", "monthly", "yearly", "annual",
                "membership", "account", "statement", "due", "upgrade", "plan",
                "spotify", "netflix", "canva", "youtube", "adobe", "microsoft",
                "apple", "google", "amazon", "paypal", "stripe", "visa", "mastercard"]
    
    """Predict if it's a subscription email"""
    results = []
    for email in batch_email:
        subject = email["subject"].lower()
        sender = email["from"].lower()
        
        # Check both subject and sender
        if (any(keyword in subject for keyword in keywords) or 
            any(keyword in sender for keyword in keywords)):
            results.append(True)
        else:
            results.append(False)
    return results


def display_results(results):
    """Display subscription information results"""
    if not results:
        print("\n❌ No subscriptions found")
        return
    
    print("\n📋 Subscription Summary:")
    
    table_data = []
    for item in results:
        amount = f"{item.get('amount', '')} {item.get('currency', '')}".strip() or "Unknown"
        service = item.get("service", "Unknown")
        next_payment = item.get("next_payment_date", "Unknown")
        billing_cycle = item.get("billing_cycle", "Unknown")
        confidence = f"{float(item.get('confidence', 0)) * 100:.1f}%"
        subject = item.get("subject", "")[:50] + ("..." if len(item.get("subject", "")) > 50 else "")
        
        table_data.append([service, amount, next_payment, billing_cycle, confidence, subject])
    
    print(tabulate(table_data, 
                   headers=["Service", "Amount", "Next Payment", "Billing", "Confidence", "Subject"],
                   tablefmt="fancy_grid"))

def process_emails(mail):
    """Process emails to extract subscription information"""
    from datetime import datetime
    
    print("📥 Fetching recent emails...")
    emails = fetch_recent_emails(50)
    
    if not emails:
        return []
    
    # Option 1: Use keyword filter (comment out if not working)
    subscription_flags = filter_with_subscription(emails)
    subscription_emails = [email for email, is_subscription in zip(emails, subscription_flags) if is_subscription]
    
    if not subscription_emails:
        print("🔍 No emails match subscription keywords, analyzing all emails...")
        subscription_emails = emails  # Fallback to all emails
    
    # Filter duplicates and prioritize
    processed_hashes = set()
    filtered_emails = [email for email in prioritize_emails(emails) 
                      if not is_duplicate_email(email, processed_hashes)]
    
    if not filtered_emails:
        return []
    
    print(f"🔍 Analyzing {len(filtered_emails)} emails...")
    
    # Debug: Show filtered emails
    print("\n📧 Filtered emails being analyzed:")
    # for i, email in enumerate(filtered_emails, 1):
    #     print(f"  {i}. From: {email['from'][:50]}...")
    #     print(f"     Subject: {email['subject'][:80]}...")
    #     print(f"     Priority: {email.get('priority', 'N/A')}")
    #     print()
    
    results = batch_analyze_emails(filtered_emails)
    
    # Filter and format results
    processed_results = []
    for result, email in zip(results, filtered_emails):
        if (isinstance(result, dict) and 
            result.get("service") and 
            result.get("confidence", 0) >= 0.8):
            result["subject"] = email.get("subject", "")
            result["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            processed_results.append(result)
    
    print(f"✨ Found {len(processed_results)} subscriptions")
    return processed_results

def main():
    print("\n🚀 Starting Email Subscription Scanner...")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    start_time = time.time()
    mail = connect_to_email()
    if not mail:
        print("❌ Email connection failed")
        return
    
    try:
        results = process_emails(mail)
        display_results(results)
        
        duration = time.time() - start_time
        print(f"\n⏱️  Completed in {duration:.1f}s | Found {len(results)} subscriptions")
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
    finally:
        try:
            mail.logout()
        except:
            pass
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print("✨ Scanner finished")

if __name__ == "__main__":
    main()