import imaplib
from dotenv import load_dotenv
import os
import email
from tabulate import tabulate
from emailClassifier import EmailClassifier
from openai import OpenAI
import time


load_dotenv()

def connect_to_email():
    """连接邮箱服务器"""
    try:
        server = os.getenv('IMAP_SERVER')
        user = os.getenv('EMAIL_USER')
        
        print(f"🔄 尝试连接服务器: {server}")
        print(f"🆔 使用账户: {user}")
        
        mail = imaplib.IMAP4_SSL(server)
        mail.login(user, os.getenv('EMAIL_PASSWORD'))
        
        print(f"✅ 成功连接到 {server}")
        return mail

    except Exception as e:
        print(f"❌ 连接失败: {str(e)}")
    return None

def fetch_recent_emails(batch_size=50):
    try:
        mail = connect_to_email()
        if not mail:
            return []

        # Select INBOX and handle potential errors
        try:
            status, messages = mail.select("INBOX")
            if status != 'OK':
                print(f"✗ Failed to select INBOX: {messages[0].decode()}")
                return []
        except Exception as e:
            print(f"✗ Error selecting INBOX: {str(e)}")
            return []

        # Search for emails
        try:
            status, messages = mail.search(None, "ALL")
            if status != 'OK':
                print(f"✗ Failed to search emails: {messages[0].decode()}")
                return []
            
            email_ids = messages[0].split()
            if not email_ids:
                print("No emails found in INBOX")
                return []
                
            email_ids = email_ids[-batch_size:]
        except Exception as e:
            print(f"✗ Error searching emails: {str(e)}")
            return []

        # Fetch and process emails
        batch = []
        for e_id in email_ids:
            try:
                status, msg_data = mail.fetch(e_id, "(RFC822)")
                if status != 'OK':
                    print(f"✗ Failed to fetch email {e_id.decode()}: {msg_data[0].decode()}")
                    continue

                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)

                email_data = {
                    "id": e_id,
                    "from": msg.get("From", "Unknown"),
                    "subject": msg.get("Subject", "No Subject"),
                    "date": msg.get("Date", "Unknown"),
                    "size": len(raw_email),
                    "raw": raw_email
                }

                batch.append(email_data)
            except Exception as e:
                print(f"✗ Error processing email {e_id.decode()}: {str(e)}")
                continue

        try:
            mail.close()
        except Exception as e:
            print(f"✗ Error closing mailbox: {str(e)}")

        print(f"✓ Successfully fetched {len(batch)} emails")
        return batch

    except Exception as e:
        print(f"✗ Unexpected error in fetch_recent_emails: {str(e)}")
        return []

def extract_email_content(raw_email):
    """Extract text content from email"""
    email_message = email.message_from_bytes(raw_email)
    text = ""
    
    if email_message.is_multipart():
        for part in email_message.walk():
            if part.get_content_type() == "text/plain":
                text += part.get_payload(decode=True).decode()
    else:
        text = email_message.get_payload(decode=True).decode()
    
    return {"text": text}

def batch_analyze_emails(email_batch):
    import json
    import os

    # Validate OpenAI API key
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("✗ Missing OPENAI_API_KEY in environment variables")
        return [{}] * len(email_batch)

    try:
        client = OpenAI()
        
        prompt = "Analyze the subscription information in the following email list:\n\n"
        
        for i, email in enumerate(email_batch):
            try:
                content = extract_email_content(email["raw"])
                prompt += f"### Email {i+1} ###\n"
                prompt += f"From: {email['from']}\n"
                prompt += f"Subject: {email['subject']}\n"
                prompt += f"Content Summary: {content['text'][:500]}...\n\n"
            except Exception as e:
                print(f"✗ Error processing email {i+1} for analysis: {str(e)}")
                continue
        
        prompt += """
        Please return the following JSON format information for each email:
        {
            "service": "Service Name",
            "amount": "Amount",
            "currency": "Currency Type",
            "next_payment_date": "YYYY-MM-DD",
            "billing_cycle": "Billing Cycle",
            "confidence": "Confidence(0-1)"
        }
        
        Return a JSON array in the same order as the input emails.
        Return an empty object if subscription information cannot be determined.
        """

        try:
            response = client.chat.completions.create(
                model="gpt-4.1",  # Use the correct model name
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that analyzes subscription information from emails."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3  # Lower temperature for more consistent results
            )
            
            results = json.loads(response.choices[0].message.content)
            if isinstance(results, list):
                print(f"✓ Successfully analyzed {len(results)} emails")
                return results
            else:
                print("✗ Unexpected response format from API")
                return [{}] * len(email_batch)

        except json.JSONDecodeError as e:
            print(f"✗ Error parsing API response: {str(e)}")
            return [{}] * len(email_batch)
        except Exception as e:
            print(f"✗ API call failed: {str(e)}")
            return [{}] * len(email_batch)

    except Exception as e:
        print(f"✗ Unexpected error in batch_analyze_emails: {str(e)}")
        return [{}] * len(email_batch)

def process_email_batch():
    emails = fetch_recent_emails(20)

    if not emails:
        return []

    classifier = EmailClassifier()
    predictions = classifier.predict(emails)

    # Extract potential subscription emails
    subscription_emails = [e for e, pred in zip(emails, predictions) if pred]
    
    if not subscription_emails:
        return []
    
    # Batch analysis
    results = batch_analyze_emails(subscription_emails)
    
    return results


"""
Functions to improve the email processing pipeline:
"""

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


def display_results(results):
    """Display subscription information results"""
    if not results:
        print("\n❌ No subscription information found")
        return
    
    print("\n📋 Subscription Summary:")
    
    table_data = []
    for item in results:
        amount = f"{item.get('amount', '')} {item.get('currency', '')}".strip()
        if not amount:
            amount = "Unknown"
        
        service = item.get("service", "Unknown")
        next_payment = item.get("next_payment_date", "Unknown")
        billing_cycle = item.get("billing_cycle", "Unknown")
        confidence = f"{float(item.get('confidence', 0)) * 100:.1f}%"
        processed_at = item.get("processed_at", "Unknown")
        subject = item.get("subject", "No subject")
        
        table_data.append([
            service,
            amount,
            next_payment,
            billing_cycle,
            confidence,
            processed_at,
            subject[:50] + "..." if len(subject) > 50 else subject
        ])
    
    print(tabulate(table_data, 
                   headers=["Service", "Amount", "Next Payment", "Billing Cycle", 
                           "Confidence", "Processed At", "Email Subject"],
                   tablefmt="fancy_grid",
                   numalign="left",
                   stralign="left"))

def process_emails(mail):
    """Process emails to extract subscription information"""
    import time
    from datetime import datetime, timedelta
    from tqdm import tqdm
    
    print("📥 Fetching recent emails...")
    emails = fetch_recent_emails(50)
    
    if not emails:
        print("ℹ️ No emails found to process")
        return []
    
    print(f"📊 Found {len(emails)} emails, filtering and prioritizing...")
    processed_hashes = set()
    filtered_emails = []
    
    prioritized_emails = prioritize_emails(emails)
    for email in tqdm(prioritized_emails, desc="Filtering duplicates", unit="email"):
        if not is_duplicate_email(email, processed_hashes):
            filtered_emails.append(email)
    
    if not filtered_emails:
        print("ℹ️ No new emails to analyze after filtering")
        return []
    
    print(f"🔍 Analyzing {len(filtered_emails)} unique emails...")
    results = batch_analyze_emails(filtered_emails)
    
    # Add email subjects and processing timestamp to results
    processed_results = []
    for result, email in zip(results, filtered_emails):
        if isinstance(result, dict) and result.get("service") and result.get("confidence", 0) > 0.5:
            result["subject"] = email.get("subject", "")
            result["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            processed_results.append(result)
    
    print(f"✨ Found {len(processed_results)} subscription-related emails")
    return processed_results

def main():
    print("\n🚀 Starting Email Subscription Scanner...")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    start_time = time.time()
    mail = connect_to_email()
    if not mail:
        print("\n❌ Email connection failed. Please check your settings and try again.")
        return
    
    try:
        results = process_emails(mail)
        display_results(results)
        
        duration = time.time() - start_time
        print(f"\n⏱️  Process completed in {duration:.1f} seconds")
        print(f"📊 Processed emails: {len(results)} subscription(s) found")
        
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        print("Please check your settings and try again.")
    finally:
        try:
            mail.logout()
            print("\n✓ Successfully logged out from email server")
        except Exception:
            pass  # Ignore logout errors
        
        print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print("✨ Scanner finished")

if __name__ == "__main__":
    main()