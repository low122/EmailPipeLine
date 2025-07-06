"""
Email Subscription Scanner

A professional CLI tool that scans your email for active subscription services using AI analysis.

Usage:
    python subtrack.py

Requirements:
    - .env file with Gmail credentials and Claude API key
    - Gmail App Password enabled
    - Claude API access

The tool will analyze your emails from the past 4 months and identify active subscriptions,
displaying results in a clean table format and exporting to CSV.

Author: AutoAsset CLI Toolkit
"""

import imaplib
from dotenv import load_dotenv
import os
import email
from tabulate import tabulate
import anthropic
import time
import csv
import sys


load_dotenv()

def print_progress(current, total, prefix="Progress"):
    """Print a simple progress indicator"""
    if total > 0:
        percentage = int((current / total) * 100)
        bar_length = 20
        filled_length = int(bar_length * current // total)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        print(f'\r{prefix}: |{bar}| {percentage}% ({current}/{total})', end='', flush=True)
        if current == total:
            print()  # New line when complete

def connect_to_email():
    """Connect to email server"""
    try:
        server = os.getenv('IMAP_SERVER')
        user = os.getenv('EMAIL_USER')
        
        print(f"Connecting to {server}...")
        
        mail = imaplib.IMAP4_SSL(server)
        mail.login(user, os.getenv('EMAIL_PASSWORD'))
        
        print(f"✓ Connected successfully as {user}")
        return mail

    except Exception as e:
        print(f"✗ Connection failed: {str(e)}")
        print("Please check your email credentials in the .env file")
    return None

def fetch_recent_emails(months_back=4, max_emails=500):
    from datetime import datetime, timedelta
    
    mail = connect_to_email()
    if not mail:
        return []

    try:
        mail.select("INBOX")
        
        # Calculate date 4 months ago
        four_months_ago = datetime.now() - timedelta(days=months_back * 30)
        search_date = four_months_ago.strftime("%d-%b-%Y")
        
        print(f"Searching emails since {search_date}...")
        
        # Try to search for Gmail's "Important" emails first with date filter
        try:
            # Method 1: Search for Important label with date
            search_criteria = f'X-GM-LABELS "\\\\Important" SINCE "{search_date}"'
            status, messages = mail.search(None, search_criteria)
            if status == 'OK' and messages[0]:
                email_ids = messages[0].split()
            else:
                raise Exception("Important search failed")
                
        except:
            # Fallback: Use regular search with date
            status, messages = mail.search(None, f'SINCE "{search_date}"')
            if status != 'OK' or not messages[0]:
                return []
            email_ids = messages[0].split()
        
        # Limit to max_emails for safety
        if len(email_ids) > max_emails:
            email_ids = email_ids[-max_emails:]
        
        print(f"Processing {len(email_ids)} emails...")
        
        batch = []
        total_emails = len(email_ids)
        
        for idx, e_id in enumerate(email_ids):
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
                    
                # Show progress every 10 emails or at the end
                if (idx + 1) % 10 == 0 or idx == total_emails - 1:
                    print_progress(idx + 1, total_emails, "Fetching emails")
                    
            except:
                continue
        
        mail.close()
        print(f"✓ Retrieved {len(batch)} emails")
        return batch
        
    except Exception as e:
        print(f"✗ Failed to fetch emails: {str(e)}")
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
    
    # Process emails in smaller batches for better AI quality
    batch_size = 20  # Smaller batches for better analysis
    all_results = []
    
    total_batches = (len(email_batch) + batch_size - 1) // batch_size
    print("Analyzing emails with AI...")
    
    for i in range(0, len(email_batch), batch_size):
        current_batch = email_batch[i:i+batch_size]
        batch_num = i//batch_size + 1
        print_progress(batch_num - 1, total_batches, "AI Analysis")
        
        # Build prompt with email data
        prompt = "Analyze these emails for subscription information:\n\n"
        for j, email in enumerate(current_batch):
            content = extract_email_content(email["raw"])
            text_content = content['text'][:500] if content['text'] else "No content"
            
            prompt += f"### Email {j+1} ###\n"
            prompt += f"From: {email['from']}\n"
            prompt += f"Subject: {email['subject']}\n"
            prompt += f"Content: {text_content}\n\n"
    
        prompt += """
        IMPORTANT: Only identify ACTIVE, SUCCESSFUL subscription payments or renewals. 
        EXCLUDE: Failed payments, cancelled subscriptions, one-time purchases, trials, alerts.
        
        For each email, return ONLY the JSON object in this exact format:
        ```json
        {"service": "ServiceName", "amount": "19.99", "currency": "USD", "next_payment_date": "2025-01-15", "billing_cycle": "Monthly", "confidence": 0.9}
        ```
        
        If no active subscription: 
        ```json
        {"service": "", "amount": "", "currency": "", "next_payment_date": "", "billing_cycle": "", "confidence": 0}
        ```
        
        Return one JSON object per email. No explanations.
        """

        try:
            response = client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            
            content_text = response.content[0].text if response.content else ""
            
            if not content_text.strip():
                continue
            
            # Extract individual JSON objects from the response
            # Find all JSON objects in ```json blocks
            json_objects = re.findall(r'```json\s*(\{[^`]*?\})\s*```', content_text, re.DOTALL)
            
            if not json_objects:
                # Try to find standalone JSON objects
                json_objects = re.findall(r'\{[^{}]*"service"[^{}]*\}', content_text, re.DOTALL)
            
            if json_objects:
                batch_results = []
                
                for json_obj_str in json_objects:
                    try:
                        json_obj = json.loads(json_obj_str.strip())
                        batch_results.append(json_obj)
                    except json.JSONDecodeError:
                        # If individual object parsing fails, create empty object
                        batch_results.append({})
                        
            else:
                # Create empty results for this batch
                batch_results = [{} for _ in current_batch]
            
            # Add results to collection
            if isinstance(batch_results, list):
                all_results.extend(batch_results)
            else:
                empty_results = [{} for _ in current_batch]
                all_results.extend(empty_results)
            
        except json.JSONDecodeError:
            # Create empty results for this batch
            empty_results = [{} for _ in current_batch]
            all_results.extend(empty_results)
            continue
        except Exception as e:
            print(f"\n    Warning: Batch {batch_num} failed ({str(e)})")
            # Create empty results for this batch
            empty_results = [{} for _ in current_batch]
            all_results.extend(empty_results)
            continue
        
        # Update progress
        print_progress(batch_num, total_batches, "AI Analysis")
    
    return all_results



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
        print("\nNo active subscriptions detected.")
        return
    
    print(f"\nDetected {len(results)} Active Subscription(s):")
    print("=" * 60)
    
    table_data = []
    for item in results:
        amount = f"{item.get('amount', '')} {item.get('currency', '')}".strip() or "Unknown"
        service = item.get("service", "Unknown")
        next_payment = item.get("next_payment_date", "Unknown")
        billing_cycle = item.get("billing_cycle", "Unknown")
        confidence = f"{float(item.get('confidence', 0)) * 100:.0f}%"
        
        table_data.append([service, amount, next_payment, billing_cycle, confidence])
    
    print(tabulate(table_data, 
                   headers=["Service", "Amount", "Next Payment", "Billing Cycle", "Confidence"],
                   tablefmt="grid"))

def process_emails():
    """Process emails to extract subscription information"""
    from datetime import datetime
    
    print("Fetching recent emails...")
    emails = fetch_recent_emails(months_back=4, max_emails=500)
    
    if not emails:
        return []
    
    # Filter duplicates and prioritize all emails
    processed_hashes = set()
    filtered_emails = [email for email in prioritize_emails(emails) 
                      if not is_duplicate_email(email, processed_hashes)]
    
    # Limit to reasonable batch size for AI processing
    if len(filtered_emails) > 100:
        filtered_emails = filtered_emails[:100]
        print(f"Limiting analysis to {len(filtered_emails)} most relevant emails...")
    
    if not filtered_emails:
        return []
    
    results = batch_analyze_emails(filtered_emails)
    
    # Filter and format results with additional validation
    processed_results = []
    failed_keywords = ["unsuccessful", "failed", "declined", "cancelled", "refund", 
                      "terminated", "expired", "suspended", "overdue", "error"]
    
    for result, email in zip(results, filtered_emails):
        if (isinstance(result, dict) and 
            result.get("service") and 
            result.get("confidence", 0) >= 0.8):
            
            # Double-check: exclude failed payments based on subject/content
            subject_lower = email.get("subject", "").lower()
            if any(keyword in subject_lower for keyword in failed_keywords):
                continue
                
            result["subject"] = email.get("subject", "")
            result["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            processed_results.append(result)
    
    print(f"Analysis complete. Found {len(processed_results)} subscriptions.")
    return processed_results

def check_configuration():
    """Check if required environment variables are set"""
    required_vars = ['IMAP_SERVER', 'EMAIL_USER', 'EMAIL_PASSWORD', 'CLAUDE_API_KEY']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("Configuration Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nPlease check your .env file and ensure all required variables are set.")
        return False
    
    return True

def main():
    print("\n" + "=" * 60)
    print("            EMAIL SUBSCRIPTION SCANNER")
    print("=" * 60)
    print("Scanning your email for active subscription services...")
    print()
    
    # Check configuration first
    if not check_configuration():
        print("\n" + "=" * 60)
        return
    
    start_time = time.time()
    
    try:
        results = process_emails()
        display_results(results)

        if results:
            with open("subscriptions.csv", 'w', newline='') as f:
                fieldnames = ['service', 'amount', 'currency', 'billing_cycle', 'next_payment_date', 'confidence', 'subject', 'processed_at']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
                print(f"\n✓ Results exported to subscriptions.csv")
            
            # Calculate total monthly cost
            total_monthly = 0
            for result in results:
                try:
                    amount = float(result.get('amount', 0))
                    if result.get('billing_cycle', '').lower() == 'yearly':
                        amount = amount / 12
                    total_monthly += amount
                except (ValueError, TypeError):
                    pass
            
            if total_monthly > 0:
                print(f"\nEstimated monthly subscription cost: ${total_monthly:.2f}")
        
        duration = time.time() - start_time
        print(f"\nScan completed in {duration:.1f} seconds")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"\nError: {str(e)}")
        print("Please check your configuration and try again.")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()