# Email Subscription Scanner

A professional CLI tool that scans your email for active subscription services using AI analysis.

## Features

- **Gmail Integration**: Connects to Gmail via IMAP to fetch emails
- **AI-Powered Analysis**: Uses Claude AI to identify subscription services
- **Smart Filtering**: Focuses on important emails from the past 4 months
- **Clean Export**: Saves results to CSV for further analysis
- **Professional Interface**: Clean, progress-tracked CLI experience

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configuration

Create a `.env` file in the project root with the following variables:

```env
# Gmail Configuration
IMAP_SERVER=imap.gmail.com
EMAIL_USER=your-email@gmail.com
EMAIL_PASSWORD=your-app-password

# Claude AI Configuration
CLAUDE_API_KEY=your-claude-api-key
```

### 3. Gmail App Password

For Gmail accounts:
1. Enable 2-factor authentication
2. Generate an App Password for this tool
3. Use the App Password (not your regular password) in EMAIL_PASSWORD

### 4. Claude API Key

1. Sign up at https://console.anthropic.com/
2. Create an API key
3. Add it to your .env file

## Usage

Run the scanner:

```bash
python subtrack.py
```

The tool will:
1. Connect to your Gmail account
2. Fetch relevant emails from the past 4 months
3. Analyze emails with AI to identify subscriptions
4. Display results in a clean table
5. Export data to `subscriptions.csv`

## Output

The scanner provides:
- **Service Name**: The subscription service
- **Amount**: Monthly/yearly cost
- **Next Payment**: When the next payment is due
- **Billing Cycle**: Monthly, yearly, etc.
- **Confidence**: AI confidence level (80%+)

## Data Export

Results are automatically saved to `subscriptions.csv` with detailed information for further analysis.

## Security

- Your email credentials are only used locally
- No data is stored or transmitted except to Claude AI for analysis
- Email content is processed in small batches for privacy

## Troubleshooting

- **Connection Failed**: Check your email credentials and app password
- **No Subscriptions Found**: Ensure you have subscription emails in your inbox
- **Configuration Error**: Verify all environment variables are set correctly

## Requirements

- Python 3.8+
- Gmail account with App Password
- Claude API key
- Internet connection 