# JobScout Notification Service

A powerful, extensible notification system with message queue (Redis + RQ) for async processing, following SOLID principles.

## Overview

The notification service provides:
- **Multiple channels**: Email, Discord, Telegram, Webhooks, In-app
- **Custom channels**: Easily add your own notification handlers via configuration or environment variables
- **Async processing**: Redis Queue for reliable background processing
- **Deduplication**: Prevent notification fatigue with configurable strategies
- **Priority levels**: Low, Normal, High, Urgent
- **Queue monitoring**: Track queue status and notification delivery

## Architecture

The notification system follows SOLID principles with a clean 4-file architecture:

```
notification/
├── __init__.py          # Public API exports
├── channels.py          # Channel implementations + Factory
├── tracker.py           # Deduplication logic + strategies
├── service.py           # Main service orchestration
└── worker.py            # RQ worker for async processing
```

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Main App   │────▶│  Redis Queue │────▶│ RQ Worker   │
│             │     │              │     │             │
│ - Matching  │     │ - Persistent │     │ - Process   │
│ - Scoring   │     │ - Ordered    │     │ - Send      │
│ - Triggers  │     │ - Reliable   │     │ - Retry     │
└─────────────┘     └──────────────┘     └─────────────┘
                                                  │
                                                  ▼
                                         ┌─────────────┐
                                         │ Notification│
                                         │ Channels    │
                                         │             │
                                         │ • Email     │
                                         │ • Discord   │
                                         │ • Telegram  │
                                         │ • Webhook   │
                                         │ • In-app    │
                                         │ • Custom*   │
                                         └─────────────┘

*Custom channels loaded dynamically from config/env vars
```

## Quick Start

### 1. Start Redis

```bash
# Using Docker (recommended)
docker-compose -f docker-compose.redis.yml up -d

# Or use existing Redis
export REDIS_URL="redis://localhost:6379/0"
```

### 2. Start Worker

```bash
# Run notification worker in terminal 1
python -m notification.worker

# Or run in burst mode (process all and exit)
python -m notification.worker --burst

# With verbose logging
python -m notification.worker --verbose
```

### 3. Send Test Notification

```bash
# Via API
curl -X POST "http://localhost:5000/api/notifications/send" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email",
    "recipient": "user@example.com",
    "subject": "Test Notification",
    "body": "This is a test notification from JobScout",
    "priority": "normal"
  }'

# Or test via dashboard
open http://localhost:5000
```

## Configuration

### Environment Variables

```bash
# Redis Configuration
export REDIS_URL="redis://localhost:6379/0"

# Email Configuration (SMTP)
export SMTP_SERVER="smtp.gmail.com"
export SMTP_PORT="587"
export SMTP_USERNAME="your-email@gmail.com"
export SMTP_PASSWORD="your-app-password"
export FROM_EMAIL="notifications@jobscout.app"

# Discord Configuration
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/YOUR/WEBHOOK/TOKEN"

# Telegram Configuration
export TELEGRAM_BOT_TOKEN="your-bot-token-from-botfather"

# Custom Channel Loading (see Custom Channels section)
export NOTIFICATION_CHANNEL_PATH="/path/to/custom_channel.py"
export NOTIFICATION_CHANNEL_MODULES="my_package.channels:CustomChannel,another.module:AnotherChannel"

# Webhook Configuration (per-notification via metadata)
# No global env var needed - specify URL per notification
```

### Integration with Matching Pipeline

Add notifications to your matching pipeline:

```python
from notification import NotificationService

notification_service = NotificationService(repo)

# After scoring matches
for scored_match in scored_matches:
    if scored_match.overall_score >= 75:
        notification_service.notify_new_match(
            user_id="user123",
            match_id=str(scored_match.job.id),
            job_title=scored_match.job.title,
            company=scored_match.job.company,
            score=scored_match.overall_score,
            location=scored_match.job.location_text,
            is_remote=scored_match.job.is_remote
        )

# After batch completes
notification_service.notify_batch_complete(
    user_id="user123",
    total_matches=len(scored_matches),
    high_score_matches=len([m for m in scored_matches if m.overall_score >= 70])
)
```

## API Endpoints

### POST /api/notifications/send

Send a notification via the queue.

**Request Body:**
```json
{
  "type": "email",
  "recipient": "user@example.com",
  "subject": "New Job Match!",
  "body": "You have a great match...",
  "priority": "high"
}
```

**Types:** `email`, `discord`, `telegram`, `webhook`, `in_app`
**Priorities:** `low`, `normal`, `high`, `urgent`

**Response:**
```json
{
  "success": true,
  "notification_id": "uuid-here",
  "message": "Notification queued successfully (email)"
}
```

### GET /api/notifications/queue-status

Check queue status.

**Response:**
```json
{
  "success": true,
  "status": "active",
  "queue_length": 5,
  "redis_connected": true
}
```

### POST /api/notifications/test-match

Send a test match notification.

**Query Parameters:**
- `user_id` (required): User to notify
- `job_title`: Job title (default: "Senior Python Developer")
- `company`: Company name (default: "TechCorp")
- `score`: Match score 0-100 (default: 85)

**Response:**
```json
{
  "success": true,
  "notification_id": "uuid-here",
  "message": "Test match notification queued"
}
```

## Notification Types

### Email Notifications

Sends HTML/text emails via SMTP.

**Requirements:**
- SMTP server credentials
- Valid email addresses

**Example:**
```python
from notification import NotificationChannelFactory

channel = NotificationChannelFactory.get_channel('email')
channel.send(
    recipient="user@example.com",
    subject="🎯 Great job match found!",
    body="Match details...",
    metadata={}
)
```

### Discord Notifications

Sends rich embed messages to Discord channels via webhooks.

**Requirements:**
- Discord webhook URL

**Example:**
```python
channel = NotificationChannelFactory.get_channel('discord')
channel.send(
    recipient="",  # Not used, webhook in metadata
    subject="New Match Found!",
    body="You have a 95% match for Python Developer at TechCorp",
    metadata={
        'discord_webhook_url': 'https://discord.com/api/webhooks/...',
        'score': 95.0,
        'company': 'TechCorp'
    }
)
```

### Telegram Notifications

Sends messages via Telegram Bot API.

**Requirements:**
- Telegram Bot Token (get from @BotFather)
- Chat ID or channel username (e.g., @mychannel)

**Example:**
```python
channel = NotificationChannelFactory.get_channel('telegram')
channel.send(
    recipient="@mychannel",  # Or numeric chat ID
    subject="New Match!",
    body="Python Developer at TechCorp - 95% match",
    metadata={}
)
```

### Webhook Notifications

Sends HTTP POST requests to custom endpoints.

**Requirements:**
- Webhook endpoint URL
- Endpoint must accept POST requests

**Example:**
```python
channel = NotificationChannelFactory.get_channel('webhook')
channel.send(
    recipient="https://my-app.com/webhook",
    subject="",
    body=json.dumps({
        "event": "new_match",
        "match_id": "uuid",
        "score": 85.5
    }),
    metadata={'Authorization': 'Bearer token123'}
)
```

### In-App Notifications

Stores notifications in database for in-app display.

**Requirements:**
- None (logs only by default)

**Example:**
```python
channel = NotificationChannelFactory.get_channel('in_app')
channel.send(
    recipient="user123",
    subject="Match Found",
    body="Details...",
    metadata={}
)
```

## Custom Notification Channels

The notification system supports **dynamic loading of custom channels** without modifying core code!

### Method 1: Configuration-Driven (Recommended)

Add custom channels directly in `config.yaml`:

```yaml
notifications:
  enabled: true
  user_id: "my_user"

  custom_channels:
    - name: "company_webhook"
      module: "my_company.notifications"
      class: "CompanyWebhookChannel"
    - name: "pagerduty"
      module: "integrations.pagerduty"
      class: "PagerDutyChannel"

  channels:
    email:
      enabled: true
      recipient: "user@example.com"
    company_webhook:  # Reference the custom channel
      enabled: true
      recipient: "https://company.com/alerts"
```

### Method 2: Environment Variable (File Path)

Set the path to a Python file containing your channel:

```bash
export NOTIFICATION_CHANNEL_PATH="/path/to/my_custom_channel.py"
```

Your custom channel file:
```python
# my_custom_channel.py
from notification import NotificationChannel

class MyCustomChannel(NotificationChannel):
    @property
    def channel_type(self):
        return 'my_custom'

    def validate_config(self) -> bool:
        # Return True if properly configured
        return True

    def send(self, recipient: str, subject: str, body: str, metadata: dict) -> bool:
        # Implement your notification logic here
        print(f"Sending to {recipient}: {subject}")
        # ... your custom logic ...
        return True
```

### Method 3: Environment Variable (Module Path)

For installed packages:

```bash
export NOTIFICATION_CHANNEL_MODULES="my_package.channels:CustomChannel,another.module:AnotherChannel"
```

### Custom Channel Requirements

Your custom channel must:
1. Extend `NotificationChannel`
2. Implement `channel_type` property (returns string identifier)
3. Implement `send(recipient, subject, body, metadata)` method
4. Implement `validate_config()` method

### Example: MS Teams Channel

```python
# teams_channel.py
import requests
from notification import NotificationChannel

class MSTeamsChannel(NotificationChannel):
    @property
    def channel_type(self):
        return 'ms_teams'

    def validate_config(self):
        return True  # Webhook passed in metadata

    def send(self, recipient, subject, body, metadata):
        webhook_url = metadata.get('teams_webhook_url', recipient)

        payload = {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "title": subject,
            "text": body
        }

        try:
            response = requests.post(webhook_url, json=payload, timeout=30)
            return response.status_code == 200
        except Exception as e:
            print(f"Failed to send Teams notification: {e}")
            return False
```

Use it:
```bash
export NOTIFICATION_CHANNEL_PATH="./teams_channel.py"
```

Then in config:
```yaml
notifications:
  channels:
    ms_teams:
      enabled: true
      recipient: "https://outlook.office.com/webhook/..."
```

## Deduplication

The notification service includes intelligent deduplication to prevent notification fatigue.

### Deduplication Strategies

Two built-in strategies are available:

1. **DefaultDeduplicationStrategy** (default):
   - Never resend the exact same notification
   - Allow resend if content changes significantly
   - Allow resend after 24 hours for `score_improved` and `status_changed` events

2. **AggressiveDeduplicationStrategy**:
   - Never resend any notification
   - Only notify once per event type per user/match

### Using Deduplication

```python
from notification import NotificationService, AggressiveDeduplicationStrategy
from notification.tracker import NotificationTrackerService

# Use aggressive deduplication
service = NotificationService(repo)
tracker = NotificationTrackerService(repo, strategy=AggressiveDeduplicationStrategy())

# Check if should notify before sending
from notification import should_notify_user

if should_notify_user(repo, "user123", "match456", "new_match"):
    service.notify_new_match(...)
```

## Worker Management

### Starting Workers

```bash
# Run continuously
python -m notification.worker

# Run in burst mode (process all and exit)
python -m notification.worker --burst

# Run with multiple queues
python -m notification.worker --queues notifications emails

# Run with verbose logging
python -m notification.worker --verbose
```

### Running Multiple Workers

For high throughput, run multiple workers:

```bash
# Terminal 1
python -m notification.worker

# Terminal 2
python -m notification.worker

# Terminal 3
python -m notification.worker
```

### Process Supervision

Use systemd or supervisor for production:

```ini
# /etc/systemd/system/jobscout-worker.service
[Unit]
Description=JobScout Notification Worker
After=network.target

[Service]
Type=simple
User=jobscout
WorkingDirectory=/path/to/jobscout
ExecStart=python -m notification.worker
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Monitoring

### Check Queue Status

```python
from notification import NotificationService

service = NotificationService(repo)
status = service.get_queue_status()

print(f"Queue length: {status['queue_length']}")
print(f"Redis connected: {status['redis_connected']}")
```

### Via API

```bash
curl "http://localhost:5000/api/notifications/queue-status"
```

### Redis CLI

```bash
# Check queue length
redis-cli LLEN rq:queue:notifications

# View pending jobs
redis-cli LRANGE rq:queue:notifications 0 -1

# Check failed jobs
redis-cli LRANGE rq:queue:failed 0 -1
```

## Troubleshooting

### Worker Not Processing Jobs

1. Check Redis connection:
   ```bash
   redis-cli ping
   ```

2. Verify worker is running:
   ```bash
   ps aux | grep notification.worker
   ```

3. Check queue status via API

4. Review worker logs:
   ```bash
   python -m notification.worker --verbose
   ```

### Emails Not Sending

1. Verify SMTP credentials
2. Check firewall for port 587
3. Review logs for errors
4. Test SMTP connection manually

### Notifications Lost

Redis is persistent by default (with appendonly yes), but jobs may be lost if:
- Worker crashes before processing
- Redis restarts without persistence
- Job fails all retries

Check `rq:queue:failed` in Redis for failed jobs.

### High Queue Latency

Solutions:
1. Run multiple workers
2. Optimize notification processing
3. Use higher priority for urgent notifications
4. Add Redis monitoring

## Best Practices

1. **Always run worker** - Notifications won't send without worker
2. **Monitor queue** - Watch queue length to catch backlogs
3. **Use priorities** - Mark urgent notifications as HIGH
4. **Handle failures** - Check failed queue regularly
5. **Secure Redis** - Use authentication in production
6. **Rate limiting** - Don't overwhelm recipients

## Integration with Dashboard

The web dashboard includes:
- Queue status monitoring
- Test notification sending
- Notification history view
- Real-time queue updates (planned)

Access at: http://localhost:5000

## Docker Deployment

### With Redis

```bash
# Start both Redis and worker
docker-compose -f docker-compose.redis.yml up -d

# View logs
docker-compose -f docker-compose.redis.yml logs -f
```

### Full Stack

```yaml
# docker-compose.yml
version: '3.8'

services:
  app:
    build: .
    environment:
      - DATABASE_URL=postgresql://...
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - db
      - redis

  worker:
    build: .
    command: python -m notification.worker
    environment:
      - DATABASE_URL=postgresql://...
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - redis

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

  db:
    image: ankane/pgvector:latest
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  redis_data:
  postgres_data:
```

## Next Steps

Planned features:
- [ ] Scheduled notifications (delayed sending)
- [ ] Notification templates
- [ ] Rate limiting per recipient
- [ ] Notification analytics (delivery tracking)
- [ ] WebSocket for real-time updates
- [ ] SMS notifications (Twilio)
- [ ] Microsoft Teams notifications
- [ ] Notification preferences UI

## Resources

- [RQ Documentation](https://python-rq.org/)
- [Redis Documentation](https://redis.io/documentation)
- [FastAPI Background Tasks](https://fastapi.tiangolo.com/tutorial/background-tasks/)

## Support

For issues:
1. Check worker logs
2. Verify Redis connection
3. Review notification service logs
4. Check queue status
5. Open GitHub issue
