# 08 — Notifications (digest, reminders, event alerts)

## Goal
Deliver high-leverage nudges that convert intent into applications and follow-ups.

## Tasks
### A. Notification types
- Daily digest: “Top N matches today” with reasons and actions.
- Follow-up reminders: based on status age (e.g., applied 7 days ago).
- Event alerts: new match above threshold, job closing soon, interview tomorrow.

### B. Scheduling and idempotency
- Per-user quiet hours.
- One digest per day per user maximum.
- Store notification send attempts and outcomes.

### C. Email templates
- Provide follow-up email templates.
- Allow personalization fields: company, role, hiring manager (if known), application date.

### D. Delivery channels
- Email first.
- Later: push notifications and Slack/Discord.

## Acceptance criteria
- Users receive daily digest and reminders at configured times.
- No duplicate notifications for the same event.

## Risks / gotchas
- Email deliverability: set up SPF/DKIM/DMARC early for production.
