# Security Policy

Report vulnerabilities through GitHub's private security-advisory feature. Do not include tokens, database dumps, raw X payloads, Discord interaction bodies, or production logs in public issues.

Operational requirements:

- Use a dedicated least-privilege Discord application.
- Keep the API behind HTTPS; expose only liveness publicly.
- Store every `XV_*TOKEN`, `XV_*SECRET`, public-key configuration, admin token, and database credential in a secret manager or uncommitted `.env`.
- Rotate a credential immediately if it appears in logs, chat, CI output, or Git history.
- Use official X APIs only. No scraping, credential sharing, or access-control bypasses.
- Keep PostgreSQL and backups private, encrypted, and access logged.

Supported security fixes target the latest `main` branch.
