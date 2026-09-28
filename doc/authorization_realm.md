# Authorization realm (advanced / Backblaze development)

By default, the B2 CLI talks to the production Backblaze B2 authorization realm. The controls
below exist mainly for Backblaze development and testing.

- `B2_ENVIRONMENT` selects the Backblaze authorization realm. Leave it unset for production, or
  set it to a known realm name or a full B2 API endpoint URL.
- The hidden flags `--environment <realm-or-url>`, `--dev`, and `--staging` provide equivalent
  realm-selection controls for the same purpose.

> **Note:** These controls select which Backblaze realm the CLI authenticates against. They are
> not a way to namespace your own data or point the CLI at your own testing or staging server.
