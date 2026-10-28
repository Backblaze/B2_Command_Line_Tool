# Security Policy

Security-response owner: Backblaze Security.

## Supported Versions

The latest release of the `b2` CLI is supported. Fixes ship as a new release on top of the latest one;
older releases are not patched in place. Every ApiVer entry point in that release (`b2`, `b2v3`, `b2v4`,
`b2v5`) receives the fix. See [`RELEASE.md`](RELEASE.md).

| Version | Supported |
|---|---|
| Latest release (`5.x`) | Yes |
| Older releases | No |

## Reporting a Vulnerability

**Do not file a public GitHub issue for a security vulnerability.**

Instead, report it privately via GitHub Security Advisories:

1. Go to the [Security tab](https://github.com/Backblaze/B2_Command_Line_Tool/security/advisories/new) of this repository
2. Click **Report a vulnerability**
3. Provide a clear description, the affected version, reproduction steps, and impact assessment

You can also email **security@backblaze.com** if you cannot use GitHub Security Advisories.

## What to expect

- Acknowledgement within 3 business days
- Initial assessment within 7 business days
- Coordinated disclosure once a fix is available, shipped as a patch release with a changelog entry and a
  GitHub security advisory

## Scope

In scope:

- Credential-handling flaws in the CLI: application keys, authorization tokens or SSE-C keys exposed in
  output, logs (`--verbose`, `--debug-logs`, `b2_cli.log`), errors, shell history or to unintended hosts
- Commands that delete, overwrite or expose data beyond what the user asked for (`rm`, `sync`, file and
  bucket updates)
- The published artifacts: PyPI packages, standalone binaries and the `backblazeit/b2` Docker image

Out of scope:

- Vulnerabilities in the Backblaze B2 service itself — report those to Backblaze directly
- Vulnerabilities in `b2sdk` — report them to [b2-sdk-python](https://github.com/Backblaze/b2-sdk-python/security)
- Credentials the user passes or stores insecurely, such as a master key in a shared script
- Issues requiring root or local access on the machine running the CLI

## Security-relevant areas

- **Credentials**: `b2 account authorize` reads the key from arguments, a prompt, or
  `B2_APPLICATION_KEY_ID` / `B2_APPLICATION_KEY`, and stores the resulting authorization in a local SQLite
  database (`B2_ACCOUNT_INFO`, or the default path; `b2 account get` shows which). Anyone who can read that file
  can act as that key.
- **SSE-C keys**: passed through `B2_SOURCE_SSE_C_KEY_B64` / `B2_DESTINATION_SSE_C_KEY_B64` and sent to
  B2 in request headers. They are not persisted.
- **Logs**: `--debug-logs` writes `b2_cli.log` in the current directory; do not run it inside a directory
  you sync.
- **Transport**: all B2 API traffic uses HTTPS through `b2sdk`.
- **Tests and CI**: live integration tests read `B2_TEST_APPLICATION_KEY` and `B2_TEST_APPLICATION_KEY_ID`
  from repository secrets. The `.sdkharness` checks refuse any non-loopback endpoint and drop ambient
  `B2_*` variables. Releases sign the Windows binary and publish hashes for every bundle.

## Best practices for users

- Use application keys scoped to specific buckets, prefixes and capabilities.
- Prefer environment variables or the prompt over passing keys as arguments.
- Protect the account info database and run `b2 account clear` on shared machines.
- Rotate application keys regularly, and verify the published hashes of standalone binaries.
