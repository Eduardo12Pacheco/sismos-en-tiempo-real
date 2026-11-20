# Security Policy

## Supported versions

Only the latest `0.1.x` release line is supported with security fixes.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Report it privately
through GitHub's ["Report a vulnerability"](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities)
feature on this repository, including:

- the affected version or commit,
- a minimal reproduction,
- the impact you believe it has.

You will receive an acknowledgement as soon as possible. This is a local-first,
single-user portfolio project without authentication or TLS, so the expected impact of
most findings is limited to the local machine and its data volumes.

## Scope

- Secrets are never committed; the history is scanned with gitleaks.
- Dependencies are audited in CI with `pip-audit`.
- The default stack binds published ports to `127.0.0.1` only.
- Development passwords in `.env.example` are local defaults, not secrets.
