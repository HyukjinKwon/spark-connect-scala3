# Security Policy

## Supported versions

This project is pre-1.0; security fixes are applied to the latest released `0.x`
version and `main`.

| Version | Supported |
|---------|-----------|
| latest `0.x` | ✅ |
| older | ❌ |

## Reporting a vulnerability

Please **do not** open a public issue for security vulnerabilities.

Instead, report privately via GitHub's
[security advisory form](https://github.com/HyukjinKwon/spark-connect-scala3/security/advisories/new),
or contact the maintainer directly. Include:

- a description of the vulnerability and its impact,
- steps to reproduce or a proof of concept,
- affected versions, and
- any suggested mitigation.

You can expect an initial acknowledgement within a few days. Once the issue is
confirmed and fixed, we will coordinate disclosure and credit you (unless you prefer
to remain anonymous).

## Scope

This is a client library that connects to a Spark Connect server over gRPC. Note that:

- Connection strings may contain credentials (`token=...`); keep them out of logs and
  source control.
- The client trusts the server it connects to; only connect to servers you control or
  trust, ideally over TLS (`use_ssl=true`).
