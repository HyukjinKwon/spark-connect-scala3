# Security Policy

## Supported versions

`spark-connect-scala3` is pre-1.0; security fixes are applied to the latest
released version and the `main` branch.

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |
| < 0.1   | No        |

## Reporting a vulnerability

Please report security issues privately rather than opening a public issue.

- Preferred: open a private advisory via GitHub Security Advisories
  (the "Report a vulnerability" button under the repository's **Security** tab).
- Alternatively, email the maintainer at <gurwls223@apache.org>.

Include a description of the issue, the affected version, and reproduction steps
if possible. You can expect an acknowledgement within a few business days and a
coordinated disclosure once a fix is available.

## Scope notes

This project is a gRPC client for Apache Spark Connect. It transmits credentials
(for example a bearer token from the `sc://host/;token=...` connection string)
to the configured server, and enables TLS automatically when a token is present.
Vulnerabilities in Apache Spark itself should be reported to the
[Apache Spark project](https://spark.apache.org/security.html).
