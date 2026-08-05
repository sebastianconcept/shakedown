# Security Policy

## Supported versions

Security fixes are applied to the latest published `0.x` release on crates.io. Older patch lines may not receive backports until 1.0.

## Reporting a vulnerability

Please report security issues privately via [GitHub Security Advisories](https://github.com/sebastianconcept/shakedown/security/advisories/new) for this repository.

Do not open a public issue for vulnerabilities that could be exploited before a fix is available.

## Scope notes for adopters

Shakedown is a **testing harness**. Depending on enabled features it may:

- Start **Docker / OCI containers** via testcontainers and pull images from public registries
- Talk to a local **Docker Engine** API (for example diagnostics via bollard)
- Bind mock services on **localhost** ports
- Run network fault injection (Toxiproxy) when `fault-injection` is enabled

Treat CI runners and developer machines accordingly: restrict Docker socket access, pin or mirror images when your org requires it, and do not expose harness ports beyond the test environment.

## Non-security bugs

Ordinary bugs and feature requests belong in [GitHub Issues](https://github.com/sebastianconcept/shakedown/issues).
