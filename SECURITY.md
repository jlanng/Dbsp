# Security

## Reporting a vulnerability

Report it privately through GitHub: the repository's **Security** tab, then **Report a
vulnerability**, or directly at
<https://github.com/jlanng/Dbsp/security/advisories/new>. Please do not open a public
issue for it.

Say which package and version, what an attacker can do, and how to reproduce it — a small
program is best.

Dbsp has one maintainer and promises no response time. A report is read and answered; a
fix, when there is one, goes out as a new release of all the packages, with a GitHub
security advisory naming the versions affected.

## Supported versions

Fixes go into the latest release on nuget.org only. This is a 0.x project: there are no
maintained older lines, and the fix for a vulnerability may arrive in a minor version
that also changes the API. `CHANGELOG.md` says what each release changed.

## Known limits, which are not vulnerabilities

These are stated in the documentation and are how the library is built today. A report of
one of them is welcome as an issue, in the open.

- **The TCP cluster transport is unauthenticated cleartext.** Anyone who can reach the
  port can join the mesh and inject state. Run it on a trusted, isolated network, or
  inside a tunnel or mesh the deployment provides (`README.md`, *Known limitations*).
- **State in Redis is plain JSON.** Whoever can write to the Redis instance can change a
  circuit's state; protecting the instance is the deployment's job.
- **Kafka's security is the client's.** TLS and SASL are librdkafka settings passed
  through `KafkaLogOptions.ClientConfig` and the sources' and sinks' options; the
  library adds nothing of its own.
- **The service (in the repository, not released) runs plugins as trusted code**, in its
  own process with its own rights, and its admin API without an authorization policy
  answers loopback requests only — a default for a developer's machine, not
  authentication (`docs/enrichment-service.md`, *Security*).
