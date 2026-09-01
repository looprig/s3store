# Contributing to looprig/s3store

Thanks for contributing. Read [`CLAUDE.md`](CLAUDE.md) before changing code; it
defines the dependency, credential-redaction, transport, and testing boundaries.

## Before writing code

- Keep this provider limited to `storage.Blobs`. Structured primitives belong to
  `pgstore`, and SessionStore composition belongs to consumers.
- Do not add a local `replace`, vendor dependencies, or add PostgreSQL, a logging
  framework, or another cloud SDK.
- Write a failing test first and run it. Live service behavior belongs in an
  `integration`-tagged test against P2.2's disposable S3-compatible fixture.
- Never put credentials, authorization headers, provider errors, or presigned
  URLs into test failure output, package errors, or logs.
- Preserve both transfer bounds: the Store-wide operation gate and the validated
  512 MiB aggregate buffer ceiling.

## Checks

Every target runs the module standalone with `GOWORK=off`.

```sh
make fmt
make fmt-check
make vet
make test
make test-integration
make check
make secure
```

P2.1's integration target has no live fixture; P2.2 owns making the Storage
Blobs conformance suite operational against a disposable service.

## Pull requests

Keep changes repository-local and focused. Include the red/green commands,
mutation evidence for new guards, and standalone verification output. Do not
commit secrets, force-push reviewed work, or mix release/tag work into a feature
commit.

Contributions are licensed under Apache License 2.0; see [`LICENSE`](LICENSE).
