# CLAUDE.md — s3store

`s3store` implements [`storage`](../storage)'s immutable Blobs primitive over
an S3-compatible service, plus the optional `BlobReaderLifecycle` capability
that `sessionstore.Open` requires of a Blobs provider. PostgreSQL structured primitives belong to
`pgstore`; SessionStore and composite assembly belong to consumers.

## Dependencies

Production code may import only:

- `github.com/looprig/storage` at its released pinned version;
- the approved `github.com/aws/aws-sdk-go-v2` modules for configuration,
  credentials, S3, and bounded transfers;
- this module's internal packages and the Go standard library.

Transitive modules belong to the AWS SDK. No local `replace`, vendor tree,
PostgreSQL dependency, or SessionStore dependency is allowed.

Production code emits to no output stream. `TestDependencyBoundary` enforces
exactly that scope: no `log` or `log/slog` import, no reference to `os.Stdout`
or `os.Stderr`, and no `print`/`println` builtin. Reporting is by returned
error only.

## S3 and security rules

- Credentials come only from an injected `aws.CredentialsProvider` or the AWS
  SDK's standard secure chain. Never include credentials, provider error text,
  request authorization, or presigned URLs in an error or log.
- Production endpoints require HTTPS. HTTP is accepted only when
  `AllowInsecureLocalhostOnly` is explicitly true *and* the host is loopback.
  Neither condition alone is sufficient, and both quadrants of that pair are
  covered by the option table.
- Validate endpoint, region, bucket, deployment prefix, addressing style,
  encryption mode, multipart sizes, and concurrency before constructing clients.
- `Encryption` must be stated explicitly. `EncryptionUnspecified` is the zero
  value and is rejected, because an unset field must not license `Open`.
  `EncryptionAES256` and `EncryptionKMS` are configured here: every
  object-creating request carries the header, asserted on all of them, not on
  one. `EncryptionBucketDefault` sends no header and is a declared intent.
- `RequireConfirmedEncryption` is the deployment-policy gate. With it set,
  `Open` refuses `EncryptionBucketDefault` with a typed `*EncryptionPolicyError`
  reachable by `errors.As`, because `Open` issues no request and so confirms
  nothing. It is a startup gate only and must never change a header on the wire.
  Live confirmation against a real service stays behind the `cloud` build tag,
  is excluded from the default and `integration` paths, and has not been run.
- Two tenants may share one bucket. `namespaceRoot` is the ONE derivation every
  key path uses, so the deployment prefix cannot be dropped from one path and
  survive in another. Tenancy tests construct the collision — identical
  SessionID and ObjectID, both tenants resident — and assert write, read, list,
  and delete behaviour; asserting that a key contains the tenant is not enough.
- There is no orphan collection, hard delete, or GC, and no operation may
  enumerate multipart uploads. A failed transfer aborts only the upload it
  created. Deleting an upload merely believed orphaned is forbidden: the proof
  would have to come from the owning SessionStore retention process, which this
  provider cannot see.
- Reject configurations whose accounted transfer buffers,
  `(threshold + (concurrency+1) x partSize) x maxConcurrentTransfers`, exceed
  512 MiB. This is a configuration-time bound on the SDK's steady-state pools,
  not a runtime memory guarantee. Pre-pool read amplification remains named in
  the README. `Put` rejects above `partSize x 10000 - 1`, and every operation
  acquires the Store-wide gate.
- The Store-wide gate is only real when acquired. `acquireTransfer` requires a
  deadline, fails closed on a Store not built by `newStore` instead of blocking
  on a nil channel, and waits in a `select` against `ctx.Done()`. Every method
  pairs acquisition with release; a Get reader holds its slot until terminal
  read or Close.
- Errors returned to callers are not automatically recordable. An invalid key
  surfaces as `*storage.InvalidNameError` retaining the key, as Storage's
  conformance suite requires. Anything that records an error must call
  `RedactedErrorText` first.
- Deployment prefixes and logical keys are canonical Storage names. Manifest
  object keys bind a reversible encoding to the logical key's SHA-256. Payloads
  are uniquely staged, range-verified, and published only through a conditional
  manifest create.
- Every operation, including `Open`, requires a caller context deadline.
- `Open` performs no S3 request. Blob methods use the bounded standard SDK retry
  classifier; source reads are never retried, while materialized upload parts
  are replayable.
- Never add a signed/public URL method. Factory authorizes logical reads and
  streams the returned reader.
- `Store` implements `storage.BlobReaderLifecycle`. Four rules hold it, each
  probed at the layer that can see it: `Close` both cancels the payload
  request context and closes the body; `Close` shares no lock with `Read`, so it
  never waits for one; `Close` is latched and classification-stable; and no
  `Read` after `Close` returns bytes or `io.EOF` -- it returns
  `*BlobReaderClosedError`, matching `fs.ErrClosed`. The EOF rule is not
  cosmetic: sessionstore compares bare `io.EOF` by identity to mean verified
  content. Do NOT adopt memstore's single-mutex Read/Close serialization; it
  makes `Close` wait for a blocked network read.
- Over a real HTTP body most of those parts mask each other -- abort and body
  Close each unblock a stalled read alone, net/http Close is idempotent, and
  the two closed checks are interchangeable after Close returns. They are held
  by synthetic fixtures in `blob_reader_test.go`, not by the integration probe,
  which holds only the emergent property that a stalled read is released.

## Testing and build

Unit tests require no endpoint. Integration-tagged tests start the disposable
in-process S3 fixture. `cloud`-tagged tests contact a service this repository
does not start and are excluded from both paths; never run them against a real
account without explicit human approval. Every Go command uses `GOWORK=off`,
and tests always run with `-race`.

Run `make check` and `make test-integration` before each commit.

`scripts/mutation-test.sh` snapshots the files it mutates and restores them.
Append new mutations BEFORE the trailing `restore_snapshot` / `rm -rf` epilogue;
anything after it runs with no snapshot, so every restore silently becomes a
no-op and mutations stack on one another while still reporting KILLED.
