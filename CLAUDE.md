# CLAUDE.md — s3store

`s3store` implements only [`storage`](../storage)'s immutable Blobs primitive
over an S3-compatible service. PostgreSQL structured primitives belong to
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
  `EncryptionBucketDefault` is a declared intent; P2.3 owns live verification.
- Reject configurations whose accounted transfer buffers,
  `(threshold + (concurrency+1) x partSize) x maxConcurrentTransfers`, exceed
  512 MiB. This is a configuration-time bound on the SDK's steady-state pools,
  not a runtime memory guarantee. Its three known leaks — part-size inflation
  above `partSize x 10000`, pre-pool read amplification, and the still-unacquired
  Store gate — are named in the README and must stay named there.
- The Store-wide gate is only real when acquired. `acquireTransfer` requires a
  deadline, fails closed on a Store not built by `newStore` instead of blocking
  on a nil channel, and waits in a `select` against `ctx.Done()`. P2.2 must
  acquire it around every transfer and pair it with a deferred
  `releaseTransfer`.
- Errors returned to callers are not automatically recordable. An invalid key
  surfaces as `*storage.InvalidNameError` retaining the key, as Storage's
  conformance suite requires. Anything that records an error must call
  `RedactedErrorText` first.
- Deployment prefixes and logical keys are canonical Storage names. P2.2 owns
  collision-free backend-key derivation and immutable S3 operations.
- Every operation, including `Open`, requires a caller context deadline.
- P2.1 performs no S3 request. Every blob operation returns a typed
  `NotImplementedError`; nil readers and nil listings always carry that error.

## Testing and build

Unit tests require no endpoint. S3 conformance tests use the `integration` build
tag once P2.2 adds the disposable fixture. Every Go command uses `GOWORK=off`,
and tests always run with `-race`.

Run `make check` before each commit and `make test-integration` when a disposable
S3-compatible endpoint is available.
