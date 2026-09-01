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
logging framework, PostgreSQL dependency, or SessionStore dependency is allowed.

## S3 and security rules

- Credentials come only from an injected `aws.CredentialsProvider` or the AWS
  SDK's standard secure chain. Never include credentials, provider error text,
  request authorization, or presigned URLs in an error or log.
- Production endpoints require HTTPS. HTTP is accepted only for an explicitly
  enabled loopback test endpoint.
- Validate endpoint, region, bucket, deployment prefix, addressing style,
  encryption mode, multipart sizes, and concurrency before constructing clients.
- Bound simultaneous transfers at Store scope and reject configurations whose
  worst-case aggregate transfer buffers exceed 512 MiB.
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
