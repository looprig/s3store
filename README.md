# s3store

`s3store` is Looprig's S3-compatible provider for the immutable
`storage.Blobs` primitive. It deliberately does not implement Ledger, Leaser,
KV, OrderedIndex, or SessionStore. A cloud composition combines this provider
with the structured primitives from `pgstore`.

`Open` validates configuration and constructs lazy AWS SDK clients without
contacting an endpoint. `Put` streams into a uniquely-owned payload while
hashing, verifies the committed length and SHA-256 through bounded immutable
ranges, then publishes a small manifest with `If-None-Match: *`. That manifest
is the atomic logical object: identical retries are no-ops, different bytes
conflict, and no writer overwrites another logical value.

Backend manifest keys contain a SHA-256 binding plus raw-URL encoding of the
validated logical key. The longest supported 512-byte key under the longest
256-byte deployment prefix produces a 1,014-byte backend key, below S3's
1,024-byte limit. Listing validates each row independently and pages past
foreign or malformed keys.

## Configuration

`Options` requires an S3 endpoint, region, bucket, canonical deployment prefix,
and an explicit `Encryption` posture: the zero value is rejected so an unset
field cannot read as an accepted posture. `RequireConfirmedEncryption` states
that the deployment policy requires server-side encryption; see below. HTTPS is mandatory except for an
explicit loopback-only test option; a loopback host is not itself sufficient.
Addressing style, encryption policy, multipart threshold/part size, per-transfer
concurrency, and Store-wide concurrent transfers are validated before SDK
construction.

### The 512 MiB transfer-memory bound is a configuration-time bound

`Open` rejects any configuration whose accounted transfer buffers,
`(MultipartThreshold + (Concurrency+1) x MultipartPartSize) x MaxConcurrentTransfers`,
exceed 512 MiB. That is a bound on the SDK's steady-state transfer pools
computed before any client is constructed. It is **not** a runtime guarantee on
the process's resident memory. Three edge conditions are named explicitly:

1. **Part-size inflation above the accounted object size.** The transfer
   manager raises `PartSizeBytes` to `objectSize/MaxUploadParts + 1` once
   `objectSize/PartSizeBytes >= MaxUploadParts`. `MaxUploadParts` is pinned here
   at the SDK's hard maximum of 10000, which is as high as it may go, so the
   accounted part size holds only up to
   `MultipartPartSize x 10000 - 1` — about 156 GiB at the defaults. Above that,
   parts grow with the object and the product above is exceeded; at S3's 5 TiB
   object maximum the accounted figure is off by roughly an order of magnitude.
   `Put` rejects the first byte above that resolved accounted object size, so
   the SDK never silently raises the configured part size.
2. **Buffering before the pool exists.** Reading a body into memory with
   `io.ReadAll`-style append growth transiently exceeds `MultipartThreshold` by
   above the configured threshold before a pooled buffer is allocated. Uploads
   remain bounded by the configured threshold and parts rather than the whole
   object, but this is why the 512 MiB number is not advertised as RSS.
3. **The `MaxConcurrentTransfers` factor is enforced only if it is used.** The
   Store-wide gate is acquired by every operation. `Get` retains its slot until
   the returned response body reaches a terminal result or is closed, bounding
   simultaneously live bodies as well as method calls.

Credentials are supplied through an injected `aws.CredentialsProvider`. When
none is provided, the AWS SDK's standard credential chain is used. There are no
raw access-key or secret-key option fields, and SDK/provider error text is not
returned by `Open`.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

blobs, err := s3store.Open(ctx, s3store.Options{
    Endpoint:         "https://s3.example.com",
    Region:           "us-east-1",
    Bucket:           "looprig-objects",
    DeploymentPrefix: "deployments/production",
    AddressingStyle:  s3store.AddressingPath,
    Encryption:       s3store.EncryptionAES256,
    MaxConcurrentTransfers: 4,
})
if err != nil {
    return err
}

var primitive storage.Blobs = blobs
_ = primitive
```

Do not log `Options`, injected provider failures, request authorization, or
presigned URLs when reporting an `Open` failure. `Store` exposes only the four
`storage.Blobs` methods and never mints a public or signed URL; Factory owns
authorization and streaming to clients.

## Recording errors

Errors returned to callers are not automatically safe to record. Storage's
shared `Blobs` conformance suite requires an invalid key to surface as a
`*storage.InvalidNameError` whose `Name` is exactly the offending key, and blob
keys are tenant- and session-scoped. Anything that records, logs, or emits an
s3store error must pass it through `RedactedErrorText` first, which keeps the
failure class and the name-grammar rule and drops the identifier. Errors this
package cannot classify are withheld entirely rather than recorded verbatim.

## Tenant isolation

Two tenants may share one bucket. They are separated by the deployment prefix,
which is the sole component every backend key path is derived from — manifest
write, manifest reverse, payload write, payload match, and the listing prefix
all go through one `namespaceRoot`, so the prefix cannot be dropped from one
path while surviving in another.

The integration suite constructs the collision rather than asserting the key
format: two tenants with an **identical SessionID and ObjectID** write, read,
list, and delete under one bucket. Writing different bytes to the identical
logical key must not conflict, each read must return its own bytes, neither
listing may name the other, and deleting one must not disturb the other. The
same collision is also exercised one position over — one bucket, one deployment
prefix, tenants separated only by the leading component of the logical key —
which is where exact-key and exact-prefix behaviour, not the derivation, does
the work.

## Encryption posture

`Encryption` states the posture and `RequireConfirmedEncryption` states whether
the deployment policy demands one it can stand behind.

- `EncryptionAES256` and `EncryptionKMS` are configured by this module: every
  object-creating request it issues — payload `PutObject`, the multipart
  `CreateMultipartUpload`, and the manifest `PutObject` — carries the
  corresponding server-side encryption header, which the integration suite
  asserts on every such request rather than on one representative.
- `EncryptionBucketDefault` sends no header and delegates to an externally
  enforced bucket policy. `Open` issues no S3 request, so it confirms nothing
  about that bucket.
- `EncryptionUnspecified`, the zero value, is rejected outright.

With `RequireConfirmedEncryption` set, `Open` therefore **refuses to start**
against `EncryptionBucketDefault`, returning a typed `*EncryptionPolicyError`
reachable with `errors.As`. The refusal is about the missing confirmation, not
about the bucket: a correctly configured bucket-default deployment is refused
too, because nothing available to `Open` distinguishes it from a bucket that
enforces nothing. The option changes startup only; it does not alter any header
on the wire.

Confirming a live service's applied encryption or its bucket policy requires an
endpoint this repository does not create. That check lives in
`encryption_cloud_test.go` behind the `cloud` build tag, is excluded from both
`go test ./...` and `go test -tags integration ./...`, skips unless
`S3STORE_CLOUD_ENDPOINT` and `S3STORE_CLOUD_BUCKET` are set, and **has not been
executed**.

## Multipart uploads, aborts, and orphans

A multipart upload whose parts fail past the bounded retry classifier is
aborted, leaves no upload in progress, and commits neither payload nor manifest.
That abort is the transfer manager's, issued while the upload is still owned by
the failing `Put`; it is not orphan collection.

There is no orphan collection, hard delete, or GC here, and the tests state that
as behaviour: no operation enumerates multipart uploads, an upload left open by
another process is still open afterwards, and an abort never names an upload
this transfer did not create. Orphan collection would require proving an upload
unreferenced by the owning SessionStore retention process, which this provider
cannot see.

## Development

```sh
make check
GOWORK=off go test ./...
```

Unit tests require no service. Integration-tagged tests start a disposable
in-process S3-compatible service and exercise shared Storage conformance,
tenant isolation, encryption headers, multipart retry and abort, cancellation,
paging, range bounds, integrity failures, and concurrent immutable publication.
`cloud`-tagged tests are excluded from both paths and contact a service this
repository does not start.
