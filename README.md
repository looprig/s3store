# s3store

`s3store` is Looprig's S3-compatible provider for the immutable
`storage.Blobs` primitive. It deliberately does not implement Ledger, Leaser,
KV, OrderedIndex, or SessionStore. A cloud composition combines this provider
with the structured primitives from `pgstore`.

This P2.1 scaffold validates configuration and constructs lazy AWS SDK clients
without contacting an endpoint. Blob operations return typed
`NotImplementedError` values; P2.2 adds immutable streaming, conditional create,
range/list behavior, and disposable-service conformance.

## Configuration

`Options` requires an S3 endpoint, region, bucket, canonical deployment prefix,
and an explicit `Encryption` posture: the zero value is rejected so an unset
field cannot read as an accepted posture. HTTPS is mandatory except for an
explicit loopback-only test option; a loopback host is not itself sufficient.
Addressing style, encryption policy, multipart threshold/part size, per-transfer
concurrency, and Store-wide concurrent transfers are validated before SDK
construction.

### The 512 MiB transfer-memory bound is a configuration-time bound

`Open` rejects any configuration whose accounted transfer buffers,
`(MultipartThreshold + (Concurrency+1) x MultipartPartSize) x MaxConcurrentTransfers`,
exceed 512 MiB. That is a bound on the SDK's steady-state transfer pools
computed before any client is constructed. It is **not** a runtime guarantee on
the process's resident memory, and three leaks are known and named rather than
hidden:

1. **Part-size inflation above the accounted object size.** The transfer
   manager raises `PartSizeBytes` to `objectSize/MaxUploadParts + 1` once
   `objectSize/PartSizeBytes >= MaxUploadParts`. `MaxUploadParts` is pinned here
   at the SDK's hard maximum of 10000, which is as high as it may go, so the
   accounted part size holds only up to
   `MultipartPartSize x 10000 - 1` — about 156 GiB at the defaults. Above that,
   parts grow with the object and the product above is exceeded; at S3's 5 TiB
   object maximum the accounted figure is off by roughly an order of magnitude.
   P2.2 owns rejecting uploads larger than the resolved accounted object size,
   which is why `Open` retains it.
2. **Buffering before the pool exists.** Reading a body into memory with
   `io.ReadAll`-style append growth transiently exceeds `MultipartThreshold` by
   up to about 2x before a pooled buffer is allocated. P2.2 owns streaming
   rather than accumulating.
3. **The `MaxConcurrentTransfers` factor is enforced only if it is used.** The
   Store-wide gate exists and `acquireTransfer`/`releaseTransfer` define its
   contract, but P2.1 starts no transfer, so nothing acquires it yet. Until
   P2.2 acquires the gate around every transfer, that factor is arithmetic
   only.

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
presigned URLs when reporting an `Open` failure.

## Recording errors

Errors returned to callers are not automatically safe to record. Storage's
shared `Blobs` conformance suite requires an invalid key to surface as a
`*storage.InvalidNameError` whose `Name` is exactly the offending key, and blob
keys are tenant- and session-scoped. Anything that records, logs, or emits an
s3store error must pass it through `RedactedErrorText` first, which keeps the
failure class and the name-grammar rule and drops the identifier. Errors this
package cannot classify are withheld entirely rather than recorded verbatim.

## Encryption posture and the P2.3 handoff

`EncryptionBucketDefault` declares an intent to rely on an externally enforced
bucket policy. Neither P2.1 nor P2.2 can confirm that policy, so selecting it is
not evidence that objects are encrypted. P2.3 owns verifying required
server-side encryption headers or bucket policy against a live service and
failing startup when the deployment requires encryption that cannot be
confirmed. `EncryptionUnspecified`, the zero value, is rejected outright so an
unset field never stands in for that verification.

Two further absences are handed to P2.2 rather than resolved here:
`validateListPrefix` accepts the empty prefix, which P2.2 must ensure cannot
escape the deployment/tenant prefix; and the accounted object size above is
retained but unenforced until `Put` streams.

## Development

```sh
make check
GOWORK=off go test ./...
```

P2.1 starts no live S3 service. The `internal/testserver` package is reserved for
P2.2's disposable fixture and integration-tagged Storage conformance wiring.
