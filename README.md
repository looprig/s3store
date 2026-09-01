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

`Options` requires an S3 endpoint, region, bucket, and canonical deployment
prefix. HTTPS is mandatory except for an explicit loopback-only test option.
Addressing style, server-side encryption policy, multipart threshold/part size,
per-transfer concurrency, and Store-wide concurrent transfers are validated
before SDK construction. Their worst-case aggregate transfer buffers may not
exceed 512 MiB.

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

## Development

```sh
make check
GOWORK=off go test ./...
```

P2.1 starts no live S3 service. The `internal/testserver` package is reserved for
P2.2's disposable fixture and integration-tagged Storage conformance wiring.
