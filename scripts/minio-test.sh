#!/bin/sh
# Runs the `integration && minio` tests against a disposable local MinIO.
# The image is pinned by digest; the container is removed on exit.
set -eu

image='minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e'
name="s3store-minio-$$"
access="s3store$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
secret="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
bucket='s3store-minio-test'

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT HUP INT TERM

docker run -d --name "$name" -p 127.0.0.1::9000 \
	-e MINIO_ROOT_USER="$access" -e MINIO_ROOT_PASSWORD="$secret" \
	"$image" server /data >/dev/null
port=$(docker port "$name" 9000/tcp | head -n1 | sed 's/.*://')
endpoint="http://127.0.0.1:$port"

tries=0
until curl -fsS "$endpoint/minio/health/ready" >/dev/null 2>&1; do
	tries=$((tries + 1))
	if test "$tries" -gt 60; then
		echo "MinIO did not become ready" >&2
		exit 1
	fi
	sleep 0.5
done
docker exec "$name" sh -c "mc alias set local http://127.0.0.1:9000 '$access' '$secret' >/dev/null && mc mb local/$bucket >/dev/null"

S3STORE_MINIO_ENDPOINT="$endpoint" S3STORE_MINIO_BUCKET="$bucket" \
	S3STORE_MINIO_ACCESS_KEY="$access" S3STORE_MINIO_SECRET_KEY="$secret" \
	GOWORK=off go test -count=1 -race -tags 'integration minio' -run '^TestMinIO' -v .
