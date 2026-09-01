// Package testserver provides a disposable in-process subset of the S3 HTTP
// protocol used by s3store integration tests.
package testserver

import (
	"crypto/md5" // #nosec G501 -- S3 ETags use MD5 as an opaque fixture identifier, not for security.
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type object struct {
	body []byte
	etag string
}

type upload struct {
	bucket string
	key    string
	parts  map[int][]byte
}

type fault struct {
	status int
	code   string
}

type requestBlock struct {
	started chan struct{}
}

type commitBlock struct {
	committed chan struct{}
}

// Server is an isolated S3-compatible service with in-memory object state.
type Server struct {
	http *httptest.Server
	mu   sync.Mutex

	objects         map[string]object
	uploads         map[string]*upload
	nextID          uint64
	counts          map[string]int
	faults          map[string][]fault
	blocks          map[string][]*requestBlock
	commitBlocks    map[string][]*commitBlock
	commitFaults    map[string][]fault
	pageLimit       int
	ranges          []string
	rangeIfMatches  []string
	corruptPayloads []string
}

// New starts an empty disposable service.
func New() *Server {
	server := &Server{
		objects: make(map[string]object), uploads: make(map[string]*upload),
		counts: make(map[string]int), faults: make(map[string][]fault), blocks: make(map[string][]*requestBlock),
		commitBlocks: make(map[string][]*commitBlock),
		commitFaults: make(map[string][]fault),
	}
	server.http = httptest.NewServer(http.HandlerFunc(server.serveHTTP))
	return server
}

func (s *Server) URL() string { return s.http.URL }
func (s *Server) Close()      { s.http.Close() }

// Count returns how many requests reached an operation.
func (s *Server) Count(operation string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[operation]
}

// FailNext injects count identical S3 failures for one operation.
func (s *Server) FailNext(operation string, count, status int, code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for range count {
		s.faults[operation] = append(s.faults[operation], fault{status: status, code: code})
	}
}

// BlockNext blocks one operation until its request context is canceled and
// returns a channel closed once the request reaches the fixture.
func (s *Server) BlockNext(operation string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocked := &requestBlock{started: make(chan struct{})}
	s.blocks[operation] = append(s.blocks[operation], blocked)
	return blocked.started
}

// CommitThenBlockNext commits one single-part PUT, withholds its response, and
// blocks until the request context is canceled. It models a lost acknowledgement.
func (s *Server) CommitThenBlockNext(operation string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocked := &commitBlock{committed: make(chan struct{})}
	s.commitBlocks[operation] = append(s.commitBlocks[operation], blocked)
	return blocked.committed
}

// CommitThenFailNext commits one single-part PUT but returns the supplied S3
// failure instead of its success response.
func (s *Server) CommitThenFailNext(operation string, status int, code string) {
	s.mu.Lock()
	s.commitFaults[operation] = append(s.commitFaults[operation], fault{status: status, code: code})
	s.mu.Unlock()
}

// SetPageLimit forces pagination below the client's requested MaxKeys.
func (s *Server) SetPageLimit(limit int) {
	s.mu.Lock()
	s.pageLimit = limit
	s.mu.Unlock()
}

// PutRaw inserts foreign fixture data without going through the SDK.
func (s *Server) PutRaw(bucket, key string, body []byte) {
	s.mu.Lock()
	s.objects[objectID(bucket, key)] = newObject(body)
	s.mu.Unlock()
}

// CorruptNextPayload mutates the next payload PUT after reading it. Supported
// modes are "truncate", "flip", and "extend".
func (s *Server) CorruptNextPayload(mode string) {
	s.mu.Lock()
	s.corruptPayloads = append(s.corruptPayloads, mode)
	s.mu.Unlock()
}

// Ranges returns a snapshot of every Range header received by GetObject.
func (s *Server) Ranges() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranges...)
}

// RangeIfMatches returns the If-Match headers paired with ranged GETs.
func (s *Server) RangeIfMatches() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.rangeIfMatches...)
}

func (s *Server) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	bucket, key, ok := splitPath(request.URL.Path)
	if !ok {
		writeError(writer, http.StatusBadRequest, "InvalidURI")
		return
	}
	query := request.URL.Query()
	operation := operationName(request.Method, key, query)
	s.mu.Lock()
	s.counts[operation]++
	var injected *fault
	if queued := s.faults[operation]; len(queued) > 0 {
		current := queued[0]
		injected = &current
		s.faults[operation] = queued[1:]
	}
	var blocked *requestBlock
	if queued := s.blocks[operation]; len(queued) > 0 {
		blocked = queued[0]
		s.blocks[operation] = queued[1:]
	}
	var committed *commitBlock
	if queued := s.commitBlocks[operation]; len(queued) > 0 {
		committed = queued[0]
		s.commitBlocks[operation] = queued[1:]
	}
	var commitFailure *fault
	if queued := s.commitFaults[operation]; len(queued) > 0 {
		current := queued[0]
		commitFailure = &current
		s.commitFaults[operation] = queued[1:]
	}
	s.mu.Unlock()
	if blocked != nil {
		close(blocked.started)
		_, _ = io.Copy(io.Discard, request.Body)
		<-request.Context().Done()
		return
	}
	if injected != nil {
		writeError(writer, injected.status, injected.code)
		return
	}
	if commitFailure != nil {
		recorder := httptest.NewRecorder()
		s.putObject(recorder, request, bucket, key)
		writeError(writer, commitFailure.status, commitFailure.code)
		return
	}
	if committed != nil {
		recorder := httptest.NewRecorder()
		s.putObject(recorder, request, bucket, key)
		close(committed.committed)
		<-request.Context().Done()
		return
	}
	switch {
	case request.Method == http.MethodGet && key == "" && query.Get("list-type") == "2":
		s.listObjects(writer, request, bucket)
	case request.Method == http.MethodPost && key != "" && hasQueryKey(query, "uploads"):
		s.createMultipart(writer, bucket, key)
	case request.Method == http.MethodPut && key != "" && query.Get("uploadId") != "":
		s.uploadPart(writer, request, query.Get("uploadId"), query.Get("partNumber"))
	case request.Method == http.MethodPost && key != "" && query.Get("uploadId") != "":
		s.completeMultipart(writer, request, bucket, key, query.Get("uploadId"))
	case request.Method == http.MethodDelete && key != "" && query.Get("uploadId") != "":
		s.abortMultipart(writer, query.Get("uploadId"))
	case request.Method == http.MethodPut && key != "":
		s.putObject(writer, request, bucket, key)
	case request.Method == http.MethodHead && key != "":
		s.headObject(writer, bucket, key)
	case request.Method == http.MethodGet && key != "":
		s.getObject(writer, request, bucket, key)
	case request.Method == http.MethodDelete && key != "":
		s.deleteObject(writer, bucket, key)
	default:
		writeError(writer, http.StatusNotImplemented, "NotImplemented")
	}
}

func operationName(method, key string, query url.Values) string {
	switch {
	case method == http.MethodGet && key == "" && query.Get("list-type") == "2":
		return "ListObjectsV2"
	case method == http.MethodPost && hasQueryKey(query, "uploads"):
		return "CreateMultipartUpload"
	case method == http.MethodPut && query.Get("uploadId") != "":
		return "UploadPart"
	case method == http.MethodPost && query.Get("uploadId") != "":
		return "CompleteMultipartUpload"
	case method == http.MethodDelete && query.Get("uploadId") != "":
		return "AbortMultipartUpload"
	case method == http.MethodPut && strings.Contains(key, "/payloads/v1/"):
		return "PutPayload"
	case method == http.MethodPut && strings.Contains(key, "/blobs/v1/"):
		return "PutManifest"
	case method == http.MethodHead && strings.Contains(key, "/payloads/v1/"):
		return "HeadPayload"
	case method == http.MethodHead && strings.Contains(key, "/blobs/v1/"):
		return "HeadManifest"
	case method == http.MethodGet && strings.Contains(key, "/payloads/v1/"):
		return "GetPayload"
	case method == http.MethodGet && strings.Contains(key, "/blobs/v1/"):
		return "GetManifest"
	case method == http.MethodDelete && strings.Contains(key, "/payloads/v1/"):
		return "DeletePayload"
	case method == http.MethodDelete && strings.Contains(key, "/blobs/v1/"):
		return "DeleteManifest"
	default:
		return method
	}
}

func splitPath(path string) (string, string, bool) {
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", false
	}
	if len(parts) == 1 {
		return parts[0], "", true
	}
	return parts[0], parts[1], true
}

func objectID(bucket, key string) string { return bucket + "\x00" + key }

func newObject(body []byte) object {
	digest := md5.Sum(body) // #nosec G401 -- fixture ETag compatibility only.
	return object{body: append([]byte(nil), body...), etag: `"` + hex.EncodeToString(digest[:]) + `"`}
}

func (s *Server) putObject(writer http.ResponseWriter, request *http.Request, bucket, key string) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "IncompleteBody")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.Contains(key, "/payloads/v1/") && len(s.corruptPayloads) > 0 {
		mode := s.corruptPayloads[0]
		s.corruptPayloads = s.corruptPayloads[1:]
		switch mode {
		case "truncate":
			if len(body) > 0 {
				body = body[:len(body)-1]
			}
		case "flip":
			if len(body) > 0 {
				body[0] ^= 0xff
			}
		case "extend":
			body = append(body, 0)
		}
	}
	id := objectID(bucket, key)
	if request.Header.Get("If-None-Match") == "*" {
		if _, exists := s.objects[id]; exists {
			writeError(writer, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
	}
	stored := newObject(body)
	s.objects[id] = stored
	writer.Header().Set("ETag", stored.etag)
	writer.WriteHeader(http.StatusOK)
}

func (s *Server) headObject(writer http.ResponseWriter, bucket, key string) {
	s.mu.Lock()
	stored, ok := s.objects[objectID(bucket, key)]
	s.mu.Unlock()
	if !ok {
		writeError(writer, http.StatusNotFound, "NoSuchKey")
		return
	}
	writer.Header().Set("Content-Length", strconv.Itoa(len(stored.body)))
	writer.Header().Set("ETag", stored.etag)
	writer.Header().Set("Accept-Ranges", "bytes")
	writer.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(writer http.ResponseWriter, request *http.Request, bucket, key string) {
	if requested := request.Header.Get("Range"); requested != "" {
		s.mu.Lock()
		s.ranges = append(s.ranges, requested)
		s.rangeIfMatches = append(s.rangeIfMatches, request.Header.Get("If-Match"))
		s.mu.Unlock()
	}
	s.mu.Lock()
	stored, ok := s.objects[objectID(bucket, key)]
	s.mu.Unlock()
	if !ok {
		writeError(writer, http.StatusNotFound, "NoSuchKey")
		return
	}
	body := stored.body
	writer.Header().Set("ETag", stored.etag)
	if match := request.Header.Get("If-Match"); match != "" && match != stored.etag {
		writeError(writer, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	if requested := request.Header.Get("Range"); requested != "" {
		start, end, valid := parseRange(requested, len(body))
		if !valid {
			writer.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(body)))
			writeError(writer, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
			return
		}
		body = body[start : end+1]
		writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(stored.body)))
		writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
		writer.WriteHeader(http.StatusPartialContent)
		_, _ = writer.Write(body)
		return
	}
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(body)
}

func parseRange(value string, length int) (int, int, bool) {
	if !strings.HasPrefix(value, "bytes=") || strings.Contains(value, ",") {
		return 0, 0, false
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, false
	}
	start, startErr := strconv.Atoi(parts[0])
	end, endErr := strconv.Atoi(parts[1])
	if startErr != nil || endErr != nil || start < 0 || end < start || start >= length {
		return 0, 0, false
	}
	if end >= length {
		end = length - 1
	}
	return start, end, true
}

func (s *Server) deleteObject(writer http.ResponseWriter, bucket, key string) {
	s.mu.Lock()
	delete(s.objects, objectID(bucket, key))
	s.mu.Unlock()
	writer.WriteHeader(http.StatusNoContent)
}

func (s *Server) listObjects(writer http.ResponseWriter, request *http.Request, bucket string) {
	prefix := request.URL.Query().Get("prefix")
	s.mu.Lock()
	keys := make([]string, 0)
	for id := range s.objects {
		objectBucket, objectKey, _ := strings.Cut(id, "\x00")
		if objectBucket == bucket && strings.HasPrefix(objectKey, prefix) {
			keys = append(keys, objectKey)
		}
	}
	s.mu.Unlock()
	sort.Strings(keys)
	start := 0
	if token := request.URL.Query().Get("continuation-token"); token != "" {
		parsed, err := strconv.Atoi(token)
		if err != nil || parsed < 0 || parsed > len(keys) {
			writeError(writer, http.StatusBadRequest, "InvalidToken")
			return
		}
		start = parsed
	}
	limit := 1000
	if requested, err := strconv.Atoi(request.URL.Query().Get("max-keys")); err == nil && requested > 0 && requested < limit {
		limit = requested
	}
	s.mu.Lock()
	if s.pageLimit > 0 && s.pageLimit < limit {
		limit = s.pageLimit
	}
	s.mu.Unlock()
	end := min(start+limit, len(keys))
	type content struct {
		Key  string `xml:"Key"`
		Size int    `xml:"Size"`
		ETag string `xml:"ETag"`
	}
	response := struct {
		XMLName               xml.Name  `xml:"ListBucketResult"`
		Name                  string    `xml:"Name"`
		Prefix                string    `xml:"Prefix"`
		KeyCount              int       `xml:"KeyCount"`
		MaxKeys               int       `xml:"MaxKeys"`
		IsTruncated           bool      `xml:"IsTruncated"`
		NextContinuationToken string    `xml:"NextContinuationToken,omitempty"`
		Contents              []content `xml:"Contents"`
	}{Name: bucket, Prefix: prefix, KeyCount: end - start, MaxKeys: limit, IsTruncated: end < len(keys)}
	if response.IsTruncated {
		response.NextContinuationToken = strconv.Itoa(end)
	}
	for _, key := range keys[start:end] {
		s.mu.Lock()
		stored := s.objects[objectID(bucket, key)]
		s.mu.Unlock()
		response.Contents = append(response.Contents, content{Key: key, Size: len(stored.body), ETag: stored.etag})
	}
	writeXML(writer, http.StatusOK, response)
}

func (s *Server) createMultipart(writer http.ResponseWriter, bucket, key string) {
	s.mu.Lock()
	s.nextID++
	id := strconv.FormatUint(s.nextID, 10)
	s.uploads[id] = &upload{bucket: bucket, key: key, parts: make(map[int][]byte)}
	s.mu.Unlock()
	writeXML(writer, http.StatusOK, struct {
		XMLName xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket  string   `xml:"Bucket"`
		Key     string   `xml:"Key"`
		Upload  string   `xml:"UploadId"`
	}{Bucket: bucket, Key: key, Upload: id})
}

func (s *Server) uploadPart(writer http.ResponseWriter, request *http.Request, uploadID, rawPart string) {
	part, err := strconv.Atoi(rawPart)
	if err != nil || part <= 0 {
		writeError(writer, http.StatusBadRequest, "InvalidPart")
		return
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "IncompleteBody")
		return
	}
	s.mu.Lock()
	active, ok := s.uploads[uploadID]
	if ok {
		active.parts[part] = append([]byte(nil), body...)
	}
	s.mu.Unlock()
	if !ok {
		writeError(writer, http.StatusNotFound, "NoSuchUpload")
		return
	}
	stored := newObject(body)
	writer.Header().Set("ETag", stored.etag)
	writer.WriteHeader(http.StatusOK)
}

func (s *Server) completeMultipart(writer http.ResponseWriter, request *http.Request, bucket, key, uploadID string) {
	_, _ = io.Copy(io.Discard, request.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	active, ok := s.uploads[uploadID]
	if !ok || active.bucket != bucket || active.key != key {
		writeError(writer, http.StatusNotFound, "NoSuchUpload")
		return
	}
	id := objectID(bucket, key)
	if request.Header.Get("If-None-Match") == "*" {
		if _, exists := s.objects[id]; exists {
			delete(s.uploads, uploadID)
			writeError(writer, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
	}
	partNumbers := make([]int, 0, len(active.parts))
	for part := range active.parts {
		partNumbers = append(partNumbers, part)
	}
	sort.Ints(partNumbers)
	var body []byte
	for _, part := range partNumbers {
		body = append(body, active.parts[part]...)
	}
	stored := newObject(body)
	s.objects[id] = stored
	delete(s.uploads, uploadID)
	writeXML(writer, http.StatusOK, struct {
		XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
		Bucket  string   `xml:"Bucket"`
		Key     string   `xml:"Key"`
		ETag    string   `xml:"ETag"`
	}{Bucket: bucket, Key: key, ETag: stored.etag})
}

func (s *Server) abortMultipart(writer http.ResponseWriter, uploadID string) {
	s.mu.Lock()
	delete(s.uploads, uploadID)
	s.mu.Unlock()
	writer.WriteHeader(http.StatusNoContent)
}

func hasQueryKey(values map[string][]string, key string) bool {
	_, ok := values[key]
	return ok
}

func writeError(writer http.ResponseWriter, status int, code string) {
	writeXML(writer, status, struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{Code: code, Message: code})
}

func writeXML(writer http.ResponseWriter, status int, value any) {
	encoded, err := xml.Marshal(value)
	if err != nil {
		http.Error(writer, "fixture encoding failure", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/xml")
	writer.WriteHeader(status)
	_, _ = writer.Write(append([]byte(xml.Header), encoded...))
}
