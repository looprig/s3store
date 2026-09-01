package s3store

import (
	"strings"

	"github.com/looprig/storage"
)

func validateBlobKey(key string) error {
	return storage.ValidateName(key)
}

func validateListPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	return storage.ValidateName(strings.TrimSuffix(prefix, "/"))
}
