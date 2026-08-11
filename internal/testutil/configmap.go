// Package testutil provides shared helpers for chalert tests.
package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

var payloadSeq atomic.Int64

// WriteConfigMapVolume emulates the kubelet AtomicWriter layout for a
// ConfigMap volume: files live in a payload directory, ..data is a symlink to
// it, and each key is a top-level symlink through ..data. Calling it again on
// the same dir performs the same atomic ..data swap the kubelet does on
// ConfigMap updates.
func WriteConfigMapVolume(t testing.TB, dir string, files map[string]string) {
	t.Helper()
	payloadName := fmt.Sprintf("..payload_%d", payloadSeq.Add(1))
	payload := filepath.Join(dir, payloadName)
	if err := os.Mkdir(payload, 0755); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(payload, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	tmpLink := filepath.Join(dir, "..data_tmp")
	if err := os.Symlink(payloadName, tmpLink); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpLink, filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}

	for name := range files {
		link := filepath.Join(dir, name)
		if _, err := os.Lstat(link); os.IsNotExist(err) {
			if err := os.Symlink(filepath.Join("..data", name), link); err != nil {
				t.Fatal(err)
			}
		}
	}
}
