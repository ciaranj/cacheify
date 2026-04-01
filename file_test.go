package cacheify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testCacheKey = "GETlocalhost:8080/test/path"

func TestFileCache(t *testing.T) {
	dir := createTempDir(t)

	fc, err := newFileCache(dir, time.Second, 255, 100, 8192)
	if err != nil {
		t.Errorf("unexpected newFileCache error: %v", err)
	}

	_, err = fc.GetStream(testCacheKey)
	if err == nil {
		t.Error("unexpected cache content")
	}

	cacheContent := []byte("some random cache content that should be exact")
	metadata := cacheMetadata{
		Status: 200,
		Headers: map[string][]string{
			"Content-Type": {"text/plain"},
		},
	}

	writer, err := fc.SetStream(testCacheKey, metadata, time.Second)
	if err != nil {
		t.Errorf("unexpected cache set error: %v", err)
	}
	if _, err := writer.Write(cacheContent); err != nil {
		t.Errorf("unexpected write error: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Errorf("unexpected commit error: %v", err)
	}

	resp, err := fc.GetStream(testCacheKey)
	if err != nil {
		t.Errorf("unexpected cache get error: %v", err)
	}
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("unexpected read error: %v", err)
	}

	if !bytes.Equal(got, cacheContent) {
		t.Errorf("unexpected cache content: want %s, got %s", cacheContent, got)
	}

	if resp.Metadata.Status != 200 {
		t.Errorf("unexpected status: want 200, got %d", resp.Metadata.Status)
	}
}

func TestFileCache_ConcurrentAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	dir := createTempDir(t)

	fc, err := newFileCache(dir, time.Second, 255, 100, 8192)
	if err != nil {
		t.Errorf("unexpected newFileCache error: %v", err)
	}

	cacheContent := []byte("some random cache content that should be exact")
	metadata := cacheMetadata{
		Status: 200,
		Headers: map[string][]string{
			"Content-Type": {"text/plain"},
		},
	}

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for {
			resp, _ := fc.GetStream(testCacheKey)
			if resp != nil {
				got, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if !bytes.Equal(got, cacheContent) {
					panic(fmt.Sprintf("unexpected cache content: want %s, got %s", cacheContent, got))
				}
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	go func() {
		defer wg.Done()

		for {
			writer, err := fc.SetStream(testCacheKey, metadata, time.Second)
			if err != nil {
				// Expected: cache write in progress (another writer has the lock)
				// Just skip and try again (don't panic on expected errors)
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			if _, err := writer.Write(cacheContent); err != nil {
				panic(fmt.Sprintf("unexpected write error: %v", err))
			}
			if err := writer.Commit(); err != nil {
				panic(fmt.Sprintf("unexpected commit error: %v", err))
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	wg.Wait()
}

func TestLockManager(t *testing.T) {
	lm := newLockManager()

	mu := lm.getLock("sometestpath")
	mu.Lock(lm)

	var (
		wg     sync.WaitGroup
		locked uint32
	)

	wg.Add(1)

	go func() {
		defer wg.Done()

		mu := lm.getLock("sometestpath")
		mu.Lock(lm)
		defer mu.Unlock(lm)

		atomic.AddUint32(&locked, 1)
	}()

	// locked should be 0 as we already have a lock on the path.
	if atomic.LoadUint32(&locked) != 0 {
		t.Error("unexpected second lock")
	}

	mu.Unlock(lm)

	wg.Wait()

	if l := len(lm.locks); l > 0 {
		t.Errorf("unexpected lock length: want 0, got %d", l)
	}
}

func TestLockManager_NoLeakAfterSetStreamCommit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Commit uses atomicRename which is not supported on Windows")
	}

	dir := createTempDir(t)

	fc, err := newFileCache(dir, time.Minute, 255, 100, 8192)
	if err != nil {
		t.Fatalf("unexpected newFileCache error: %v", err)
	}
	defer fc.Stop()

	metadata := cacheMetadata{
		Status: 200,
		Headers: map[string][]string{
			"Content-Type": {"text/plain"},
		},
	}

	// Write multiple unique keys through SetStream → Commit
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("GETlocalhost:8080/test/leak/%d", i)
		writer, err := fc.SetStream(key, metadata, time.Minute)
		if err != nil {
			t.Fatalf("unexpected SetStream error on key %d: %v", i, err)
		}
		if _, err := writer.Write([]byte("body")); err != nil {
			t.Fatalf("unexpected write error: %v", err)
		}
		if err := writer.Commit(); err != nil {
			t.Fatalf("unexpected commit error: %v", err)
		}
	}

	// After all writes complete, no lock entries should remain
	fc.lm.mu.Lock()
	remaining := len(fc.lm.locks)
	fc.lm.mu.Unlock()

	if remaining != 0 {
		t.Errorf("lockManager leaked %d entries, want 0", remaining)
	}
}

func TestLockManager_NoLeakAfterSetStreamAbort(t *testing.T) {
	dir := createTempDir(t)

	fc, err := newFileCache(dir, time.Minute, 255, 100, 8192)
	if err != nil {
		t.Fatalf("unexpected newFileCache error: %v", err)
	}
	defer fc.Stop()

	metadata := cacheMetadata{
		Status: 200,
		Headers: map[string][]string{
			"Content-Type": {"text/plain"},
		},
	}

	// Write multiple unique keys through SetStream → Abort
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("GETlocalhost:8080/test/abort-leak/%d", i)
		writer, err := fc.SetStream(key, metadata, time.Minute)
		if err != nil {
			t.Fatalf("unexpected SetStream error on key %d: %v", i, err)
		}
		if _, err := writer.Write([]byte("body")); err != nil {
			t.Fatalf("unexpected write error: %v", err)
		}
		if err := writer.Abort(); err != nil {
			t.Fatalf("unexpected abort error: %v", err)
		}
	}

	// After all aborts complete, no lock entries should remain
	fc.lm.mu.Lock()
	remaining := len(fc.lm.locks)
	fc.lm.mu.Unlock()

	if remaining != 0 {
		t.Errorf("lockManager leaked %d entries, want 0", remaining)
	}
}

func TestLockManager_NoLeakAfterGetStream(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Commit uses atomicRename which is not supported on Windows")
	}

	dir := createTempDir(t)

	fc, err := newFileCache(dir, time.Minute, 255, 100, 8192)
	if err != nil {
		t.Fatalf("unexpected newFileCache error: %v", err)
	}
	defer fc.Stop()

	metadata := cacheMetadata{
		Status: 200,
		Headers: map[string][]string{
			"Content-Type": {"text/plain"},
		},
	}

	// Seed one key
	writer, err := fc.SetStream(testCacheKey, metadata, time.Minute)
	if err != nil {
		t.Fatalf("unexpected SetStream error: %v", err)
	}
	_, _ = writer.Write([]byte("body"))
	if err := writer.Commit(); err != nil {
		t.Fatalf("unexpected commit error: %v", err)
	}

	// Read it many times (hits) and read missing keys (misses)
	for i := 0; i < 10; i++ {
		// Cache hit path
		resp, err := fc.GetStream(testCacheKey)
		if err != nil {
			t.Fatalf("unexpected GetStream error: %v", err)
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()

		// Cache miss path
		_, _ = fc.GetStream(fmt.Sprintf("GETlocalhost:8080/missing/%d", i))
	}

	// After all reads complete, no lock entries should remain
	fc.lm.mu.Lock()
	remaining := len(fc.lm.locks)
	fc.lm.mu.Unlock()

	if remaining != 0 {
		t.Errorf("lockManager leaked %d entries, want 0", remaining)
	}
}

func BenchmarkFileCache_Get(b *testing.B) {
	dir := createTempDir(b)

	fc, err := newFileCache(dir, time.Minute, 255, 100, 8192)
	if err != nil {
		b.Errorf("unexpected newFileCache error: %v", err)
	}

	metadata := cacheMetadata{
		Status: 200,
		Headers: map[string][]string{
			"Content-Type": {"text/plain"},
		},
	}
	cacheContent := []byte("some random cache content that should be exact")
	writer, _ := fc.SetStream(testCacheKey, metadata, time.Minute)
	_, _ = writer.Write(cacheContent)
	_ = writer.Commit()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, _ := fc.GetStream(testCacheKey)
		if resp != nil {
			_, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
	}
}
