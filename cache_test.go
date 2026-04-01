package cacheify

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "should error if path is not valid",
			cfg:     &Config{Path: "/foo/bar", MaxExpiry: 300, Cleanup: 600},
			wantErr: true,
		},
		{
			name:    "should error if maxExpiry <= 1",
			cfg:     &Config{Path: os.TempDir(), MaxExpiry: 1, Cleanup: 600},
			wantErr: true,
		},
		{
			name:    "should error if cleanup <= 1",
			cfg:     &Config{Path: os.TempDir(), MaxExpiry: 300, Cleanup: 1},
			wantErr: true,
		},
		{
			name:    "should be valid",
			cfg:     &Config{Path: os.TempDir(), MaxExpiry: 300, Cleanup: 600},
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(context.Background(), nil, test.cfg, "cacheify")

			if test.wantErr && err == nil {
				t.Fatal("expected error on bad regexp format")
			}
		})
	}
}

func TestCache_ServeHTTP(t *testing.T) {
	dir := createTempDir(t)

	next := func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Cache-Control", "max-age=20")
		rw.WriteHeader(http.StatusOK)
	}

	cfg := &Config{Path: dir, MaxExpiry: 10, Cleanup: 20, AddStatusHeader: true, MaxHeaderPairs: 2, MaxHeaderKeyLen: 30, MaxHeaderValueLen: 100}

	c, err := New(context.Background(), http.HandlerFunc(next), cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/some/path", nil)
	rw := httptest.NewRecorder()

	c.ServeHTTP(rw, req)

	if state := rw.Header().Get("Cache-Status"); state != "miss" {
		t.Errorf("unexpected cache state: want \"miss\", got: %q", state)
	}

	rw = httptest.NewRecorder()

	c.ServeHTTP(rw, req)

	if state := rw.Header().Get("Cache-Status"); state != "hit" {
		t.Errorf("unexpected cache state: want \"hit\", got: %q", state)
	}
}

func TestCache_WebSocketUpgradeBypass(t *testing.T) {
	dir := createTempDir(t)

	var nextReceivedWriter http.ResponseWriter

	next := func(rw http.ResponseWriter, req *http.Request) {
		nextReceivedWriter = rw
		rw.WriteHeader(http.StatusSwitchingProtocols)
	}

	cfg := &Config{
		Path:              dir,
		MaxExpiry:         10,
		Cleanup:           20,
		AddStatusHeader:   true,
		MaxHeaderPairs:    2,
		MaxHeaderKeyLen:   30,
		MaxHeaderValueLen: 100,
	}

	c, err := New(context.Background(), http.HandlerFunc(next), cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/ws", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	rw := httptest.NewRecorder()

	c.ServeHTTP(rw, req)

	// The next handler must receive the original ResponseWriter directly,
	// not our responseWriter wrapper — otherwise http.Hijacker will not be
	// available and Traefik's proxy will fail with "not a hijacker".
	if nextReceivedWriter != rw {
		t.Error("upgrade request was not passed through: next handler did not receive the original ResponseWriter")
	}

	// No Cache-Status header should be set for upgrade requests
	if state := rw.Header().Get("Cache-Status"); state != "" {
		t.Errorf("unexpected Cache-Status header on upgrade request: %q", state)
	}

	// Nothing should have been written to the cache directory
	verifyNoTempFiles(t, dir)
}

func TestCache_UpstreamFailureDuringStream(t *testing.T) {
	dir := createTempDir(t)

	// Simulate upstream server that fails mid-response
	next := func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Cache-Control", "max-age=20")
		rw.WriteHeader(http.StatusOK)

		// Write partial response
		_, _ = rw.Write([]byte("partial data"))

		// Simulate upstream failure by panicking
		// In a real scenario, this could be a network error, connection reset, etc.
		panic("upstream server failed mid-response")
	}

	cfg := &Config{
		Path:              dir,
		MaxExpiry:         10,
		Cleanup:           20,
		AddStatusHeader:   true,
		MaxHeaderPairs:    2,
		MaxHeaderKeyLen:   30,
		MaxHeaderValueLen: 100,
	}

	c, err := New(context.Background(), http.HandlerFunc(next), cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/some/path", nil)
	rw := httptest.NewRecorder()

	// Note: The panic may or may not propagate depending on the implementation
	// (for yaegi compatibility, we gracefully handle it rather than re-panicking)
	// The important test is whether partial data gets cached below
	func() {
		defer func() {
			_ = recover() // Silently catch if it panics
		}()
		c.ServeHTTP(rw, req)
	}()

	// The key question: is there a partial response cached?
	// Try to fetch from cache
	rw2 := httptest.NewRecorder()

	// Use a working backend for the second request
	workingNext := func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Cache-Control", "max-age=20")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("complete data from working backend"))
	}

	c2, err := New(context.Background(), http.HandlerFunc(workingNext), cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	c2.ServeHTTP(rw2, req)

	// Check if we got a cache hit or miss
	cacheStatus := rw2.Header().Get("Cache-Status")
	t.Logf("Cache status on second request: %s", cacheStatus)
	t.Logf("Response body on second request: %s", rw2.Body.String())

	// We expect a cache MISS because the partial write should have been cleaned up
	// If we get a cache HIT with partial data, that's a bug!
	if cacheStatus == "hit" && rw2.Body.String() == "partial data" {
		t.Fatal("BUG: partial response was cached! This should not happen.")
	}

	// Verify no partial or temp files remain in the cache directory
	verifyNoTempFiles(t, dir)
}

func TestCache_DownstreamFailureDuringStream(t *testing.T) {
	dir := createTempDir(t)

	// Upstream server that works fine
	next := func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Cache-Control", "max-age=20")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("complete response data"))
	}

	cfg := &Config{
		Path:              dir,
		MaxExpiry:         10,
		Cleanup:           20,
		AddStatusHeader:   true,
		MaxHeaderPairs:    2,
		MaxHeaderKeyLen:   30,
		MaxHeaderValueLen: 100,
	}

	c, err := New(context.Background(), http.HandlerFunc(next), cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/some/path", nil)

	// Create a ResponseWriter that simulates downstream failure
	failingWriter := &failingResponseWriter{
		ResponseWriter: httptest.NewRecorder(),
		failAfterBytes: 10, // Fail after writing 10 bytes
	}

	// Serve the request with a failing downstream
	// The error won't be returned directly (ServeHTTP doesn't return errors)
	// but we can check if the cache was properly aborted
	c.ServeHTTP(failingWriter, req)

	t.Logf("First request completed (downstream failed during write)")

	// The key question: is there a partial response cached?
	// Try to fetch from cache with a working client
	// This will HANG if the lock was not released!
	t.Log("Attempting second request (will hang if lock not released)...")

	done := make(chan bool)
	go func() {
		rw2 := httptest.NewRecorder()
		c.ServeHTTP(rw2, req)

		// Check if we got a cache hit or miss
		cacheStatus := rw2.Header().Get("Cache-Status")
		t.Logf("Cache status on second request: %s", cacheStatus)
		t.Logf("Response body on second request: %s", rw2.Body.String())

		// Question: Should this be a hit or miss?
		// - If MISS: downstream failure prevented caching (safest)
		// - If HIT with full data: cache succeeded despite downstream failure (acceptable)
		// - If HIT with partial data: BUG!

		if cacheStatus == "hit" && rw2.Body.String() != "complete response data" {
			t.Errorf("BUG: partial or incorrect response was cached! Got: %s", rw2.Body.String())
		}

		done <- true
	}()

	// Wait for completion with timeout
	select {
	case <-done:
		t.Log("Test completed successfully")
	case <-time.After(2 * time.Second):
		t.Fatal("BUG: Second request hung! The cache lock was never released after panic. This means finalize() was never called.")
	}

	// Verify no partial or temp files remain in the cache directory
	verifyNoTempFiles(t, dir)
}

func TestCache_NoCacheControl(t *testing.T) {
	dir := createTempDir(t)

	next := func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}

	cfg := &Config{Path: dir, MaxExpiry: 10, Cleanup: 20, AddStatusHeader: true, MaxHeaderPairs: 2, MaxHeaderKeyLen: 30, MaxHeaderValueLen: 100}

	c, err := New(context.Background(), http.HandlerFunc(next), cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/some/path", nil)
	rw := httptest.NewRecorder()

	c.ServeHTTP(rw, req)

	if state := rw.Header().Get("Cache-Status"); state != cacheMissStatus {
		t.Errorf("unexpected cache state: want \"miss\", got: %q", state)
	}

	rw = httptest.NewRecorder()

	c.ServeHTTP(rw, req)

	if state := rw.Header().Get("Cache-Status"); state != cacheHitStatus {
		t.Errorf("unexpected cache state: want \"hit\", got: %q", state)
	}
}

// failingResponseWriter simulates a downstream client that fails after writing N bytes
type failingResponseWriter struct {
	http.ResponseWriter
	written        int
	failAfterBytes int
}

func (w *failingResponseWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.failAfterBytes {
		// Fail partway through - return error like a real disconnect would
		n := w.failAfterBytes - w.written
		if n > 0 {
			w.written += n
			_, _ = w.ResponseWriter.Write(p[:n])
		}
		return n, errors.New("downstream connection failed")
	}

	n, err := w.ResponseWriter.Write(p)
	w.written += n
	return n, err
}

func createTempDir(tb testing.TB) string {
	tb.Helper()

	dir, err := ioutil.TempDir("./", "example")
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err = os.RemoveAll(dir); err != nil {
			tb.Fatal(err)
		}
	})

	return dir
}

func TestCache_DoubleCheckedLocking(t *testing.T) {
	dir := createTempDir(t)

	// Track upstream requests
	var upstreamCalls int32
	requestDelay := 100 * time.Millisecond

	next := func(rw http.ResponseWriter, req *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		// Simulate slow upstream
		time.Sleep(requestDelay)
		rw.Header().Set("Cache-Control", "max-age=20")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("response data"))
	}

	cfg := &Config{
		Path:              dir,
		MaxExpiry:         10,
		Cleanup:           20,
		AddStatusHeader:   true,
		MaxHeaderPairs:    2,
		MaxHeaderKeyLen:   30,
		MaxHeaderValueLen: 100,
		UpdateTimeout:     30, // 30 second timeout for waiting
	}

	c, err := New(context.Background(), http.HandlerFunc(next), cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	// Launch 10 concurrent requests for the same resource
	const numRequests = 10
	var wg sync.WaitGroup
	results := make([]string, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "http://localhost/test/path", nil)
			rw := httptest.NewRecorder()
			c.ServeHTTP(rw, req)
			results[idx] = rw.Header().Get("Cache-Status")
		}(i)
	}

	wg.Wait()

	// Check results
	upstreamCount := atomic.LoadInt32(&upstreamCalls)
	t.Logf("Upstream was called %d times for %d concurrent requests", upstreamCount, numRequests)

	// With double-checked locking, we should have:
	// - 1 upstream call (first request)
	// - Remaining requests either wait and get cache hit, or also call upstream
	// The ideal is 1, but we might get 2-3 due to timing
	if upstreamCount > 3 {
		t.Errorf("Too many upstream calls: got %d, expected <= 3 (double-checked locking not working)", upstreamCount)
	}

	// Count cache hits vs misses
	hits := 0
	misses := 0
	for _, status := range results {
		if status == "hit" {
			hits++
		} else if status == "miss" {
			misses++
		}
	}

	t.Logf("Results: %d hits, %d misses", hits, misses)

	// We should have at least some cache hits
	if hits == 0 {
		t.Error("Expected at least some cache hits from double-checked locking")
	}
}

func TestCache_UpdateTimeout(t *testing.T) {
	dir := createTempDir(t)

	// Track upstream requests
	var upstreamCalls int32

	next := func(rw http.ResponseWriter, req *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		// Simulate VERY slow upstream (hangs for 60 seconds)
		time.Sleep(60 * time.Second)
		rw.Header().Set("Cache-Control", "max-age=20")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("response data"))
	}

	cfg := &Config{
		Path:              dir,
		MaxExpiry:         10,
		Cleanup:           20,
		AddStatusHeader:   true,
		MaxHeaderPairs:    2,
		MaxHeaderKeyLen:   30,
		MaxHeaderValueLen: 100,
		UpdateTimeout:     1, // 1 second timeout - should fire
	}

	c, err := New(context.Background(), http.HandlerFunc(next), cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	// Launch 2 requests concurrently
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "http://localhost/slow", nil)
			rw := httptest.NewRecorder()
			c.ServeHTTP(rw, req)
		}()
	}

	// Wait with overall timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		upstreamCount := atomic.LoadInt32(&upstreamCalls)

		t.Logf("Both requests completed in %v", elapsed)
		t.Logf("Upstream called %d times", upstreamCount)

		// Should have 2 upstream calls (timeout causes second request to fetch too)
		if upstreamCount != 2 {
			t.Errorf("Expected 2 upstream calls due to timeout, got %d", upstreamCount)
		}

		// Should complete in ~60s (both running in parallel after timeout)
		// not 120s (sequential)
		if elapsed > 70*time.Second {
			t.Errorf("Took too long: %v (timeout not working?)", elapsed)
		}

	case <-time.After(75 * time.Second):
		t.Fatal("Test timed out - requests hung")
	}
}

func TestCache_StartupCleansUpTempFiles(t *testing.T) {
	dir := createTempDir(t)

	// Create some fake temp files to simulate crash leftovers
	tempFile1 := filepath.Join(dir, "fakehash.tmp.1234567890abcdef")
	tempFile2 := filepath.Join(dir, "anotherhash.tmp.fedcba0987654321")

	if err := os.WriteFile(tempFile1, []byte("orphaned temp data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tempFile2, []byte("more orphaned data"), 0600); err != nil {
		t.Fatal(err)
	}

	// Verify temp files exist before cache creation
	if _, err := os.Stat(tempFile1); os.IsNotExist(err) {
		t.Fatal("Test setup failed: temp file 1 was not created")
	}
	if _, err := os.Stat(tempFile2); os.IsNotExist(err) {
		t.Fatal("Test setup failed: temp file 2 was not created")
	}

	// Create a cache instance - this should clean up temp files on startup
	cfg := &Config{
		Path:              dir,
		MaxExpiry:         10,
		Cleanup:           20,
		AddStatusHeader:   true,
		MaxHeaderPairs:    2,
		MaxHeaderKeyLen:   30,
		MaxHeaderValueLen: 100,
	}

	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
	})

	_, err := New(context.Background(), next, cfg, "cacheify")
	if err != nil {
		t.Fatal(err)
	}

	// Verify temp files were cleaned up
	verifyNoTempFiles(t, dir)
}

// verifyNoTempFiles checks that no .tmp.* files remain in the cache directory
// This ensures partial writes are properly cleaned up
func verifyNoTempFiles(t *testing.T, dir string) {
	t.Helper()

	var tempFiles []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.Contains(filepath.Base(path), ".tmp.") {
			tempFiles = append(tempFiles, path)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Error walking cache directory: %v", err)
	}

	if len(tempFiles) > 0 {
		t.Errorf("BUG: Found %d temporary files that were not cleaned up:", len(tempFiles))
		for _, f := range tempFiles {
			t.Errorf("  - %s", f)
		}
		t.Fatal("Temporary files should be cleaned up after abort")
	}
}
