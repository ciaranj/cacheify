// Package cacheify is a plugin to cache responses to disk.
package cacheify

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var errCacheMiss = errors.New("cache miss")
var errCacheWriteInProgress = errors.New("cache write in progress")

// bufferPool pools 4KB bufio.Writer buffers to reduce allocations
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, 4*1024)
	},
}

// copyBufferPool pools 4KB byte slices for io.CopyBuffer
var copyBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 4*1024)
		return &b
	},
}

// hexBufferPool pools 64-byte buffers for SHA-256 hex encoding
var hexBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 64) // SHA-256 hash = 32 bytes → 64 hex chars
		return &b
	},
}

// cacheMetadata stores the metadata for a cached response
type cacheMetadata struct {
	Status  int
	Headers map[string][]string
}

// marshalMetadata serializes metadata to binary format
// Format: [2 bytes status][2 bytes header pair count][pairs...]
// Each pair: [2 bytes key len][key][3 bytes value len][value]
// Multi-value headers (e.g., multiple Set-Cookie) are stored as separate pairs
func marshalMetadata(m cacheMetadata, maxPairs, maxKeyLen, maxValueLen int) ([]byte, error) {
	// Count total header pairs (flattened)
	pairCount := 0
	size := 4 // status (2) + pair count (2)
	for k, vals := range m.Headers {
		for _, v := range vals {
			pairCount++
			size += 5 + len(k) + len(v) // key length (2) + key + value length (3) + value
		}
	}

	// Enforce configured limits
	if pairCount > maxPairs {
		return nil, fmt.Errorf("too many header pairs: %d (max: %d)", pairCount, maxPairs)
	}

	buf := make([]byte, 0, size)

	// Write status (2 bytes)
	buf = append(buf, byte(m.Status), byte(m.Status>>8))

	// Write header pair count (2 bytes)
	buf = append(buf, byte(pairCount), byte(pairCount>>8))

	// Write each header pair
	for key, values := range m.Headers {
		for _, val := range values {
			// Key length - enforce configured limit
			keyLen := len(key)
			if keyLen > maxKeyLen {
				return nil, fmt.Errorf("header key too long: %d bytes (max: %d)", keyLen, maxKeyLen)
			}
			buf = append(buf, byte(keyLen), byte(keyLen>>8))

			// Key
			buf = append(buf, []byte(key)...)

			// Value length - enforce configured limit
			valLen := len(val)
			if valLen > maxValueLen {
				return nil, fmt.Errorf("header value too long: %d bytes (max: %d)", valLen, maxValueLen)
			}
			buf = append(buf, byte(valLen), byte(valLen>>8), byte(valLen>>16))

			// Value
			buf = append(buf, []byte(val)...)
		}
	}

	return buf, nil
}

// unmarshalMetadata deserializes metadata from binary format
func unmarshalMetadata(data []byte) (cacheMetadata, error) {
	if len(data) < 4 {
		return cacheMetadata{}, fmt.Errorf("metadata too short: %d bytes", len(data))
	}

	pos := 0

	// Read status (2 bytes)
	status := int(data[pos]) | int(data[pos+1])<<8
	pos += 2

	// Read header pair count (2 bytes)
	pairCount := int(data[pos]) | int(data[pos+1])<<8
	pos += 2

	headers := make(map[string][]string)

	// Read each header pair
	for i := 0; i < pairCount; i++ {
		if pos+2 > len(data) {
			return cacheMetadata{}, fmt.Errorf("unexpected end of metadata at pair %d", i)
		}

		// Key length (2 bytes)
		keyLen := int(data[pos]) | int(data[pos+1])<<8
		pos += 2

		if pos+keyLen > len(data) {
			return cacheMetadata{}, fmt.Errorf("unexpected end of metadata reading key")
		}

		// Key
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		if pos+3 > len(data) {
			return cacheMetadata{}, fmt.Errorf("unexpected end of metadata at value length")
		}

		// Value length (3 bytes)
		valLen := int(data[pos]) | int(data[pos+1])<<8 | int(data[pos+2])<<16
		pos += 3

		if pos+valLen > len(data) {
			return cacheMetadata{}, fmt.Errorf("unexpected end of metadata reading value")
		}

		// Value
		value := string(data[pos : pos+valLen])
		pos += valLen

		// Append to headers map (handles multi-value headers)
		headers[key] = append(headers[key], value)
	}

	return cacheMetadata{
		Status:  status,
		Headers: headers,
	}, nil
}

// cachedResponse represents a cached HTTP response with streamable body
type cachedResponse struct {
	Metadata cacheMetadata
	Body     io.ReadCloser
}

type fileCache struct {
	path              string
	lm                *lockManager
	maxHeaderPairs    int
	maxHeaderKeyLen   int
	maxHeaderValueLen int
	stopVacuum        chan struct{}
	stopOnce          sync.Once
	updateIntents     map[string]*updateIntent // Track in-progress cache updates
	updateIntentsLock sync.RWMutex             // Protects updateIntents map
}

// updateIntent tracks an in-progress cache update
// Uses a channel for efficient signaling without goroutines or mutexes
type updateIntent struct {
	done chan struct{} // Closed when update completes, signals all waiters
}

func newFileCache(path string, vacuum time.Duration, maxHeaderPairs, maxHeaderKeyLen, maxHeaderValueLen int) (*fileCache, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("invalid cache path: %w", err)
	}

	if !info.IsDir() {
		return nil, errors.New("path must be a directory")
	}

	fc := &fileCache{
		path:              path,
		lm:                newLockManager(),
		maxHeaderPairs:    maxHeaderPairs,
		maxHeaderKeyLen:   maxHeaderKeyLen,
		maxHeaderValueLen: maxHeaderValueLen,
		stopVacuum:        make(chan struct{}),
		updateIntents:     make(map[string]*updateIntent),
	}

	// Clean up any orphaned temp files from previous crashes/restarts
	// This runs synchronously on startup to ensure clean state
	fc.cleanupTempFiles()

	go fc.vacuum(vacuum)

	return fc, nil
}

func (c *fileCache) vacuum(interval time.Duration) {
	timer := time.NewTicker(interval)
	defer timer.Stop()

	for {
		select {
		case <-c.stopVacuum:
			return
		case <-timer.C:
			c.cleanupExpiredFiles()
		}
	}
}

// cleanupExpiredFiles removes expired cache files and aged temp files
func (c *fileCache) cleanupExpiredFiles() {
	_ = filepath.Walk(c.path, func(path string, info os.FileInfo, err error) error {
		switch {
		case err != nil:
			return err
		case info.IsDir():
			return nil
		}

		filename := filepath.Base(path)

		// Clean up temp files that are older than 1 hour
		// These are orphaned from crashes or failed writes
		if isTempFile(filename) {
			if time.Since(info.ModTime()) > time.Hour {
				_ = os.Remove(path)
			}
			return nil
		}

		key := filename
		mu := c.lm.getLock(key)
		mu.Lock(c.lm)

		// Get the expiry from cache file
		var t [8]byte
		f, err := os.Open(filepath.Clean(path))
		if err != nil {
			mu.Unlock(c.lm)
			// Just skip the file in this case.
			return nil // nolint:nilerr // skip
		}
		if n, err := f.Read(t[:]); err != nil || n != 8 {
			_ = f.Close()
			mu.Unlock(c.lm)
			return nil
		}
		_ = f.Close()

		expires := time.Unix(int64(binary.LittleEndian.Uint64(t[:])), 0)
		if !expires.Before(time.Now()) {
			mu.Unlock(c.lm)
			return nil
		}

		// Delete the expired cache file
		_ = os.Remove(path)
		mu.Unlock(c.lm)
		return nil
	})
}

// Stop gracefully stops the vacuum goroutine
// Safe to call multiple times
func (c *fileCache) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopVacuum)
	})
}

// claimUpdateIntent attempts to claim responsibility for updating this cache key
// Returns true if we claimed it (we should fetch), false if someone else has it (we should wait)
func (c *fileCache) claimUpdateIntent(key string) bool {
	c.updateIntentsLock.Lock()
	defer c.updateIntentsLock.Unlock()

	// Check if someone else is already updating this key
	if _, exists := c.updateIntents[key]; exists {
		// Someone else claimed it already
		return false
	}

	// We claim it - create intent with done channel
	intent := &updateIntent{
		done: make(chan struct{}),
	}
	c.updateIntents[key] = intent

	return true
}

// releaseUpdateIntent releases our claim on updating this cache key
func (c *fileCache) releaseUpdateIntent(key string) {
	c.updateIntentsLock.Lock()
	defer c.updateIntentsLock.Unlock()

	if intent, ok := c.updateIntents[key]; ok {
		close(intent.done) // Signal all waiters that update is complete
		delete(c.updateIntents, key)
	}
}

// waitForUpdateIntent waits for someone else's update to complete
// Returns true if wait completed, false if timed out
// Uses channel-based signaling for efficient waiting
func (c *fileCache) waitForUpdateIntent(key string, timeout time.Duration) bool {
	// Get the intent to wait on
	c.updateIntentsLock.RLock()
	intent, exists := c.updateIntents[key]
	c.updateIntentsLock.RUnlock()

	if !exists {
		// No intent found - return immediately
		return true
	}

	// Wait for done channel to be closed or timeout
	select {
	case <-intent.done:
		return true // Wait completed successfully
	case <-time.After(timeout):
		// Timeout - clean up the stuck intent to prevent permanent blocking
		c.updateIntentsLock.Lock()
		delete(c.updateIntents, key)
		c.updateIntentsLock.Unlock()
		return false // Timed out
	}
}

// GetStream returns a cached response with a streamable body
// File format: [8 bytes expiry][4 bytes metadata length][metadata JSON][body data]
func (c *fileCache) GetStream(key string) (*cachedResponse, error) {
	mu := c.lm.getLock(key)
	mu.RLock(c.lm)

	p := keyPath(c.path, key)

	// Check if file exists
	if info, err := os.Stat(p); err != nil || info.IsDir() {
		mu.RUnlock(c.lm)
		return nil, errCacheMiss
	}

	// Open file for reading
	file, err := os.Open(filepath.Clean(p))
	if err != nil {
		mu.RUnlock(c.lm)
		return nil, fmt.Errorf("error opening cache file %q: %w", p, err)
	}

	// Read expiry timestamp (8 bytes)
	var expiryBuf [8]byte
	if _, err := io.ReadFull(file, expiryBuf[:]); err != nil {
		mu.RUnlock(c.lm)
		_ = file.Close()
		return nil, errCacheMiss
	}

	expires := time.Unix(int64(binary.LittleEndian.Uint64(expiryBuf[:])), 0)
	if expires.Before(time.Now()) {
		mu.RUnlock(c.lm)
		_ = file.Close()
		_ = os.Remove(p)
		return nil, errCacheMiss
	}

	// Read metadata length (4 bytes)
	var lenBuf [4]byte
	if _, err := io.ReadFull(file, lenBuf[:]); err != nil {
		mu.RUnlock(c.lm)
		_ = file.Close()
		return nil, fmt.Errorf("error reading metadata length: %w", err)
	}
	metadataLen := binary.LittleEndian.Uint32(lenBuf[:])

	// Protect against metadata bomb DoS attack
	const maxMetadataSize = 1 << 20 // 1MB should be more than enough for headers
	if metadataLen > maxMetadataSize {
		mu.RUnlock(c.lm)
		_ = file.Close()
		_ = os.Remove(p) // Remove malicious/corrupt file
		return nil, fmt.Errorf("metadata too large: %d bytes (max: %d)", metadataLen, maxMetadataSize)
	}

	// Read metadata binary
	metadataBuf := make([]byte, metadataLen)
	if _, err := io.ReadFull(file, metadataBuf); err != nil {
		mu.RUnlock(c.lm)
		_ = file.Close()
		return nil, fmt.Errorf("error reading metadata: %w", err)
	}

	// Unmarshal metadata
	metadata, err := unmarshalMetadata(metadataBuf)
	if err != nil {
		mu.RUnlock(c.lm)
		_ = file.Close()
		return nil, fmt.Errorf("error unmarshaling metadata: %w", err)
	}

	// File is now positioned at start of body data - wrap for streaming
	body := &bodyReader{
		file: file,
		lm:   c.lm,
		path: key,
	}

	return &cachedResponse{
		Metadata: metadata,
		Body:     body,
	}, nil
}

// bodyReader wraps a file and adds cleanup on close
type bodyReader struct {
	file *os.File
	lm   *lockManager
	path string
}

func (br *bodyReader) Read(p []byte) (n int, err error) {
	return br.file.Read(p)
}

func (br *bodyReader) Close() error {
	err := br.file.Close()
	if br.lm != nil {
		handle := lockHandle{path: br.path}
		handle.RUnlock(br.lm)
		br.lm = nil // Prevent double unlock
	}
	return err
}

// streamingCacheWriter handles streaming writes to a cache file
type streamingCacheWriter struct {
	file         *bufio.Writer
	rawFile      *os.File
	lm           *lockManager
	lockHandle   lockHandle // Original lock handle from SetStream — must be used for Unlock
	key          string
	filepath     string // Final destination path
	tempFilepath string // Temporary write path
	done         bool   // Prevents double Commit/Abort
	mu           sync.Mutex
}

func (scw *streamingCacheWriter) Write(p []byte) (int, error) {
	return scw.file.Write(p)
}

func (scw *streamingCacheWriter) Abort() error {
	scw.mu.Lock()
	defer scw.mu.Unlock()

	if scw.done {
		return nil // Already committed or aborted
	}
	scw.done = true

	_ = scw.file.Flush()
	_ = scw.rawFile.Close()

	// Clean up temporary file (not the final filepath, which shouldn't exist yet)
	if err := os.Remove(scw.tempFilepath); err != nil {
		// Log but don't fail - cleanup is best effort
		// The vacuum process will eventually remove expired temp files
	}

	// Return buffer to pool
	scw.file.Reset(nil)
	bufferPool.Put(scw.file)

	scw.lockHandle.Unlock(scw.lm)
	return nil
}

func (scw *streamingCacheWriter) Commit() error {
	scw.mu.Lock()
	defer scw.mu.Unlock()

	if scw.done {
		return nil // Already committed or aborted
	}
	scw.done = true

	// Flush buffered writes
	if err := scw.file.Flush(); err != nil {
		_ = scw.rawFile.Close()
		_ = os.Remove(scw.tempFilepath) // Clean up temp file on error
		scw.file.Reset(nil)
		bufferPool.Put(scw.file)
		scw.lockHandle.Unlock(scw.lm)
		return fmt.Errorf("error flushing cache file: %w", err)
	}

	// Close the file BEFORE rename (required on Windows)
	if err := scw.rawFile.Close(); err != nil {
		_ = os.Remove(scw.tempFilepath) // Clean up temp file on error
		scw.file.Reset(nil)
		bufferPool.Put(scw.file)
		scw.lockHandle.Unlock(scw.lm)
		return fmt.Errorf("error closing cache file: %w", err)
	}

	// Return buffer to pool
	scw.file.Reset(nil)
	bufferPool.Put(scw.file)

	// Atomically move temp file to final destination
	// This ensures partial writes are never visible as valid cache entries
	if err := atomicRename(scw.tempFilepath, scw.filepath); err != nil {
		_ = os.Remove(scw.tempFilepath) // Clean up on failure
		scw.lockHandle.Unlock(scw.lm)
		return fmt.Errorf("error committing cache file: %w", err)
	}

	scw.lockHandle.Unlock(scw.lm)

	return nil
}

// SetStream starts a streaming cache write
// Caller holds exclusive write lock until Commit() or Abort()
// The updateIntent mechanism ensures only one writer per key, so we use blocking Lock()
func (c *fileCache) SetStream(key string, metadata cacheMetadata, expiry time.Duration) (*streamingCacheWriter, error) {
	mu := c.lm.getLock(key)

	// Acquire write lock (blocking)
	// This is safe because updateIntent ensures only one writer per key attempts this
	mu.Lock(c.lm)

	p := keyPath(c.path, key)

	// Create directory structure
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		mu.Unlock(c.lm)
		return nil, fmt.Errorf("error creating file path: %w", err)
	}

	// Marshal metadata to binary with configured limits
	metadataBinary, err := marshalMetadata(metadata, c.maxHeaderPairs, c.maxHeaderKeyLen, c.maxHeaderValueLen)
	if err != nil {
		mu.Unlock(c.lm)
		return nil, fmt.Errorf("error marshaling metadata: %w", err)
	}

	// Write to a temporary file first, then atomically rename on commit
	// This prevents partial writes from ever being visible as valid cache entries
	// Format: {final-path}.tmp.{random}
	tempPath := p + ".tmp." + randomSuffix()
	file, err := os.OpenFile(filepath.Clean(tempPath), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		mu.Unlock(c.lm)
		return nil, fmt.Errorf("error creating temp cache file: %w", err)
	}

	// Get pooled 4KB buffer and attach to file
	bufWriter := bufferPool.Get().(*bufio.Writer)
	bufWriter.Reset(file)

	// Write expiry timestamp (8 bytes)
	timestamp := uint64(time.Now().Add(expiry).Unix())
	var expiryBuf [8]byte
	binary.LittleEndian.PutUint64(expiryBuf[:], timestamp)
	if _, err = bufWriter.Write(expiryBuf[:]); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath) // Clean up temp file
		bufWriter.Reset(nil)
		bufferPool.Put(bufWriter)
		mu.Unlock(c.lm)
		return nil, fmt.Errorf("error writing expiry: %w", err)
	}

	// Write metadata length (4 bytes)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(metadataBinary)))
	if _, err = bufWriter.Write(lenBuf[:]); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath) // Clean up temp file
		bufWriter.Reset(nil)
		bufferPool.Put(bufWriter)
		mu.Unlock(c.lm)
		return nil, fmt.Errorf("error writing metadata length: %w", err)
	}

	// Write metadata binary
	if _, err = bufWriter.Write(metadataBinary); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath) // Clean up temp file
		bufWriter.Reset(nil)
		bufferPool.Put(bufWriter)
		mu.Unlock(c.lm)
		return nil, fmt.Errorf("error writing metadata: %w", err)
	}

	// Return streaming writer (body will be written via Write() calls)
	return &streamingCacheWriter{
		file:         bufWriter,
		rawFile:      file,
		lm:           c.lm,
		lockHandle:   mu,
		key:          key,
		filepath:     p,
		tempFilepath: tempPath,
	}, nil
}

func keyHash(key string) [32]byte {
	return sha256.Sum256([]byte(key))
}

// randomSuffix generates a random suffix for temporary files
func randomSuffix() string {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

// isTempFile checks if a filename is a temporary cache file
// Temp files have the format: {hash}.tmp.{random16chars}
func isTempFile(filename string) bool {
	return strings.Contains(filename, ".tmp.")
}

// cleanupTempFiles removes all temporary files in the cache directory
// This is called on startup to clean up orphaned temp files from crashes
func (c *fileCache) cleanupTempFiles() {
	_ = filepath.Walk(c.path, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		if isTempFile(filepath.Base(path)) {
			_ = os.Remove(path)
		}

		return nil
	})
}

func keyPath(path, key string) string {
	h := keyHash(key)

	// Get pooled buffer for hex encoding (avoids allocation)
	bufPtr := hexBufferPool.Get().(*[]byte)
	defer hexBufferPool.Put(bufPtr)
	hexBuf := *bufPtr
	hex.Encode(hexBuf, h[:])

	// Build path manually to minimize allocations
	// Estimate: len(path) + 5 separators + 2+2+2+2+64 = len(path) + 77
	const pathElements = 77
	result := make([]byte, 0, len(path)+pathElements)
	result = append(result, path...)
	result = append(result, filepath.Separator)
	result = append(result, hexBuf[0:2]...)
	result = append(result, filepath.Separator)
	result = append(result, hexBuf[2:4]...)
	result = append(result, filepath.Separator)
	result = append(result, hexBuf[4:6]...)
	result = append(result, filepath.Separator)
	result = append(result, hexBuf[6:8]...)
	result = append(result, filepath.Separator)
	result = append(result, hexBuf...)

	return string(result)
}

// lockManager manages per-path locks using string keys instead of pointers
// This avoids yaegi issues with custom pointer types in maps and closures
type lockManager struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

// lockEntry contains the actual RWMutex and reference count
// Note: We still use *lockEntry in the map, but we never pass this
// pointer type through reflection or store it in other structs
type lockEntry struct {
	mu  sync.RWMutex
	ref int
}

// lockHandle is what we return to callers - just data, no pointers
// This is the key to yaegi compatibility
type lockHandle struct {
	path string
}

func newLockManager() *lockManager {
	return &lockManager{
		locks: make(map[string]*lockEntry),
	}
}

// getLock returns a lock handle for the given path
// This is the replacement for pathMutex.MutexAt()
func (lm *lockManager) getLock(path string) lockHandle {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[path]
	if !exists {
		entry = &lockEntry{ref: 0}
		lm.locks[path] = entry
	}
	entry.ref++

	return lockHandle{path: path}
}

// releaseLock decrements reference count and cleans up if needed
// This replaces the closure-based cleanup
func (lm *lockManager) releaseLock(path string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[path]
	if !exists {
		return // Already cleaned up
	}

	entry.ref--
	if entry.ref == 0 {
		delete(lm.locks, path)
	}
}

// lockEntry returns the actual mutex for operations
// This is an internal helper - never exposes *lockEntry to callers
func (lm *lockManager) lockEntry(path string) *lockEntry {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.locks[path] // May be nil if already cleaned up
}

// RLock acquires a read lock
func (h lockHandle) RLock(lm *lockManager) {
	entry := lm.lockEntry(h.path)
	if entry != nil {
		entry.mu.RLock()
	}
}

// RUnlock releases a read lock and cleans up
func (h lockHandle) RUnlock(lm *lockManager) {
	entry := lm.lockEntry(h.path)
	if entry != nil {
		entry.mu.RUnlock()
	}
	lm.releaseLock(h.path)
}

// Lock acquires a write lock
func (h lockHandle) Lock(lm *lockManager) {
	entry := lm.lockEntry(h.path)
	if entry != nil {
		entry.mu.Lock()
	}
}

// Unlock releases a write lock and cleans up
func (h lockHandle) Unlock(lm *lockManager) {
	entry := lm.lockEntry(h.path)
	if entry != nil {
		entry.mu.Unlock()
	}
	lm.releaseLock(h.path)
}

// atomicRename performs an atomic rename operation
// On Windows, returns an error as this is not supported in Yaegi
// On Unix systems, os.Rename is atomic when source and destination are on the same filesystem
func atomicRename(oldpath, newpath string) error {
	if runtime.GOOS == "windows" {
		return errors.New("cacheify plugin is not supported on Windows - Unix/Linux/macOS only")
	}
	return os.Rename(oldpath, newpath)
}
