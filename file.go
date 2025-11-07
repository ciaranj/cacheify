// Package cacheify is a plugin to cache responses to disk.
package cacheify

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var errCacheMiss = errors.New("cache miss")

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
	pm                *pathMutex
	maxHeaderPairs    int
	maxHeaderKeyLen   int
	maxHeaderValueLen int
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
		pm:                &pathMutex{lock: map[string]*fileLock{}},
		maxHeaderPairs:    maxHeaderPairs,
		maxHeaderKeyLen:   maxHeaderKeyLen,
		maxHeaderValueLen: maxHeaderValueLen,
	}

	go fc.vacuum(vacuum)

	return fc, nil
}

func (c *fileCache) vacuum(interval time.Duration) {
	timer := time.NewTicker(interval)
	defer timer.Stop()

	for range timer.C {
		_ = filepath.Walk(c.path, func(path string, info os.FileInfo, err error) error {
			switch {
			case err != nil:
				return err
			case info.IsDir():
				return nil
			}

			key := filepath.Base(path)

			mu := c.pm.MutexAt(key)
			mu.Lock()
			defer mu.Unlock()

			// Get the expiry from cache file
			var t [8]byte
			f, err := os.Open(filepath.Clean(path))
			if err != nil {
				// Just skip the file in this case.
				return nil // nolint:nilerr // skip
			}
			if n, err := f.Read(t[:]); err != nil || n != 8 {
				_ = f.Close()
				return nil
			}
			_ = f.Close()

			expires := time.Unix(int64(binary.LittleEndian.Uint64(t[:])), 0)
			if !expires.Before(time.Now()) {
				return nil
			}

			// Delete the cache file
			_ = os.Remove(path)
			return nil
		})
	}
}

// GetStream returns a cached response with a streamable body
// File format: [8 bytes expiry][4 bytes metadata length][metadata JSON][body data]
func (c *fileCache) GetStream(key string) (*cachedResponse, error) {
	mu := c.pm.MutexAt(key)
	mu.RLock()

	p := keyPath(c.path, key)

	// Check if file exists
	if info, err := os.Stat(p); err != nil || info.IsDir() {
		mu.RUnlock()
		return nil, errCacheMiss
	}

	// Open file for reading
	file, err := os.Open(filepath.Clean(p))
	if err != nil {
		mu.RUnlock()
		return nil, fmt.Errorf("error opening cache file %q: %w", p, err)
	}

	// Read expiry timestamp (8 bytes)
	var expiryBuf [8]byte
	if _, err := io.ReadFull(file, expiryBuf[:]); err != nil {
		mu.RUnlock()
		_ = file.Close()
		return nil, errCacheMiss
	}

	expires := time.Unix(int64(binary.LittleEndian.Uint64(expiryBuf[:])), 0)
	if expires.Before(time.Now()) {
		mu.RUnlock()
		_ = file.Close()
		_ = os.Remove(p)
		return nil, errCacheMiss
	}

	// Read metadata length (4 bytes)
	var lenBuf [4]byte
	if _, err := io.ReadFull(file, lenBuf[:]); err != nil {
		mu.RUnlock()
		_ = file.Close()
		return nil, fmt.Errorf("error reading metadata length: %w", err)
	}
	metadataLen := binary.LittleEndian.Uint32(lenBuf[:])

	// Read metadata binary
	metadataBuf := make([]byte, metadataLen)
	if _, err := io.ReadFull(file, metadataBuf); err != nil {
		mu.RUnlock()
		_ = file.Close()
		return nil, fmt.Errorf("error reading metadata: %w", err)
	}

	// Unmarshal metadata
	metadata, err := unmarshalMetadata(metadataBuf)
	if err != nil {
		mu.RUnlock()
		_ = file.Close()
		return nil, fmt.Errorf("error unmarshaling metadata: %w", err)
	}

	// File is now positioned at start of body data - wrap for streaming
	body := &bodyReader{
		file: file,
		unlock: func() {
			mu.RUnlock()
		},
	}

	return &cachedResponse{
		Metadata: metadata,
		Body:     body,
	}, nil
}

// bodyReader wraps a file and adds cleanup on close
type bodyReader struct {
	file   *os.File
	unlock func()
}

func (br *bodyReader) Read(p []byte) (n int, err error) {
	return br.file.Read(p)
}

func (br *bodyReader) Close() error {
	err := br.file.Close()
	if br.unlock != nil {
		br.unlock()
		br.unlock = nil // Prevent double unlock
	}
	return err
}

// streamingCacheWriter handles streaming writes to a cache file
type streamingCacheWriter struct {
	file    *bufio.Writer
	rawFile *os.File
	pm      *pathMutex
	key     string
}

func (scw *streamingCacheWriter) Write(p []byte) (int, error) {
	return scw.file.Write(p)
}

func (scw *streamingCacheWriter) Abort() error {
	_ = scw.file.Flush()
	_ = scw.rawFile.Close()

	// Return buffer to pool
	scw.file.Reset(nil)
	bufferPool.Put(scw.file)

	mu := scw.pm.MutexAt(scw.key)
	mu.Unlock()
	return nil
}

func (scw *streamingCacheWriter) Commit() error {
	// Flush buffered writes
	if err := scw.file.Flush(); err != nil {
		_ = scw.rawFile.Close()
		scw.file.Reset(nil)
		bufferPool.Put(scw.file)
		mu := scw.pm.MutexAt(scw.key)
		mu.Unlock()
		return fmt.Errorf("error flushing cache file: %w", err)
	}

	if err := scw.rawFile.Close(); err != nil {
		scw.file.Reset(nil)
		bufferPool.Put(scw.file)
		mu := scw.pm.MutexAt(scw.key)
		mu.Unlock()
		return fmt.Errorf("error closing cache file: %w", err)
	}

	// Return buffer to pool
	scw.file.Reset(nil)
	bufferPool.Put(scw.file)

	mu := scw.pm.MutexAt(scw.key)
	mu.Unlock()

	return nil
}

// SetStream starts a streaming cache write
// Caller holds exclusive write lock until Commit() or Abort()
func (c *fileCache) SetStream(key string, metadata cacheMetadata, expiry time.Duration) (*streamingCacheWriter, error) {
	mu := c.pm.MutexAt(key)
	mu.Lock()

	p := keyPath(c.path, key)

	// Create directory structure
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		mu.Unlock()
		return nil, fmt.Errorf("error creating file path: %w", err)
	}

	// Marshal metadata to binary with configured limits
	metadataBinary, err := marshalMetadata(metadata, c.maxHeaderPairs, c.maxHeaderKeyLen, c.maxHeaderValueLen)
	if err != nil {
		mu.Unlock()
		return nil, fmt.Errorf("error marshaling metadata: %w", err)
	}

	// Write directly to destination file (we hold exclusive lock)
	file, err := os.OpenFile(filepath.Clean(p), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		mu.Unlock()
		return nil, fmt.Errorf("error creating cache file: %w", err)
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
		bufWriter.Reset(nil)
		bufferPool.Put(bufWriter)
		mu.Unlock()
		return nil, fmt.Errorf("error writing expiry: %w", err)
	}

	// Write metadata length (4 bytes)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(metadataBinary)))
	if _, err = bufWriter.Write(lenBuf[:]); err != nil {
		_ = file.Close()
		bufWriter.Reset(nil)
		bufferPool.Put(bufWriter)
		mu.Unlock()
		return nil, fmt.Errorf("error writing metadata length: %w", err)
	}

	// Write metadata binary
	if _, err = bufWriter.Write(metadataBinary); err != nil {
		_ = file.Close()
		bufWriter.Reset(nil)
		bufferPool.Put(bufWriter)
		mu.Unlock()
		return nil, fmt.Errorf("error writing metadata: %w", err)
	}

	// Return streaming writer (body will be written via Write() calls)
	return &streamingCacheWriter{
		file:    bufWriter,
		rawFile: file,
		pm:      c.pm,
		key:     key,
	}, nil
}

func keyHash(key string) [32]byte {
	return sha256.Sum256([]byte(key))
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

type pathMutex struct {
	mu   sync.Mutex
	lock map[string]*fileLock
}

func (m *pathMutex) MutexAt(path string) *fileLock {
	m.mu.Lock()
	defer m.mu.Unlock()

	if fl, ok := m.lock[path]; ok {
		fl.ref++
		return fl
	}

	fl := &fileLock{ref: 1}
	fl.cleanup = func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		fl.ref--
		if fl.ref == 0 {
			delete(m.lock, path)
		}
	}
	m.lock[path] = fl

	return fl
}

type fileLock struct {
	ref     int
	cleanup func()

	mu sync.RWMutex
}

func (l *fileLock) RLock() {
	l.mu.RLock()
}

func (l *fileLock) RUnlock() {
	l.mu.RUnlock()
	l.cleanup()
}

func (l *fileLock) Lock() {
	l.mu.Lock()
}

func (l *fileLock) Unlock() {
	l.mu.Unlock()
	l.cleanup()
}
