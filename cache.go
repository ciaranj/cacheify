// Package cacheify is a plugin to cache responses to disk.
package cacheify

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pquerna/cachecontrol"
)

// Config configures the middleware.
type Config struct {
	Path                 string `json:"path" yaml:"path" toml:"path"`
	MaxExpiry            int    `json:"maxExpiry" yaml:"maxExpiry" toml:"maxExpiry"`
	Cleanup              int    `json:"cleanup" yaml:"cleanup" toml:"cleanup"`
	AddStatusHeader      bool   `json:"addStatusHeader" yaml:"addStatusHeader" toml:"addStatusHeader"`
	QueryInKey           bool   `json:"queryInKey" yaml:"queryInKey" toml:"queryInKey"`
	StripResponseCookies bool   `json:"stripResponseCookies" yaml:"stripResponseCookies" toml:"stripResponseCookies"`
	MaxHeaderPairs       int    `json:"maxHeaderPairs" yaml:"maxHeaderPairs" toml:"maxHeaderPairs"`
	MaxHeaderKeyLen      int    `json:"maxHeaderKeyLen" yaml:"maxHeaderKeyLen" toml:"maxHeaderKeyLen"`
	MaxHeaderValueLen    int    `json:"maxHeaderValueLen" yaml:"maxHeaderValueLen" toml:"maxHeaderValueLen"`
}

// CreateConfig returns a config instance.
func CreateConfig() *Config {
	return &Config{
		MaxExpiry:            int((5 * time.Minute).Seconds()),
		Cleanup:              int((10 * time.Minute).Seconds()),
		AddStatusHeader:      true,
		QueryInKey:           true,
		StripResponseCookies: true,
		MaxHeaderPairs:       255,
		MaxHeaderKeyLen:      100,
		MaxHeaderValueLen:    8192,
	}
}

const (
	cacheHeader      = "Cache-Status"
	cacheHitStatus   = "hit"
	cacheMissStatus  = "miss"
	cacheErrorStatus = "error"
)

type cache struct {
	name  string
	cache *fileCache
	cfg   *Config
	next  http.Handler
}

// New returns a plugin instance.
func New(_ context.Context, next http.Handler, cfg *Config, name string) (http.Handler, error) {
	if cfg.MaxExpiry <= 1 {
		return nil, errors.New("maxExpiry must be greater or equal to 1")
	}

	if cfg.Cleanup <= 1 {
		return nil, errors.New("cleanup must be greater or equal to 1")
	}

	fc, err := newFileCache(
		cfg.Path,
		time.Duration(cfg.Cleanup)*time.Second,
		cfg.MaxHeaderPairs,
		cfg.MaxHeaderKeyLen,
		cfg.MaxHeaderValueLen,
	)
	if err != nil {
		return nil, err
	}

	m := &cache{
		name:  name,
		cache: fc,
		cfg:   cfg,
		next:  next,
	}

	return m, nil
}

type cacheData struct {
	Status  int
	Headers map[string][]string
	Body    []byte
}

// ServeHTTP serves an HTTP request.
func (m *cache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cs := cacheMissStatus

	key := cacheKey(r, m.cfg.QueryInKey)

	// Try to serve from cache
	cached, err := m.cache.GetStream(key)
	if err == nil {
		defer cached.Body.Close()

		// Write headers
		for key, vals := range cached.Metadata.Headers {
			for _, val := range vals {
				w.Header().Add(key, val)
			}
		}
		if m.cfg.AddStatusHeader {
			w.Header().Set(cacheHeader, cacheHitStatus)
		}

		// Write status
		w.WriteHeader(cached.Metadata.Status)

		// Stream body using pooled buffer to reduce allocations
		buf := copyBufferPool.Get().(*[]byte)
		_, _ = io.CopyBuffer(w, cached.Body, *buf)
		copyBufferPool.Put(buf)
		return
	}

	// Cache miss - proceed with backend request
	rw := &responseWriter{
		ResponseWriter: w,
		cache:          m.cache,
		cacheKey:       key,
		request:        r,
		config:         m.cfg,
		checkCacheable: m.cacheable,
	}
	m.next.ServeHTTP(rw, r)

	// Finalize cache write if started
	if err := rw.finalize(); err != nil {
		log.Printf("Error finalizing cache: %v", err)
	}

	// Add Cache-Status header after response is complete
	if m.cfg.AddStatusHeader {
		if rw.wasCached {
			w.Header().Set(cacheHeader, cacheMissStatus)
		} else {
			w.Header().Set(cacheHeader, cs)
		}
	}
}

func (m *cache) cacheable(r *http.Request, w http.ResponseWriter, status int) (time.Duration, bool) {
	reasons, expireBy, err := cachecontrol.CachableResponseWriter(r, status, w, cachecontrol.Options{})
	if err != nil || len(reasons) > 0 {
		return 0, false
	}

	expiry := time.Until(expireBy)
	maxExpiry := time.Duration(m.cfg.MaxExpiry) * time.Second

	if maxExpiry < expiry {
		expiry = maxExpiry
	}

	return expiry, true
}

func cacheKey(r *http.Request, includeQuery bool) string {
	// Use strings.Builder to avoid multiple allocations
	var b strings.Builder

	// Pre-allocate approximate capacity
	b.Grow(len(r.Method) + len(r.Host) + len(r.URL.Path) + len(r.URL.RawQuery) + 10)

	// Base key with method, host and path
	b.WriteString(r.Method)
	b.WriteString(r.Host)
	b.WriteString(r.URL.Path)

	// Handle query parameters in a sorted, consistent way
	if includeQuery && r.URL.RawQuery != "" {
		query := r.URL.Query() // Parse once and cache

		if len(query) > 0 {
			// Get all query parameter keys
			params := make([]string, 0, len(query))
			for param := range query {
				params = append(params, param)
			}

			// Sort the parameter keys
			sort.Strings(params)

			b.WriteByte('?')
			first := true
			for _, param := range params {
				values := query[param]
				sort.Strings(values)

				for _, value := range values {
					if !first {
						b.WriteByte('&')
					}
					first = false
					b.WriteString(url.QueryEscape(param))
					b.WriteByte('=')
					b.WriteString(url.QueryEscape(value))
				}
			}
		}
	}

	return b.String()
}

type responseWriter struct {
	http.ResponseWriter
	cache          *fileCache
	cacheKey       string
	request        *http.Request
	config         *Config
	checkCacheable func(*http.Request, http.ResponseWriter, int) (time.Duration, bool)

	status        int
	headerWritten bool
	wasCached     bool
	cacheWriter   *streamingCacheWriter
}

func (rw *responseWriter) Header() http.Header {
	return rw.ResponseWriter.Header()
}

func (rw *responseWriter) WriteHeader(s int) {
	if rw.headerWritten {
		return
	}
	rw.headerWritten = true
	rw.status = s

	// Make cache decision now that we have status and headers
	expiry, cacheable := rw.checkCacheable(rw.request, rw.ResponseWriter, s)

	if cacheable {
		// Strip Set-Cookie headers if configured (affects both cache and response)
		if rw.config.StripResponseCookies {
			rw.ResponseWriter.Header().Del("Set-Cookie")
		}

		// Start streaming cache write
		metadata := cacheMetadata{
			Status:  s,
			Headers: rw.ResponseWriter.Header(),
		}

		var err error
		rw.cacheWriter, err = rw.cache.SetStream(rw.cacheKey, metadata, expiry)
		if err != nil {
			log.Printf("Error starting cache write: %v", err)
		} else {
			rw.wasCached = true
		}
	}

	rw.ResponseWriter.WriteHeader(s)
}

func (rw *responseWriter) Write(p []byte) (int, error) {
	// Ensure WriteHeader was called
	if !rw.headerWritten {
		rw.WriteHeader(http.StatusOK)
	}

	// Write to cache if we're caching
	if rw.cacheWriter != nil {
		if _, err := rw.cacheWriter.Write(p); err != nil {
			log.Printf("Error writing to cache: %v", err)
			// Don't fail the request, just stop caching
			_ = rw.cacheWriter.Abort()
			rw.cacheWriter = nil
		}
	}

	// Always write to client
	return rw.ResponseWriter.Write(p)
}

func (rw *responseWriter) finalize() error {
	if rw.cacheWriter != nil {
		return rw.cacheWriter.Commit()
	}
	return nil
}
