# Cacheify

Simple cache plugin middleware caches responses on disk. 

Based on the original plugin-simplecache, but with some significant performance improvements
- Cache hits are now up to 13× faster and use 45% less memory, with large payloads seeing over 90% latency reduction.
- Streamlined miss handling and buffer reuse (-45–60% memory use on large bodies)
- Reduced allocations per operation by ~20% overall
- Improved concurrent hit performance (no longer contended)
- Simplified and accelerated cache key generation (-75–88% time)
- Achieved ~36% faster benchmarks overall and ~60% higher throughput

## Configuration

To configure this plugin you should add its configuration to the Traefik dynamic configuration as explained [here](https://docs.traefik.io/getting-started/configuration-overview/#the-dynamic-configuration).
The following snippet shows how to configure this plugin with the File provider in TOML and YAML: 

Static:

```toml
[experimental.plugins.cache]
  modulename = "github.com/ciaranj/cacheify"
  version = "v0.0.1"
```

Dynamic:

```toml
[http.middlewares]
  [http.middlewares.my-cache.plugin.cache]
    path = "/some/path/to/cache/dir"
```

```yaml
http:
  middlewares:
   my-cache:
      plugin:
        cache:
          path: /some/path/to/cache/dir
```

### Options

#### Path (`path`)

The base path that files will be created under. This must be a valid existing
filesystem path.

#### Max Expiry (`maxExpiry`)

*Default: 300*

The maximum number of seconds a response can be cached for. The 
actual cache time will always be lower or equal to this.

#### Cleanup (`cleanup`)

*Default: 600*

The number of seconds to wait between cache cleanup runs.
	
#### Add Status Header (`addStatusHeader`)

*Default: true*

This determines if the cache status header `Cache-Status` will be added to the
response headers. This header can have the value `hit`, `miss` or `error`.

#### Include Query Parameters in Cache Key (`queryInKey`)
*Default: true*

This determines whether the query parameters on the url form part of the key used for storing cacheable requests.

#### Max Header Pairs (`maxHeaderPairs`)
*Default: 255*

The maximum number of header key-value pairs allowed in cached responses. This prevents disk bloat attacks from responses with excessive headers. Multi-value headers (e.g., multiple `Set-Cookie` headers) count as separate pairs.

#### Max Header Key Length (`maxHeaderKeyLen`)
*Default: 100*

The maximum length in bytes for header keys (names). This prevents disk bloat from maliciously long header names. Standard HTTP header names are typically 10-30 bytes.

#### Max Header Value Length (`maxHeaderValueLen`)
*Default: 8192*

The maximum length in bytes for header values. This prevents disk bloat from oversized cookies, tokens, or other header values. The default allows for large JWTs and session cookies while preventing abuse.

#### Max Header Value Length (`stripResponseCookies`)
*Default: true*

If true (the default) cacheify will remove any 'Set-Cookie' headers from any cacheable responses (including the original request.)
