# http-disk-cache

[![License](https://img.shields.io/github/license/artillery/node-http-disk-cache.svg)](https://github.com/artillery/node-http-disk-cache/blob/master/LICENSE)
[![Issues](https://img.shields.io/github/issues/artillery/node-http-disk-cache.svg)](https://github.com/artillery/node-http-disk-cache/issues)
[![Build Status](https://img.shields.io/circleci/project/artillery/node-http-disk-cache.svg)](https://circleci.com/gh/artillery/node-http-disk-cache)
[![Dependencies](https://img.shields.io/gemnasium/artillery/node-http-disk-cache.svg)](https://gemnasium.com/artillery/node-http-disk-cache)

An HTTP client that maintains a persistent disk-based cache. This module was written for internal use with [Project Atlas](https://www.artillery.com/atlas) but may be of use to others.

## Installation

    npm install http-disk-cache

## Example

```javascript
var cache = new HTTPCache('/cache/root/directory');

cache.getContents('http://example.com/url', function(err, buf, path) {
  if (err) {
    console.error("Error:", err);
  } else {
    console.log("Saved", buf.length, "bytes to path", path);
  }
};

cache.openReadStream('http://example.com/url', function(err, readStream) {
  if (err) { /* handle error */ }
  else { readStream.pipe(destination); }
});
```

## API

### Class: HTTPCache

#### new HTTPCache(cacheRoot)

Creates an HTTP cache directory which will work out of `cacheRoot`. The directory is created if it doesn't exist.

#### cache.getContents(url, callback)

Main URL request method for HTTPCache.

`url` may be a string URL or an object with a `url` key and any of the following options:
- `headers`: An object of headers to pass along with the request
- `encoding`: If specified, `callback` will get a String instead of a Buffer

Callback arguments:
- `err`: Optional error message
- `contents`: A String if the `encoding` option was specified, otherwise a Buffer
- `filename`: The path to the contents on disk (contents are always saved to disk regardless of cache expiry)

#### cache.openReadStream(url[, onProgress], callback)

Creates a readable stream for the contents of `url`.

`url` may be a string URL or an object with a `url` key and any of the following options:
- `headers`: An object of headers to pass along with the request

onProgress arguments:
- `numBytes`: Number of bytes read in the last chunk.

Callback arguments:
- `err`: Optional error message
- `stream`: A readable stream containing the content of the URL
- `filename`: The path to the contents on disk (contents are always saved to disk regardless of cache expiry)

#### cache.assertCached(url[, onProgress], callback)

Checks whether `url` is already in the cache and, if not, fetches and caches it. `url` may be a string or an object with the same options as `openReadStream()`. This is used to warm up the cache and is less resource-intensive than the other methods.

onProgress arguments:
- `numBytes`: Number of bytes read in the last chunk.

Callback arguments:
- `err`: Optional error message

#### cache.abortAllInFlightRequests()

Calls `abort()` on any in-progress HTTP requests that may have been initiated by `openReadStream()` or `assertCached()`. The callbacks for those methods will get called with an error.

#### cache.getContentPathname(url[, options])

Return a path to a requested URL assuming that it's already been cached.

Options:
- `absolute`: Return an absolute path instead of a path relative to the cache root (default: false)

#### cache.reset()

`reset` must be called if you wish to reload any expired assets. (Otherwise, assets always
persist for the lifetime of the cache, even if they would not otherwise be cached at all.)


## Notes

The on-disk cache is structured as follows:

- URLs are canonicalized and hashed via the MD5 algorithm.
- The hex digest of the hash is split into 3 pieces: characters 0-1, characters 2-3,
  and characters 4-31, and used to construct a pathname relative to the cache root, e.g.
  `79/da/646f91932de1ed0267ed1abdb741`
- The contents of the cached object are stored at that pathname. Additionally, metadata
  about the cached object is stored at that pathname with the suffix `.meta`.

A cached object is valid iff:

- The `.meta` file contains a valid JSON object.
- The expiration time stored in the .meta file under the `expiry` property has not elapsed.
- The contents stored in the cache object file have an MD5 sum equal to that stored in the
  `.meta` file as the `contentMD5` property.

A valid cached object may always be served in response to an `openReadStream` request.
An invalid cached object may be deleted at any time, and in practice, will be deleted when it
is next accessed while attempting to meet a request.

Fetched resources are always saved to disk regardless of expiry. This is ease integration with
things such as SDL functions which load images or dynamic libraries from disk.

Enable debug output by setting the `DEBUG` environment variable to include `http-disk-cache`

## License

[MIT](https://github.com/artillery/node-http-disk-cache/blob/master/LICENSE)
