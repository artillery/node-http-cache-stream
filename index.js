var async = require('artillery-async');
var crypto = require('crypto');
var fs = require('fs');
var http = require('http');
var mkdirp = require('mkdirp');
var pathlib = require('path');
var request = require('request');
var urllib = require('url');
var debug = require('debug')('http-disk-cache');

/////////////// CacheEntry ///////////////

function canonicalUrl(url) {
  var parsedUrl = urllib.parse(url);
  delete parsedUrl.hash;
  delete parsedUrl.href;
  return urllib.format(parsedUrl);
}

// CacheEntry maps a url to relative pathnames for that URLs contents and metadata files.
function CacheEntry(url) {
  url = canonicalUrl(url);
  this.url = url;

  // While it would be nice to use sha256, we need compatability with s3 ETags, which are md5.
  var hash = crypto.createHash('md5');
  hash.update(url, 'utf8');
  this.urlHash = hash.digest('hex');

  // Cache objects are stored as 'ab/3f/423efacd...'
  var dirs = [this.urlHash.slice(0, 2), this.urlHash.slice(2, 4), this.urlHash.slice(4)];

  this.contentPath = pathlib.join.apply(null, dirs);
  this.metaPath = this.contentPath + '.meta';
}

/////////////// CacheWriter ///////////////

// CacheWriter encapsulates the process of writing the contents and metadata for a single
// url. Its constructor accepts a finished callback which fires when the write is finished.
// Additionally, it accepts callbacks via its `wait` method, which are all fired after the
// finished callback has run.
function CacheWriter(url, contentPath, metaPath, finished) {
  this.piping = false;
  this.finished = finished;
  this._waiters = [];
  this.contentPath = contentPath;
  this.metaPath = metaPath;
  this.meta = {
    expiry: 0, // If no expiry is set, cache entries will always be overwritten.
    contentMD5: null,
    url: url
  };
}

// Set the expiration time in UTC seconds.
CacheWriter.prototype.setExpiry = function setExpiry(expiry) {
  this.meta.expiry = expiry;
};

CacheWriter.prototype.end = function end(readable) {
  if (this.piping) { return; }
  this.finished();
  this.finished = null;
  for (var i = 0; i < this._waiters.length; i++) {
    this._waiters[i]();
  }
};

CacheWriter.prototype.wait = function wait(cb) {
  this._waiters.push(cb);
};

CacheWriter.prototype.pipeFrom = function pipeFrom(readable) {
  this.piping = true;

  var _this = this;
  var hash = crypto.createHash('md5');

  readable.pause();
  readable.on('data', function(chunk) { hash.update(chunk); });

  var dir = pathlib.dirname(this.contentPath);
  mkdirp(dir, function(err) {
    if (err) {
      readable.resume();
      console.error("Couldn't create cache directory " + dir, err);
      return;
    }

    var contentStream = fs.createWriteStream(_this.contentPath);
    readable.pipe(contentStream);
    readable.resume();

    contentStream.on('error', function(err) {
      contentStream.removeAllListeners();
      console.error("Error writing cached content to " + this.contentPath);
      _this.piping = false;
      _this.end();
    });
    contentStream.on('finish', function() {
      _this.meta.contentMD5 = hash.digest('hex');
      var metaStr = JSON.stringify(_this.meta);
      fs.writeFile(_this.metaPath, metaStr, function(err) {
        if (err) {
          console.error("Couldn't write cache metafile " + _this.metaPath, err);
        } else {
          debug("Wrote cache entry: " + JSON.stringify(_this, null, '  '));
        }
        _this.piping = false;
        _this.end();
      });
    });
  });
};

/////////////// HTTPCache ///////////////

// HTTPCache handles HTTP requests, and caches them to disk if allowed by the Cache-Control
// header.

function HTTPCache(cacheRoot) {
  this.cacheRoot = cacheRoot;
  this._inFlightWrites = {};

  // What urls have been loaded by this instance of the cache? Those urls remain valid
  // even if they would otherwise expire.
  this._sessionCache = {};
}

// Reset must be called if you wish to reload any expired assets. (Otherwise, assets always
// persist for the lifetime of the cache, even if they would not otherwise be cached at all.)
HTTPCache.prototype.reset = function() {
  this._sessionCache = {};
};

function validateMetadata(metadata) {
  if (typeof metadata.expiry !== 'number') { debug('  expiry'); return false; }
  if (typeof metadata.contentMD5 !== 'string') { debug('  contentMD5'); return false; }
  if (typeof metadata.url !== 'string') { debug('  url'); return false; }
  return true;
}

HTTPCache.prototype._createContentReadStream = function(cacheEntry) {
  return fs.createReadStream(this._absPath(cacheEntry.contentPath));
};

var CACHE_STATE_NOTCACHED = 'notcached';
var CACHE_STATE_CACHED = 'cached';
var CACHE_STATE_ERROR = 'error';

HTTPCache.prototype._cachedThisSession = function(cacheEntry) {
  return this._sessionCache[cacheEntry.url];
};

// Checks whether the cache has a valid, unexpired copy of the contents for cacheEntry
// invokes callback(err, status) when finished, where status is one of:
//    'cached' - the cache entry is valid and non-expired.
//    'notcached' - the cache entry is missing, invalid, or expired, but ready to be cached anew.
//    'error' - the cache entry is corrupted, and could not be deleted. This indicates that
//              we shouldn't try to cache any responses right now.
HTTPCache.prototype._checkCache = function(cacheEntry, callback) {
  var _this = this;
  function loadMetadata(cb) {
    debug('loading metadata from', cacheEntry.metaPath);
    fs.readFile(
      _this._absPath(cacheEntry.metaPath),
      { encoding: 'utf8' },
      function(err, contentsJSON) {
        var contents;
        if (err) { return cb('invalid'); }
        try {
          contents = JSON.parse(contentsJSON);
        } catch (e) {
          debug('metadata was invalid', cacheEntry.metaPath, "'" + contentsJSON + "'");
          return cb('invalid');
        }
        if (!validateMetadata(contents)) {
          debug("invalid metadata", contents);
          return cb('invalid');
        }
        // TODO: It would be nice to refresh cache expiry by doing a HEAD request and checking
        // the etag. I'm not sure if that's technically allowable via max-age.
        if (!_this._cachedThisSession(cacheEntry) && (_this._now() > contents.expiry)) {
          debug("cache entry expired (" + _this._now() + " > " + contents.expiry + ")");
          return cb('invalid');
        }
        return cb(null, contents);
      }
    );
  }

  function deleteEntry(cb) {
    var deleteError = null;
    var barrier = async.barrier(2, function() {
      if (deleteError) {
        console.error(
          "Couldn't delete invalid cache entry.\n" +
          "Entry:\n" + JSON.stringify(cacheEntry) + "\n" +
          "Error: " + deleteError
        );
      }
      debug('deleted', _this._absPath(cacheEntry.contentPath));
      debug('deleted', _this._absPath(cacheEntry.metaPath));
      cb(deleteError);
    });
    function deleteCb(err) {
      if (err && err.code !== "ENOENT") { deleteError = err; }
      return barrier();
    }
    fs.unlink(_this._absPath(cacheEntry.contentPath), deleteCb);
    fs.unlink(_this._absPath(cacheEntry.metaPath), deleteCb);
  }

  function validateContents(metadata) {
    debug('validating contents', cacheEntry.contentPath);
    var hash = crypto.createHash('md5');
    var readStream = _this._createContentReadStream(cacheEntry);
    readStream.on('error', function(err) {
      console.error("Couldn't read from cache entry:", cacheEntry.contentPath, err);
      readStream.removeAllListeners();
      callback(null, CACHE_STATE_NOTCACHED);
    });
    readStream.on('data', function(chunk) {
      hash.update(chunk);
    });
    readStream.on('end', function() {
      var md5 = hash.digest('hex');
      if (md5 === metadata.contentMD5) {
        debug('verified md5 digest ' + md5 + ' for', cacheEntry.contentPath);
        return callback(null, CACHE_STATE_CACHED);
      } else {
        console.error(
          "Detected corrupted cache object: " + cacheEntry.contentPath + "\n" +
          "Expected md5 " + metadata.contentMD5 + " saw " + md5
        );
        return deleteEntry(function(err) {
          if (err) {
            // If the entry was invalid and we failed to delete it, the only choice is to load the
            // asset directly from http.
            return callback(null, CACHE_STATE_ERROR);
          }
          return callback(null, CACHE_STATE_NOTCACHED);
        });
      }
    });
  }

  // Execution starts here.
  loadMetadata(function(err, metadata) {
    if (err) {
      debug("attempting to delete cache entry due to missing/invalid metadata");
      return deleteEntry(function(err) {
        if (err) {
          // If the entry was invalid and we failed to delete it, the only choice is to load the
          // asset directly from http.
          return callback(null, CACHE_STATE_ERROR);
        }
        return callback(null, CACHE_STATE_NOTCACHED);
      });
    }

    // We now have valid metadata for an un-expired cache entry. Next, we checksum the contents.
    validateContents(metadata);
  });
};

HTTPCache.prototype._now = function() {
  return Date.now() / 1000;
};

HTTPCache.prototype._absPath = function(path) {
  return pathlib.join(this.cacheRoot, path);
};

HTTPCache.prototype._createCacheWriter = function(cacheEntry) {
  var contentPath = this._absPath(cacheEntry.contentPath);
  var metaPath = this._absPath(cacheEntry.metaPath);
  var inFlightWrites = this._inFlightWrites;

  if (inFlightWrites[cacheEntry.url]) {
    throw new Error("Attempt to call _createCacheWriter while a CacheWriter is already " +
                    "in flight for url " + cacheEntry.url);
  }
  var ret = new CacheWriter(cacheEntry.url, contentPath, metaPath, function() {
    delete inFlightWrites[cacheEntry.url];
  });
  inFlightWrites[cacheEntry.url] = ret;
  return ret;
};

function parseCacheControl(cacheControl) {
  var ret = {};
  if (typeof cacheControl !== 'string') {
    return ret;
  }
  var parts = cacheControl.split(/, */);
  for (var i = 0; i < parts.length; i++) {
    var strs = parts[i].split('=');
    if (strs.length === 1) {
      ret[strs[0]] = true;
    } else if (strs.length === 2) {
      ret[strs[0]] = strs[1];
    } else {
      console.error("Unparsable cache-control: '" + cacheControl + "' at '" + parts[i] + "'");
    }
  }
  return ret;
}

HTTPCache.prototype.openReadStream = function(url, cb) {
  var _this = this;
  var options;
  if (typeof url === 'object') {
    options = url;
    url = options.url;
  } else {
    options = { url: url };
  }

  debug("openReadStream", options.url);

  // If there is currently a cache-write in progress for this url, wait until it's finished
  // before starting the request.
  var inFlight = this._inFlightWrites[url];
  if (inFlight) {
    debug("Waiting for inflight write to " + url + " to complete...");
    inFlight.wait(function() {
      debug("... inflight write to " + url + " completed, starting fetch");
      _this.openReadStream(options, cb);
    });
    return;
  }

  var entry = new CacheEntry(url);

  // We might not end up writing to the cache at all, but we create a cache writer here, as it
  // also serves as a mux for identical concurrent requests.
  var cacheWriter = this._createCacheWriter(entry);

  // Check if the entry is available in the cache.
  this._checkCache(entry, function(err, cacheStatus) {

    debug("cache entry", entry.url, "status=", cacheStatus);
    if (cacheStatus === CACHE_STATE_CACHED) {
      // The cache contents are present and valid, so serve the request from cache.
      cacheWriter.end();
      return cb(null, _this._createContentReadStream(entry), _this._absPath(entry.contentPath));
    } else if (cacheStatus == CACHE_STATE_ERROR) {
      // Some kind of error occurred and we can't access the cache.
      return cb("Error: There was a problem with the asset cache and we can't write files");
    }

    var req = request.get({ url: options.url, headers: options.headers, followRedirect: true });

    // Handle HTTP request errors.
    req.on('error', function(err) {
      cacheWriter.end();
      cb(err);
      req.removeAllListeners();
      return;
    });

    // Handle the start of the HTTP response.
    function finish(err, res) {
      cacheWriter.wait(function() {
        if (err) {
          cb(err);
        } else {
          _this._sessionCache[entry.url] = 1;
          cb(null, _this._createContentReadStream(entry), _this._absPath(entry.contentPath));
        }
      });
      cacheWriter.end();
    }
    req.on('response', function(res) {
      if (res.statusCode !== 200) {
        return finish("URL " + url + " could not be fetched. status: " + res.statusCode);
      }

      // If there's no explicit cache control, the expiry will default to 0 and the
      // cache entry will always be overwritten.
      var cacheControl = parseCacheControl(res.headers['cache-control']);
      if (!cacheControl['no-store'] && cacheControl['max-age']) {
        var maxAgeNum = Number(cacheControl['max-age']);
        if (!isNaN(maxAgeNum)) {
          cacheWriter.setExpiry(_this._now() + maxAgeNum);
        }
      }

      // Write the file out and wait.
      cacheWriter.pipeFrom(res);
      return finish(null, res);
    });

    // Start the request.
    req.end();

  });
};

HTTPCache.prototype.getContentPathname = function(url, options) {
  var cacheEntry = new CacheEntry(url);
  if (options != null && options.absolute) {
    return pathlib.join(this.cacheRoot, cacheEntry.contentPath);
  } else {
    return cacheEntry.contentPath;
  }
};

HTTPCache.prototype.getContents = function(url, cb) {
  var options;
  if (typeof url === 'object') {
    options = url;
  } else {
    options = { url: url };
  }
  debug("getContents start", options.url);

  this.openReadStream(options, function(err, readStream, path) {
    if (err) { return cb(err); }

    var chunks = [];
    readStream.on('data', function(chunk) {
      chunks.push(chunk);
    });
    readStream.on('error', function(err) {
      debug("getContents error", options.url, err);
      readStream.removeAllListeners();
      cb(err);
    });
    readStream.on('end', function() {
      debug("getContents finish", options.url);
      var buf = Buffer.concat(chunks);
      if (options.encoding == 'ArrayBuffer') {
        // Provide the client with an ArrayBuffer instead of a node Buffer object.
        arr = new Uint8Array(buf.length);
        for (var i = 0; i < buf.length; i++) {
          arr[i] = buf[i];
        }
        cb(null, arr.buffer, path);
      } else if (options.encoding) {
        cb(null, buf.toString(options.encoding), path);
      } else {
        cb(null, buf, path);
      }
    });

  });
};

exports.HTTPCache = HTTPCache;
