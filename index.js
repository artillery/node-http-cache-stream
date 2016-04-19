/**
 * Copyright (c) 2015 Artillery Games, Inc. All rights reserved.
 *
 * This source code is licensed under the MIT-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

var async = require('artillery-async');
var crypto = require('crypto');
var fs = require('fs');
var http = require('http');
var mkdirp = require('mkdirp');
var pathlib = require('path');
var request = require('request');
var urllib = require('url');
var debug = require('debug')('http-disk-cache');
var glob = require('glob');

/////////////// CacheEntry ///////////////

function canonicalUrl(url) {
  var parsedUrl = urllib.parse(url);
  delete parsedUrl.hash;
  delete parsedUrl.href;
  return urllib.format(parsedUrl);
}

// CacheEntry maps a url to relative pathnames for that URLs contents and metadata files.
function CacheEntry(url, etagFormat) {
  url = canonicalUrl(url);
  this.url = url;
  this.etagFormat = etagFormat;

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
function CacheWriter(url, etagFormat, contentPath, metaPath, finished) {
  if (etagFormat == null) { etagFormat = null; } // can't JSONify undefined.
  this.piping = false;
  this.finished = finished;
  this._waiters = [];
  this.contentPath = contentPath;
  this.metaPath = metaPath;
  this.err = null;
  this.meta = {
    expiry: 0, // If no expiry is set, cache entries will always be overwritten.
    contentMD5: null,
    url: url,
    etagFormat: etagFormat,
    etag: null
  };
}

// Set the expiration time in UTC seconds.
CacheWriter.prototype.setExpiry = function setExpiry(expiry) {
  this.meta.expiry = expiry;
};

CacheWriter.prototype.setEtag = function setEtag(etag) {
  debug('setting etag', etag);
  this.meta.etag = etag;
};

// If an etagFormat is specified, but the fetched URL has no etag header, we record
// that there was no etag present. The fetched contents are returned to the application,
// but the cached entry will never be considered valid, as we don't check noEtagPresent
// in checkCache.
CacheWriter.prototype.setNoEtagPresent = function setNoEtagPresent() {
  debug('no etag present');
  this.meta.noEtagPresent = true;
};

CacheWriter.prototype.validateEtag = function validateEtag() {
  var meta = this.meta;
  if (meta.etagFormat == null) { return true; }
  if (meta.noEtagPresent) { return true; }
  if (meta.etagFormat === 'md5') {
    if (meta.etag === meta.contentMD5) { return true; }
    else { debug('validateEtag failed:', meta.etag, 'vs', meta.contentMD5); return false; }
  } else {
    console.error('invalid etagFormat:', meta.etagFormat);
    return false;
  }
};

CacheWriter.prototype.end = function end(readable) {
  if (this.piping) { return; }
  this.finished(this.err);
  this.finished = null;
  for (var i = 0; i < this._waiters.length; i++) {
    this._waiters[i](this.err);
  }
};

CacheWriter.prototype.wait = function wait(cb) {
  this._waiters.push(cb);
};

CacheWriter.prototype.writeSync = function writeSync(contents) {
  var hash = crypto.createHash('md5');
  hash.update(contents);
  this.meta.contentMD5 = hash.digest('hex');
  var dir = pathlib.dirname(this.contentPath);
  mkdirp.sync(dir);
  fs.writeFileSync(this.contentPath, contents);
  debug("Wrote cache entry: " + JSON.stringify(this, null, '  '));
  if (!this.validateEtag()) {
    this.err = 'Failed to validate etag';
    return;
  }
  var metaStr = JSON.stringify(this.meta);
  fs.writeFileSync(this.metaPath, metaStr);
  this.end();
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
      _this.err = err;
      _this.piping = false;
      _this.end();
    });
    contentStream.on('finish', function() {
      _this.meta.contentMD5 = hash.digest('hex');
      if (!_this.validateEtag()) {
        _this.err = 'Failed to validate etag';
        _this.piping = false;
        _this.end();
        return;
      }
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
  this._inFlightRequests = [];

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

function deleteEntry(metaPath, cb) {
  var contentPath = metaPath.slice(0, -5);
  var deleteError = null;
  var barrier = async.barrier(2, function() {
    if (deleteError) {
      console.error(
        "Couldn't delete invalid cache files.\n" +
        "Files:\n" + contentPath + " " + metaPath  + "\n" +
        "Error: " + deleteError
      );
    }
    debug('deleted', contentPath);
    debug('deleted', metaPath);
    cb(deleteError);
  });
  function deleteCb(err) {
    if (err && err.code !== "ENOENT") { deleteError = err; }
    return barrier();
  }
  fs.unlink(contentPath, deleteCb);
  fs.unlink(metaPath, deleteCb);
}

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
        if (cacheEntry.etagFormat != null) {
          // If the etag format changes, the contents are invalidated.
          if (cacheEntry.etagFormat !== contents.etagFormat) {
            debug("entry had wrong etagFormat", cacheEntry.etagFormat, 'vs', contents.etagFormat);
            return cb('invalid');
          }
          // This just compares the two checksums recorded in the file. The actual contents are
          // re-checksummed and compared against contentMD5 later.
          if (contents.etagFormat === 'md5' && contents.etag !== contents.contentMD5) {
            debug("contentMD5 does not match etag", contents);
            return cb('invalid');
          }
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
        return deleteEntry(_this._absPath(cacheEntry.metaPath), function(err) {
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
      return deleteEntry(_this._absPath(cacheEntry.metaPath), function(err) {
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
  var ret = new CacheWriter(cacheEntry.url, cacheEntry.etagFormat, contentPath, metaPath,
    function() {
      delete inFlightWrites[cacheEntry.url];
    }
  );
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

HTTPCache.prototype.assertCached = function(url, onProgress, cb) {
  if (cb == null) {
    cb = onProgress;
    onProgress = null;
  }

  var _this = this;
  var options;
  if (typeof url === 'object') {
    options = url;
    url = options.url;
  } else {
    options = { url: url };
  }

  options._skipReadStream = true;

  var entry = new CacheEntry(url, options.etagFormat);

  this._checkCache(entry, function(err, cacheStatus) {
    if (cacheStatus === CACHE_STATE_CACHED) {
      debug('assert cache hit', url);
      return cb();
    } else {
      debug('assert cache miss', url);
      _this.openReadStream(options, onProgress, function(err, _, path) {
        cb(err);
      });
    }
  });
};

HTTPCache.prototype.abortAllInFlightRequests = function() {
  for (var i = 0; i < this._inFlightRequests.length; i++) {
    this._inFlightRequests[i].abort();
  }
};

HTTPCache.prototype.openReadStream = function(url, onProgress, cb) {
  if (cb == null) {
    cb = onProgress;
    onProgress = null;
  }

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
    inFlight.wait(function(cacheWriterErr) {
      if (cacheWriterErr != null) { return cb(cacheWriterErr); }
      debug("... inflight write to " + url + " completed, starting fetch");
      _this.openReadStream(options, cb);
    });
    return;
  }

  var entry = new CacheEntry(url, options.etagFormat);

  // We might not end up writing to the cache at all, but we create a cache writer here, as it
  // also serves as a mux for identical concurrent requests.
  var cacheWriter = this._createCacheWriter(entry);

  // Check if the entry is available in the cache.
  this._checkCache(entry, function(err, cacheStatus) {

    debug("cache entry", entry.url, "status=", cacheStatus);
    if (cacheStatus === CACHE_STATE_CACHED) {
      // The cache contents are present and valid, so serve the request from cache.
      cacheWriter.end();
      var readStream = options._skipReadStream ? null : _this._createContentReadStream(entry);
      return cb(null, readStream, _this._absPath(entry.contentPath));
    } else if (cacheStatus == CACHE_STATE_ERROR) {
      // Some kind of error occurred and we can't access the cache.
      return cb("Error: There was a problem with the asset cache and we can't write files");
    }

    var req = request.get({ url: options.url, headers: options.headers, followRedirect: true });

    _this._inFlightRequests.push(req);
    var removeFromInFlightRequests = function() {
      var index = _this._inFlightRequests.indexOf(req);
      if (index >= 0) {
        _this._inFlightRequests.splice(index, 1);
      }
    };

    // Handle HTTP request errors.
    req.on('error', function reqOnError(err) {
      cacheWriter.end();
      removeFromInFlightRequests();
      cb(err);
      req.removeAllListeners();
      return;
    });

    req.on('abort', function reqOnAbort() {
      cacheWriter.end();
      removeFromInFlightRequests();
      cb("Request was aborted");
      req.removeAllListeners();
      return;
    });

    // Handle the start of the HTTP response.
    function finish(err, res) {
      cacheWriter.wait(function(cacheWriterErr) {
        if (err) {
          cb(err);
        } else if (cacheWriterErr) {
          cb(cacheWriterErr);
        } else {
          _this._sessionCache[entry.url] = 1;
          var readStream = options._skipReadStream ? null : _this._createContentReadStream(entry);
          cb(null, readStream, _this._absPath(entry.contentPath));
        }
      });
      cacheWriter.end();
    }
    req.on('response', function(res) {
      removeFromInFlightRequests();

      if (res.statusCode !== 200) {
        return finish("URL " + url + " could not be fetched. status: " + res.statusCode);
      }
      if (options.etagFormat === 'md5') {
        var etag = res.headers['etag'];
        debug('setting etag', etag);
        if (typeof etag === 'string') { etag = etag.replace(/['"]/g, ''); }
        if (etag == null) {
          cacheWriter.setNoEtagPresent();
        } else {
          cacheWriter.setEtag(etag);
        }
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

    // Handle progress updates.
    req.on('data', function(data) {
      if (typeof onProgress === 'function') {
        onProgress(data.length);
      }
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

// Synchronously write a file into the cache. url must start with `cache:`.
// Throws on error.
HTTPCache.prototype.setContentsSync = function(url, contents) {
  var options;
  if (typeof url === 'object') {
    options = url;
    url = options.url;
  } else {
    options = { url: url };
  }

  if (!/^cache:/.test(url)) {
    throw new Error("setContentsSync can only be called on cache:// URLs");
  }
  var entry = new CacheEntry(url, options.etagFormat);

  var cacheWriter = this._createCacheWriter(entry);
  cacheWriter.setExpiry(this._now() + (options.maxAgeNum || 0));
  cacheWriter.setEtag(options.etag);
  cacheWriter.writeSync(contents);
  this._sessionCache[entry.url] = 1;
};

// Synchronously read a url from the cache. Returns `null` if the url is expired
// or not cached.
// Throws on error.
HTTPCache.prototype.getContentsSync = function(url) {
  var options;
  if (typeof url === 'object') {
    options = url;
    url = options.url;
  } else {
    options = { url: url };
  }

  var entry = new CacheEntry(url, options.etagFormat);
  var metaContents = fs.readFileSync(this._absPath(entry.metaPath), 'utf8');
  var meta;
  try {
    meta = JSON.parse(metaContents);
  } catch (e) {
    debug('metadata was invalid', entry.metaPath, "'" + metaContents + "'");
    return null;
  }

  if (!validateMetadata(meta)) {
    debug("invalid metadata", meta);
    return null;
  }
  if (entry.etagFormat != null) {
    // If the etag format changes, the contents are invalidated.
    if (entry.etagFormat !== meta.etagFormat) {
      debug("entry had wrong etagFormat", entry.etagFormat, 'vs', meta.etagFormat);
      return null;
    }
    // This just compares the two checksums recorded in the file. The actual metaContents are
    // re-checksummed and compared against contentMD5 later.
    if (meta.etagFormat === 'md5' && meta.etag !== meta.contentMD5) {
      debug("contentMD5 does not match etag", meta);
      return null;
    }
  }

  if (!this._cachedThisSession(entry) && (this._now() > meta.expiry)) {
    debug("Contents expired");
    return null;
  }

  var contents = fs.readFileSync(this._absPath(entry.contentPath), options.encoding);
  var hash = crypto.createHash('md5');
  hash.update(contents);
  var md5 = hash.digest('hex');
  if (md5 === meta.contentMD5) {
    debug('verified md5 digest ' + md5 + ' for', entry.contentPath);
    return contents;
  } else {
    console.error(
      "Detected corrupted cache object: " + entry.contentPath + "\n" +
      "Expected md5 " + meta.contentMD5 + " saw " + md5
    );
    return null;
  }
};

var RepairResult = {
  // We removed a bad file from the cache.
  REPAIR_CLEANED: 'REPAIR_CLEANED',
  // We found a bad file but couldn't remove it.
  REPAIR_FAILED: 'REPAIR_FAILED',
};

exports.RepairResult = RepairResult;

// Scans the entire cache, checksums every file, attempts to remove any corrupted or expired
// files.
//
// The callback receives a boolean indicating whether all detected cache corruptions were
// repaied, and a list of tuples of [RepairResults, info] describing what actions the
// repair process took.
HTTPCache.prototype.repair = function(cb) {
  var globPath = pathlib.join(this.cacheRoot, "**/*.meta");

  var success = true;
  var results = [];

  var _this = this;
  glob(globPath, function (err, files) {
    if (err != null) { return cb(err); }

    function checkFile(metaPath, nextCb) {
      function deleteCb(deleteError) {
        if (deleteError != null) {
          results.push([RepairResult.REPAIR_FAILED,
                        "Couldn't remove cache entry: " + deleteError]);
          success = false;
        } else {
          results.push([RepairResult.REPAIR_CLEANED,
                        "Removed cache entry: " + metaPath]);
        }
        nextCb();
      }
      fs.readFile(metaPath, { encoding: 'utf8' }, function(err, metaContents) {
        try {
          var meta = JSON.parse(metaContents);
        } catch (e) {
          deleteEntry(metaPath, deleteCb);
          return;
        }

        if (meta.url == null) {
          deleteEntry(metaPath, deleteCb);
          return;
        }

        var entry = new CacheEntry(meta.url);
        if (_this._absPath(entry.metaPath) != metaPath) {
          deleteEntry(metaPath, deleteCb);
          return;
        }

        _this._checkCache(entry, function (err, status) {
          if (err != null) {
            deleteEntry(metaPath, deleteCb);
            return;
          }
          if (status == CACHE_STATE_ERROR) {
            deleteEntry(metaPath, deleteCb);
            return;
          }
          if (status == CACHE_STATE_NOTCACHED) {
            results.push([RepairResult.REPAIR_CLEANED, "Removed cache entry: " + metaPath]);
          }
          nextCb()
        });
      });
    }

    async.forEachSeries(files, checkFile, function(err) {
      cb(null, success, results);
    });
  });
};

exports.HTTPCache = HTTPCache;
