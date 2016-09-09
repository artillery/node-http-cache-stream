/**
 * Copyright (c) 2015 Artillery Games, Inc. All rights reserved.
 *
 * This source code is licensed under the MIT-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

var crypto = require('crypto');
var http = require('http');
var fs = require('fs');
var temp = require('temp');
var execSync = require('child_process').execSync;
var debug = require('debug')('http-disk-cache');
var async = require('artillery-async');
var glob = require('glob');
var stream = require('stream');

var httpcache = require('./index');

process.on('uncaughtException', function (err) {
    console.error(err.stack);
    process.exit(1);
});

function newUrlReply(contents, status, headers, defer) {
  if (!headers) {
    headers = {};
  }
  return {
    contents: contents,
    status: status,
    headers: headers,
    defer: defer || false,
    fetchCount: 0
  };
}

function catStream(stream, cb) {
  chunks = [];
  stream.on('error', function (err) {
    cb(err);
  });
  stream.on('data', function (chunk) {
    chunks.push(chunk);
  });
  stream.on('end', function () {
    if (chunks.length === 0) {
      cb(null, null);
    } else if (typeof chunks[0] === 'string') {
      cb(null, chunks.join(''));
    } else { // Buffer
      cb(null, Buffer.concat(chunks));
    }
  });
}

function md5hash(str) {
  var hash = crypto.createHash('md5');
  hash.update(str);
  return hash.digest('hex');
}

exports.tests = {
  setUp: function (cb) {
    var _this = this;
    temp.mkdir('cache', function(err, path) {
      if (err) {
        throw new Error("Error creating temp dir", err);
      }
      debug("cache root: ", path);

      _this.requests = [];
      _this.nowSeconds = 1000;
      _this.cache = new httpcache.HTTPCache(path);
      _this.cache._now = function() {
        return _this.nowSeconds;
      };

      var url5contents = 'url5 contents';
      var url6contents = 'url6 contents';
      var url7contents = 'url7 contents';

      _this.serverUrls = {
        '/url1': newUrlReply('url1 contents', 200, { 'Cache-Control': 'max-age=200' }),
        '/url2': newUrlReply('url2 contents', 200, { 'Cache-Control': 'no-cache' }),
        '/url3': newUrlReply('url3 contents', 200, {}),
        '/url4': newUrlReply('url4 contents', 200, { 'Cache-Control': 'max-age=200=unparseable' }),
        '/url5': newUrlReply(url5contents, 200, {
          'Cache-Control': 'max-age=200',
          'etag': md5hash(url5contents)
        }),
        '/url6': newUrlReply(url6contents, 200, {
          'Cache-Control': 'max-age=200',
          'etag': md5hash(url6contents + 'make the hash invalid')
        }),
        '/url7': newUrlReply(url7contents, 200, {
          'Cache-Control': 'max-age=200',
          'etag': '"' + md5hash(url7contents) + '"'
        }),
        '/defer': newUrlReply('deferred contents', 200, {}, true),
      };

      _this.deferredReplies = [];
      _this.doNextDefferedReply = function() {
        var fn = _this.deferredReplies.shift();
        if (typeof fn === 'function') {
          setTimeout(fn, 0);
        }
      };

      _this.createUrl = function(urlPath) {
        var addr = _this.server.address();
        return 'http://' + addr.address + ':' + addr.port + urlPath;
      };

      _this.server = http.createServer();
      _this.server.on('request', function(request, response) {
        var reply = _this.serverUrls[request.url];
        if (!reply) {
          throw new Error("Unknown URL " + request.url);
        }
        reply.fetchCount++;
        _this.requests.push(request.url);
        response.statusCode = reply.status;
        for (var header in reply.headers) {
          response.setHeader(header, reply.headers[header]);
        }
        _this.deferredReplies.push(function() {
          response.write(reply.contents);
          response.end();
        });
        if (!reply.defer) {
          _this.doNextDefferedReply();
        } else {
        }
      });
      _this.server.listen(0, '127.0.0.1');
      _this.server.on('socket', function(socket) {
        _this.port = socket.localPort;
      });
      _this.server.on('listening', function() {
        cb();
      });
    });
  },

  tearDown: function (cb) {
    this.server.close(cb);
  },

  testConnectionRefused: function(test) {
    test.expect(1);
    this.cache.openReadStream('http://127.0.0.1:1/url1', function(err, stream, path) {
      test.equal(err.code, 'ECONNREFUSED');
      test.done();
    });
  },

  test404: function(test) {
    test.expect(1);
    this.serverUrls['/404'] = newUrlReply('404', 404, { 'Cache-Control': 'max-age=200' });
    this.cache.openReadStream(this.createUrl('/404'), function(err, stream, path) {
      test.ok(/could not be fetched.*404/.test(err));
      test.done();
    });
  },

  testBasicUrl: function(test) {
    test.expect(2);
    var _this = this;
    this.cache.openReadStream(this.createUrl('/url1'), function(err, stream, path) {
      test.ok(stream instanceof fs.ReadStream, "stream should be an fs.ReadStream");
      catStream(stream, function (err, contents) {
        test.equal(contents.toString('utf8'), 'url1 contents');
        test.done();
      });
    });
  },

  testBasicUrlEtag: function(test) {
    test.expect(4);
    var _this = this;
    async.series([
      function (cb) {
        _this.cache.openReadStream({ url: _this.createUrl('/url5'), etagFormat: 'md5' }, function(err, stream, path) {
          test.equal(_this.requests.length, 1);
          test.equal(_this.requests[0], '/url5');
          test.ok(stream instanceof fs.ReadStream, "stream should be an fs.ReadStream");
          catStream(stream, function (err, contents) {
            test.equal(contents.toString('utf8'), 'url5 contents');
            test.done();
          });
        });
      },
      function (cb) {
        _this.cache.openReadStream({ url: _this.createUrl('/url5'), etagFormat: 'md5' }, function(err, stream, path) {
          test.equal(_this.requests.length, 1); // request is handled from cache.
          test.ok(stream instanceof fs.ReadStream, "stream should be an fs.ReadStream");
          catStream(stream, function (err, contents) {
            test.equal(contents.toString('utf8'), 'url5 contents');
            test.done();
          });
        });
      }
    ], function (err) {
      if (err != null) { test.fail(err); }
      test.done();
    });
  },

  testQuotedEtag: function(test) {
    test.expect(4);
    var _this = this;
    async.series([
      function (cb) {
        _this.cache.openReadStream({ url: _this.createUrl('/url7'), etagFormat: 'md5' }, function(err, stream, path) {
          test.equal(_this.requests.length, 1);
          test.equal(_this.requests[0], '/url7');
          test.ok(stream instanceof fs.ReadStream, "stream should be an fs.ReadStream");
          catStream(stream, function (err, contents) {
            test.equal(contents.toString('utf8'), 'url7 contents');
            test.done();
          });
        });
      },
      function (cb) {
        _this.cache.openReadStream({ url: _this.createUrl('/url7'), etagFormat: 'md5' }, function(err, stream, path) {
          test.equal(_this.requests.length, 1); // request is handled from cache.
          test.ok(stream instanceof fs.ReadStream, "stream should be an fs.ReadStream");
          catStream(stream, function (err, contents) {
            test.equal(contents.toString('utf8'), 'url7 contents');
            test.done();
          });
        });
      }
    ], function (err) {
      if (err != null) { test.fail(err); }
      test.done();
    });
  },

  // url6 simply lies about its own content md5 sum, which is an easier way to test the effects of a
  // truncated download.
  testBadEtag: function(test) {
    test.expect(2);
    var _this = this;
    this.cache.openReadStream({ url: this.createUrl('/url6'), etagFormat: 'md5' }, function(err, stream, path) {
      test.equal(err, 'Failed to validate etag');
      test.equal(stream, null);
      test.done();
    });
  },

  // Specify an etagFormat when fetching a url that does not have an etag.
  testEtagFormatNoEtag: function(test) {
    var _this = this;
    async.series([
      function (cb) {
        // Verify that we can fetch a url that doesn't have an etag even when etagFormat is specified.
        _this.cache.openReadStream({ url: _this.createUrl('/url1'), etagFormat: 'md5' }, function(err, stream, path) {
          test.equal(_this.requests.length, 1);
          test.equal(_this.requests[0], '/url1');
          catStream(stream, function (err, contents) {
            test.equal(contents.toString('utf8'), 'url1 contents');
            cb();
          });
        });
      },

      function (cb) {
        // Verify that the url is not served from cache if we fetch it again.
        _this.cache.openReadStream({ url: _this.createUrl('/url1'), etagFormat: 'md5' }, function(err, stream, path) {
          test.equal(_this.requests.length, 2);
          test.equal(_this.requests[1], '/url1');
          catStream(stream, function (err, contents) {
            test.equal(contents.toString('utf8'), 'url1 contents');
            cb();
          });
        });
      },


    ], function (err) {
      if (err != null) { test.fail(err); }
      test.done();
    });
  },

  testEtagFormatChange: function(test) {
    var _this = this;
    var initialPath = null;
    async.series([
      // fetch without an etagFormat
      function(cb) {
        _this.cache.openReadStream({ url: _this.createUrl('/url5') }, function(err, stream, path) {
          test.equal(_this.requests.length, 1);
          test.equal(_this.requests[0], '/url5'); // url5 was fetched once.
          initialPath = path;
          cb(err);
        });
      },
      function(cb) {
        _this.cache.openReadStream({ url: _this.createUrl('/url5') }, function(err, stream, path) {
          // no additional fetches occurred.
          test.equal(_this.requests.length, 1);
          test.equal(_this.requests[0], '/url5');
          initialPath = path;
          cb(err);
        });
      },
      function(cb) {
        _this.cache.openReadStream({ url: _this.createUrl('/url5'), etagFormat: 'md5' },
          function(err, stream, path) {
            // Because we specified an etag format, the url was re-fetched so that the etag
            // could be verified.
            test.equal(_this.requests.length, 2);
            test.equal(_this.requests[1], '/url5');
            initialPath = path;
            cb(err);
          }
        );
      }
    ], function(err) {
      if (err != null) { test.fail(err); }
      test.done();
    });
  },

  testEtagFormatChangeAssertCached: function(test) {
    var _this = this;
    var initialPath = null;
    async.series([
      // fetch without an etagFormat
      function(cb) {
        _this.cache.assertCached({ url: _this.createUrl('/url5') }, function(err) {
          test.equal(_this.requests.length, 1);
          test.equal(_this.requests[0], '/url5'); // url5 was fetched once.
          cb(err);
        });
      },
      function(cb) {
        _this.cache.assertCached({ url: _this.createUrl('/url5') }, function(err) {
          // no additional fetches occurred.
          test.equal(_this.requests.length, 1);
          test.equal(_this.requests[0], '/url5');
          cb(err);
        });
      },
      function(cb) {
        _this.cache.assertCached({ url: _this.createUrl('/url5'), etagFormat: 'md5' }, function(err) {
          // Because we specified an etag format, the url was re-fetched so that the etag
          // could be verified.
          test.equal(_this.requests.length, 2);
          test.equal(_this.requests[1], '/url5');
          cb(err);
        });
      }
    ], function(err) {
      if (err != null) { test.fail(err); }
      test.done();
    });
  },

  testConcurrentRequests: function(test) {
    test.expect(2);
    var _this = this;
    var count = 2;

    barrier = function() {
      count--;
      if (count === 0) { test.done(); }
    };
    var cb = function(err, stream, path) {
      catStream(stream, function (err, contents) {
        test.equal(contents.toString('utf8'), 'url1 contents');
        barrier();
      });
    };
    this.cache.openReadStream(this.createUrl('/url1'), cb);
    this.cache.openReadStream(this.createUrl('/url1'), cb);
  },


  testBasicCaching: function(test) {
    test.expect(6);
    doTest(this, test, '/url1', 'url1 contents', false, true, 0, test.done);
  },

  testExplicitNoCache: function(test) {
    test.expect(6);
    doTest(this, test, '/url2', 'url2 contents', false, false, 0, test.done);
  },

  testUnparseableCacheControl: function(test) {
    test.expect(6);
    doTest(this, test, '/url4', 'url4 contents', false, false, 0, test.done);
  },

  testNoCache: function(test) {
    // URLs without a Cache-Control header don't get cached.
    test.expect(6);
    doTest(this, test, '/url3', 'url3 contents', false, false, 0, test.done);
  },

  testUnexpiredCache: function(test) {
    test.expect(6);
    // 200 is the maximum allowable age.
    doTest(this, test, '/url1', 'url1 contents', false, true, 200, test.done);
  },

  testExpiredCache: function(test) {
    test.expect(6);
    doTest(this, test, '/url1', 'url1 contents', false, false, 201, test.done);
  },

  testLotsOfFetching: function(test) {
    for (var i = 0; i < 20; i++) {
      var autourl = '/autourl' + i;
      var maxage = 7 * i;
      this.serverUrls[autourl] =
        newUrlReply(autourl + ' contents', 200, { 'Cache-Control': 'max-age=' + maxage });
    }

    var _this = this;

    var keys = Object.keys(this.serverUrls);
    var urls = [];
    for (i = 0; i < keys.length; i++) {
      if (!this.serverUrls[keys[i]].defer) {
        urls.push(keys[i]);
      }
    };

    var count = 300;
    function pickUrl() {
      return urls[count % urls.length];
    }
    function fetchOnce() {
      var url = pickUrl();
      debug('FETCHING', url);
      _this.cache.openReadStream(_this.createUrl(url), function(err, stream) {
        if (err) {
          test.fail();
          test.done();
        } else if (count-- > 0) {
          _this.nowSeconds += 30;
          process.nextTick(fetchOnce);
        } else {
          test.done();
        }
      });
    }
    fetchOnce();
  },

  testCantDeleteInvalidEntry: function(test) {
    var _this = this;
    function chmodCache() {
      var cmd = "chmod -R 500 " + _this.cache.cacheRoot;
      execSync(cmd);

      // Wait till the cached content has expired
      _this.nowSeconds += 500;

      _this.serverUrls['/url1'].fetchCount = 0;
      // Even though the content is cacheable, we now only get it directly from http because
      // the cache has been corrupted.
      doTest(_this, test, '/url1', 'url1 contents', false, false, 0, test.done);
    }
    doTest(this, test, '/url1', 'url1 contents', false, true, 0, chmodCache);
  },

  testCorruptedMeta: function(test) {
    var _this = this;
    function chmodCache() {
      var cmd = "find " + _this.cache.cacheRoot + " -name '*.meta' |" +
                " xargs -I {} bash -c 'echo foo >> {}'";
      execSync(cmd);

      _this.serverUrls['/url1'].fetchCount = 0;
      // The meta file is corrupted, so the first fetch goes to the net.
      doTest(_this, test, '/url1', 'url1 contents', false, true, 0, test.done);
    }
    doTest(this, test, '/url1', 'url1 contents', false, true, 0, chmodCache);
  },

  testCorruptedContents: function(test) {
    var _this = this;
    function chmodCache() {
      var cmd = "find " + _this.cache.cacheRoot + " -type f \\! -name '*.meta' | " +
                "xargs -I {} bash -c 'echo foo >> {}'";
      execSync(cmd);

      _this.serverUrls['/url1'].fetchCount = 0;
      // The contents file is corrupted, so the first fetch goes to the net.
      doTest(_this, test, '/url1', 'url1 contents', false, true, 0, test.done);
    }
    doTest(this, test, '/url1', 'url1 contents', false, true, 0, chmodCache);
  },

  testAssertCachedAlreadyInCache: function(test) {
    var _this = this;
    var url = this.createUrl('/url1');
    test.ok(!fs.existsSync(_this.cache.getContentPathname(url, {absolute: true})));
    doTest(this, test, '/url1', 'url1 contents', false, true, 0, function() {
      test.ok(fs.existsSync(_this.cache.getContentPathname(url, {absolute: true})));
      _this.cache.assertCached(url, function(err) {
        test.equal(err, null);
        test.ok(fs.existsSync(_this.cache.getContentPathname(url, {absolute: true})));
        test.done();
      });
    });
  },

  testAssertCachedNotInCache: function(test) {
    var _this = this;
    var url = _this.createUrl('/url1');
    test.ok(!fs.existsSync(_this.cache.getContentPathname(url, {absolute: true})));
    this.cache.assertCached(url, function(err) {
      test.equal(err, null);
      test.ok(fs.existsSync(_this.cache.getContentPathname(url, {absolute: true})));
      test.done();
    });
  },

  testRepair: function(test) {
    var _this = this;
    var cachePath = null;
    async.series([
      // Create a cache entry
      function(cb) {
        _this.cache.getContents(_this.createUrl('/url1'), function (err, buf, path) {
          cachePath = path;
          cb(err);
        });
      },
      // Corrupt it.
      function(cb) {
        fs.appendFile(cachePath, 'extra data', cb);
      },
      function(cb) {
        _this.cache.repair(function(err, allRepaired, results) {
          test.ok(allRepaired);
          test.equal(results.length, 1);
          test.equal(results[0][0], httpcache.RepairResult.REPAIR_CLEANED);
          test.ok(!fs.existsSync(cachePath));
          cb();
        });
      },

      // Create it again.
      function(cb) {
        _this.cache.getContents(_this.createUrl('/url1'), function (err, buf, path) {
          cachePath = path;
          cb(err);
        });
      },
      // Corrupt it.
      function(cb) {
        fs.appendFile(cachePath, 'extra data', cb);
      },
      // Make it undeletable.
      function(cb) {
        var cmd = "chmod -R 500 " + _this.cache.cacheRoot;
        execSync(cmd);
        cb();
      },
      function(cb) {
        _this.cache.repair(function(err, allRepaired, results) {
          test.ok(!allRepaired);
          test.equal(results.length, 1);
          test.equal(results[0][0], httpcache.RepairResult.REPAIR_FAILED);
          test.ok(fs.existsSync(cachePath));
          cb();
        });
      }
    ], function (err) {
      if (err != null) { test.fail("Failed: " + err); }
      test.done();
    });
  },

  testAbortAllInFlightRequests: function(test) {
    var _this = this;
    _this.cache.assertCached({ url: _this.createUrl('/defer') }, function(err) {
      test.equal(err, "Request was aborted");
      test.done();
    });
    // Waiting 500ms is kind of gross, but assertCached() calls openReadStream(), which calls
    // _checkCache(), which calls fs.readFile asynchronously. It's just easier to assume for testing
    // that _checkCache takes <500ms so we can schedule abortAllInFlightRequests() to occur after
    // the request has been added to _inFlightRequests.
    setTimeout(function() {
      _this.cache.abortAllInFlightRequests();
      setTimeout(function() {
        _this.doNextDefferedReply();
      }, 0);
    }, 500);
  },

  testSetAndGetContentsSync: function (test) {
    var contents = "some contents";
    this.cache.setContentsSync({
      url: 'cache://hay',
      etagFormat: 'md5',
      etag: md5hash(contents),
      maxAgeNum: 1000
    }, contents);

    var cachedContents = this.cache.getContentsSync({ url: 'cache://hay' });
    test.equal(cachedContents, contents);

    this.cache.reset();

    cachedContents = this.cache.getContentsSync({ url: 'cache://hay' });
    test.equal(cachedContents, contents);

    this.nowSeconds += 1100;
    this.cache.reset();

    cachedContents = this.cache.getContentsSync({ url: 'cache://hay' });
    test.equal(cachedContents, null);

    test.done();
  },

  testSetAndGetContentsZeroExpiry: function (test) {
    var contents = "some contents";
    this.cache.setContentsSync({
      url: 'cache://hay',
      etagFormat: 'md5',
      etag: md5hash(contents),
      maxAgeNum: 0
    }, contents);

    this.nowSeconds += 10;

    var cachedContents = this.cache.getContentsSync({ url: 'cache://hay' });
    test.equal(cachedContents, contents);

    this.cache.reset();

    cachedContents = this.cache.getContentsSync({ url: 'cache://hay' });
    test.equal(cachedContents, null);

    test.done();
  },

  testCacheCleaning: function (test) {
    test.expect(1);
    var _this = this;
    async.series([
      // populate the cache.
      function(cb) {
        async.forEachSeries(
          [1, 2, 3, 4],
          function (num, nextCb) {
            _this.cache.openReadStream(_this.createUrl('/url' + num), function(err, stream, path) {
              nextCb(err);
            });
          },
          function (err) {
            cb(err)
          }
        );
      },
      // clean the cache
      function (cb) {
        var iter = 0;
        _this.cache.clean(function(curFile, totalFiles, metadata, stat, resultCb) {
          iter++;
          if (iter > 2) {
            return resultCb('REMOVE');
          } else {
            return resultCb('KEEP');
          }
        }, function() {
          glob.glob("**/*.meta", { cwd: _this.cache.cacheRoot }, function (err, files) {
            // We created 4 files originally - only 2 should remain.
            test.equal(files.length, 2);
            cb();
          });
        });
      }
    ], function (err) {
      if (err) { test.fail(err) }
      test.done();
    });
  }
};

// Load the url twice, advancing the time by deltaT in between the two fetches.
// Verify that the url is cached or not cached as expected at each fetch.
function doTest(_this, test, url, contents, firstCached, secondCached, deltaT, cb) {
  var count = 0;
  if (!deltaT) { deltaT = 0; }

  _this.cache.getContents(_this.createUrl(url), function(err, buffer, path) {
    if (!firstCached) { count++; }
    if (!buffer) {
      test.ok(err, "if buffer is null there had better be an error");
      return cb();
    }
    test.ok(fs.existsSync(path));

    test.equal(buffer.toString('utf8'), contents);
    test.equal(_this.serverUrls[url].fetchCount, count);
    _this.nowSeconds += deltaT;
    _this.cache.reset();

    _this.cache.getContents(_this.createUrl(url), function(err, buffer, path) {
      if (!secondCached) { count++; }

      test.ok(fs.existsSync(path));
      test.equal(_this.serverUrls[url].fetchCount, count);
      test.equal(buffer.toString('utf8'), contents);
      cb();
    });
  });

}
