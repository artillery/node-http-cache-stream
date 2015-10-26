/**
 * Copyright (c) 2015 Artillery Games, Inc. All rights reserved.
 *
 * This source code is licensed under the MIT-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

var http = require('http');
var fs = require('fs');
var temp = require('temp');
var execSync = require('child_process').execSync;
var debug = require('debug')('http-disk-cache');

var httpcache = require('./index');

process.on('uncaughtException', function (err) {
    console.error(err.stack);
    process.exit(1);
});

function newUrlReply(contents, status, headers) {
  if (!headers) {
    headers = {};
  }
  return {
    contents: contents,
    status: status,
    headers: headers,
    fetchCount: 0
  };
}

function catStream(stream, cb) {
  chunks = [];
  stream.on('data', function (chunk) {
    chunks.push(chunk);
  });
  stream.on('end', function () {
    if (chunks.length === 0) {
      cb(null);
    } else if (typeof chunks[0] === 'string') {
      cb(chunks.join(''));
    } else { // Buffer
      cb(Buffer.concat(chunks));
    }
  });
}

exports.tests = {
  setUp: function (cb) {
    var _this = this;
    temp.mkdir('cache', function(err, path) {
      if (err) {
        throw new Error("Error creating temp dir", err);
      }
      debug("cache root: ", path);

      _this.nowSeconds = 1000;
      _this.cache = new httpcache.HTTPCache(path);
      _this.cache._now = function() {
        return _this.nowSeconds;
      };

      _this.serverUrls = {
        '/url1': newUrlReply('url1 contents', 200, { 'Cache-Control': 'max-age=200' }),
        '/url2': newUrlReply('url2 contents', 200, { 'Cache-Control': 'no-cache' }),
        '/url3': newUrlReply('url3 contents', 200, {}),
        '/url4': newUrlReply('url4 contents', 200, { 'Cache-Control': 'max-age=200=unparseable' }),
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
        response.statusCode = reply.status;
        for (var header in reply.headers) {
          response.setHeader(header, reply.headers[header]);
        }
        response.write(reply.contents);
        response.end();
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
      catStream(stream, function (contents) {
        test.equal(contents.toString('utf8'), 'url1 contents');
        test.done();
      });
    });
  },

  testConcurrentRequests: function(test) {
    test.expect(4);
    var _this = this;
    var count = 2;

    barrier = function() {
      count--;
      if (count === 0) { test.done(); }
    };
    var cb = function(err, stream, path) {
      test.ok(stream instanceof fs.ReadStream, "stream should be an fs.ReadStream");
      catStream(stream, function (contents) {
        test.equal(contents.toString('utf8'), 'url1 contents');
        barrier();
      });
    };
    this.cache.openReadStream(this.createUrl('/url1'), cb);
    this.cache.openReadStream(this.createUrl('/url1'), cb);
  },


  testBasicCaching: function(test) {
    test.expect(8);
    doTest(this, test, '/url1', 'url1 contents', false, true, 0, test.done);
  },

  testExplicitNoCache: function(test) {
    test.expect(8);
    doTest(this, test, '/url2', 'url2 contents', false, false, 0, test.done);
  },

  testUnparseableCacheControl: function(test) {
    test.expect(8);
    doTest(this, test, '/url4', 'url4 contents', false, false, 0, test.done);
  },

  testNoCache: function(test) {
    // URLs without a Cache-Control header don't get cached.
    test.expect(8);
    doTest(this, test, '/url3', 'url3 contents', false, false, 0, test.done);
  },

  testUnexpiredCache: function(test) {
    test.expect(8);
    // 200 is the maximum allowable age.
    doTest(this, test, '/url1', 'url1 contents', false, true, 200, test.done);
  },

  testExpiredCache: function(test) {
    test.expect(8);
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
    var urls = Object.keys(this.serverUrls);
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
  }

};

// Load the url twice, advancing the time by deltaT in between the two fetches.
// Verify that the url is cached or not cached as expected at each fetch.
function doTest(_this, test, url, contents, firstCached, secondCached, deltaT, cb) {
  var count = 0;
  if (!deltaT) { deltaT = 0; }
  _this.cache.openReadStream(_this.createUrl(url), function(err, stream, path) {
    if (!firstCached) { count++; }
    if (!stream) {
      test.ok(err, "if stream is null there had better be an error");
      return cb();
    }
    test.ok(stream instanceof fs.ReadStream, "stream should be an fs.ReadStream");
    test.ok(fs.existsSync(path));
    catStream(stream, function (contents) {
      test.equal(contents.toString('utf8'), contents);
      test.equal(_this.serverUrls[url].fetchCount, count);
      _this.nowSeconds += deltaT;
      _this.cache.reset();
      _this.cache.openReadStream(_this.createUrl(url), function(err, stream, path) {
        if (!secondCached) { count++; }
        test.ok(stream instanceof fs.ReadStream, "stream should be an fs.ReadStream");
        test.ok(fs.existsSync(path));
        test.equal(_this.serverUrls[url].fetchCount, count);
        catStream(stream, function (contents) {
          test.equal(contents.toString('utf8'), contents);
          cb();
        });
      });
    });
  });

}
