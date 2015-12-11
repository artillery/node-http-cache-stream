#!/usr/bin/env node
/**
 * Copyright (c) 2015 Artillery Games, Inc. All rights reserved.
 *
 * This source code is licensed under the MIT-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Command line tool for doing simple interactive tests with the http cache.
// Not intended to be a full-featured commandline http client like curl or wget.

var HTTPCache = require('../index.js').HTTPCache;
var fs = require('fs');
var commander = require('commander');

var argv = commander
  .version(0)
  .usage('<operation> <url> [options]')
  .description("Fetch a URL with the node-http-disk-cache module")
  .option('-d, --dir <directory>', 'Directory to use for cache.', String, '/tmp/http-disk-cache')
  .option('-v, --verbose', 'Turn on debugging output.', Boolean)
  .option('-f, --force', 'Overwrite existing files.', Boolean)
  .option('-e, --etag-format <format>', 'Specify an etag format, e.g. md5', String)
  .parse(process.argv)

if (argv.verbose) {
  process.env.DEBUG = 'http-disk-cache';
}

commander
  .command("get <url> <output-file>")
  .description("GET the specified URL")
  .action(function(url, outputFile) {
    var cache = new HTTPCache(argv.dir);
    var options = {
      url: url
    };
    if (argv.etagFormat) { options.etagFormat = argv.etagFormat; }

    cache.openReadStream(url, function(err, stream) {
      if (err != null) {
        console.error("Couldn't fetch '" + url + "': " + err);
        process.exit(1);
      }
      if (outputFile) {
        if (fs.existsSync(outputFile) && !argv.force) {
          console.error("File " + outputFile + " already exists");
          process.exit(1);
        }
        var ostream = fs.createWriteStream(outputFile);
        ostream.on('error', function (error) {
          console.error("Error writing to " + outputFile + ": " + error);
          process.exit(1);
        });
        stream.pipe(ostream);
      } else {
        stream.pipe(process.stdout);
      }

    });

    console.log(url);
  });

commander.parse(process.argv)
