/**
 * Sets up a local CouchDB instance with fresh database to run integration
 * tests against
 */

var MultiCouch = require('multicouch');
var couchr = require('highland-couchr');
var pretape = require('pre-tape');
var rimraf = require('rimraf');
var mkdirp = require('mkdirp');
var exec = require('child_process').exec;


var COUCH_PORT = exports.COUCH_PORT = 5989;
var COUCH_URL = 'http://localhost:' + COUCH_PORT;
var COUCH_DIR = __dirname + '/testdb';


var couch = new MultiCouch({
    prefix: COUCH_DIR,
    port: COUCH_PORT,
    respawn: false
});

var exports = module.exports = pretape({
    setup: function (t, done) {
      // make sure we kill couch before starting it up again:
      var cmd = 'pkill -fu ' + process.env.USER + ' ' + COUCH_DIR;
      exec(cmd, function (err, stdout, stderr) {
        // delete any existing test database from previous runs
        rimraf.sync(COUCH_DIR);
        mkdirp.sync(COUCH_DIR);
        couch.on('error', function (err) {
          console.error('CouchDB errored: %s', err);
        });
        couch.once('start', function () {
          // give couch time to start accepting requests
          setTimeout(function () {
            couchr.put(COUCH_URL + '/example', {}).apply(function () {
              done();
            });
          }, 2000);
        });
        couch.start();
      });
    },
    teardown: function (t) {
      couch.stop();
    }
});

// provide these values to users of the harness
exports.COUCH_PORT = COUCH_PORT;
exports.COUCH_URL = COUCH_URL;
exports.COUCH_DIR = COUCH_DIR;
