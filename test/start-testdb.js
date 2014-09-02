/**
 * If integration tests fail, this script will start up the couch instance
 * again so you can inspect any remaining data
 */

var MultiCouch = require('multicouch');
var couchr = require('highland-couchr');


var COUCH_PORT = 5989;
var COUCH_URL = 'http://localhost:' + COUCH_PORT;
var COUCH_DIR = __dirname + '/testdb';

var couch = new MultiCouch({
    prefix: COUCH_DIR,
    port: COUCH_PORT,
    respawn: false
});

couch.on('error', function (err) {
  console.error('CouchDB errored: %s', err);
});
couch.once('start', function () {
  console.log('CouchDB started');
});
couch.start();

setInterval(function () {
  console.log('.');
}, 10000);

process.on('exit', function () {
  couch.stop();
});
