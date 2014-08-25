var MultiCouch = require('multicouch');
var couchr = require('highland-couchr');
var pretape = require('pre-tape');
var rimraf = require('rimraf');
var mkdirp = require('mkdirp');
var basic_worker = require('./lib/basic-worker');
var slow_worker = require('./lib/slow-worker');
var multidoc_worker = require('./lib/multidoc-worker');
var exec = require('child_process').exec;
var _ = require('highland');


var COUCH_PORT = 5989;
var COUCH_URL = 'http://localhost:' + COUCH_PORT;
var COUCH_DIR = __dirname + '/testdb';


var couch = new MultiCouch({
    prefix: COUCH_DIR,
    port: COUCH_PORT,
    respawn: false
});

var test = pretape({
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
})


test('check couchdb started and example database created', function (t) {
  couchr.get(COUCH_URL + '/example', {}).apply(function (x) {
    t.equal(x.body.db_name, 'example');
    t.end();
  });
});

test('pick up non-migrated documents from couchdb', function (t) {
  var store = {};
  var config = {
    name: 'couch-worker-example',
    database: COUCH_URL + '/example',
  };

  var worker = basic_worker.start(config);
  var migrated_doc = require('./fixtures/migrated.json').doc;
  var notmigrated_doc = require('./fixtures/notmigrated.json').doc;
  delete notmigrated_doc._rev;

  var url = COUCH_URL + '/example/' + notmigrated_doc._id;

  couchr.put(url, notmigrated_doc).apply(function () {
    setTimeout(function () {
      couchr.get(url, {}).apply(function (res) {
        var doc = res.body;
        t.equal(doc._rev.substr(0, 2), '2-', '_rev should only be two');
        delete doc._rev;
        delete migrated_doc._rev;
        t.deepEqual(doc, migrated_doc);
        worker.stop();
        t.end();
      });
    }, 2000);
  });
});

test('enable conflicts when writing documents back to couchdb', function (t) {
  var store = {};
  var config = {
    name: 'couch-worker-example',
    database: COUCH_URL + '/example',
  };

  var worker = slow_worker.start(config);
  var migrated_doc = require('./fixtures/migrated.json').doc;
  var notmigrated_doc = require('./fixtures/notmigrated.json').doc;
  delete notmigrated_doc._rev;

  var url = COUCH_URL + '/example/' + notmigrated_doc._id;

  couchr.put(url, notmigrated_doc).apply(function (res) {
    setTimeout(function () {
        var conflict_doc = require('./fixtures/conflict.json');
        conflict_doc._rev = res.body.rev;
        couchr.put(url, conflict_doc).apply(function () {
            setTimeout(function () {
              couchr.get(url, {conflicts: true}).apply(function (res) {
                var doc = res.body;
                t.equal(doc._rev.substr(0, 2), '2-', '_rev should only be two');
                t.equal(doc._conflicts.length, 1, 'there should be 1 conflict');
                worker.stop();
                t.end();
              });
            }, 4000);
        });
    }, 1000);
  });
});

test('return multiple docs from worker', function (t) {
    var store = {};
    var config = {
      name: 'couch-worker-example',
      database: COUCH_URL + '/example',
    };

    var worker = multidoc_worker.start(config);
    var notmigrated_doc = require('./fixtures/notmigrated.json').doc;
    delete notmigrated_doc._rev;

    var url = COUCH_URL + '/example/' + notmigrated_doc._id;

    // TODO: move this to highland.js
    function delay(ms) {
      return _(function (push) {
        setTimeout(function () { push(null, _.nil); }, 2000);
      });
    }

    var tasks = _([
      couchr.put(url, notmigrated_doc),
      delay(2000),
      couchr.get(url, {}),
      couchr.get(COUCH_URL + '/example/a', {}),
      couchr.get(COUCH_URL + '/example/b', {})
    ]);

    tasks.series().apply(function (put, original, a, b) {
      var doc = original.body;
      t.equal(doc._rev.substr(0, 2), '2-', '_rev should only be two');
      delete doc._rev;
      delete doc._id;
      t.deepEqual(doc, {
          a: notmigrated_doc.a,
          b: notmigrated_doc.b,
          split: true
      });
      // get other documents returned from migration step
      delete a.body._rev;
      t.deepEqual(a.body, {_id: 'a', a: notmigrated_doc.a});
      delete b.body._rev;
      t.deepEqual(b.body, {_id: 'b', b: notmigrated_doc.b});
      worker.stop();
      t.end();
    });
});
