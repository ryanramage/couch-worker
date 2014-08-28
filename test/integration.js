var MultiCouch = require('multicouch');
var couchr = require('highland-couchr');
var pretape = require('pre-tape');
var rimraf = require('rimraf');
var mkdirp = require('mkdirp');
var basic_worker = require('./lib/basic-worker');
var slow_worker = require('./lib/slow-worker');
var multidoc_worker = require('./lib/multidoc-worker');
var createWorker = require('../index').createWorker;
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
                t.equal(
                  doc._conflicts && doc._conflicts.length, 1,
                  'there should be 1 conflict'
                );
                worker.stop();
                t.end();
              });
            }, 2000);
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

test('include _conflicts in documents provided to workers', function (t) {
    t.plan(4);
    // worker variable populated later, defined here for use inside api.migrate
    var worker;

    var conflictworker = createWorker(function (config) {
        var api = {};
        api.ignored = function (doc) {
            t.equal(
                doc._conflicts && doc._conflicts.length, 1,
                'should have one _conflict revision'
            );
            return false;
        };
        api.migrated = function (doc) {
            t.equal(
                doc._conflicts && doc._conflicts.length, 1,
                'should have one _conflict revision'
            );
            return false;
        };
        api.migrate = function (doc, callback) {
            t.equal(
                doc._conflicts && doc._conflicts.length, 1,
                'should have one _conflict revision'
            );
            worker.stop();
            t.end();
        };
        return api;
    });

    var config = {
      name: 'couch-worker-example',
      database: COUCH_URL + '/example',
    };

    var a = {_id: 'testdoc', a: 1};
    var b = {_id: 'testdoc', b: 2};

    couchr.put(config.database + '/testdoc', a).apply(function (res) {
        // start listening to changes
        worker = conflictworker.start(config);
        // create a conflicting revision
        var opt = {all_or_nothing: true, docs: [b]};
        couchr.post(config.database + '/_bulk_docs', opt).apply(function (res) {
            t.ok(res.body[0].ok, 'put conflicting revision');
        });
    });
});

test('skip change events for docs with in-progress migrations', function (t) {
  var store = {};
  var config = {
    name: 'couch-worker-example',
    database: COUCH_URL + '/example',
  };

  var migrate_calls = [];
  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return false;
    };
    api.migrated = function (doc) {
      return doc.migrated;
    };
    api.migrate = function (doc, callback) {
      setTimeout(function () {
        migrate_calls.push(doc._rev);
        doc.migrated = true;
        return callback(null, doc);
      }, 2000);
    };
    return api;
  });

  var worker = tmpworker.start(config);
  var doc = {_id: 'testdoc', foo: 'bar'};

  var url = COUCH_URL + '/example/testdoc';

  couchr.put(url, doc).apply(function (res) {
    doc.asdf = 'asdf';
    doc._rev = res.body.rev;
    couchr.put(url, doc).apply(function () {
      setTimeout(function () {
        couchr.get(url, {conflicts: true}).apply(function (res) {
          var newdoc = res.body;
          t.equal(newdoc._rev.substr(0, 2), '2-', '_rev should only be two');
          t.equal(
            newdoc._conflicts && newdoc._conflicts.length, 1,
            'there should be 1 conflict'
          );
          t.equal(migrate_calls.length, 1);
          worker.stop();
          t.end();
        });
      }, 4000);
    });
  });
});