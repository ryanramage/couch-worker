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
  var config = {
    name: 'couch-worker-example',
    database: COUCH_URL + '/example',
    log_database: COUCH_URL + '/errors'
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
  var config = {
    name: 'couch-worker-example',
    database: COUCH_URL + '/example',
    log_database: COUCH_URL + '/errors'
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
    var config = {
      name: 'couch-worker-example',
      database: COUCH_URL + '/example',
      log_database: COUCH_URL + '/errors'
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
      log_database: COUCH_URL + '/errors'
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
  var config = {
    name: 'couch-worker-example',
    database: COUCH_URL + '/example',
    log_database: COUCH_URL + '/errors',
    concurrency: 5
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
        delete doc._rev;
        doc.migrated = true;
        return callback(null, doc);
      }, 1000);
    };
    return api;
  });

  var worker = tmpworker.start(config);
  var doc = {_id: 'testdoc', foo: 'bar'};

  var url = COUCH_URL + '/example/testdoc';

  couchr.put(url, doc).apply(function (res) {
    doc.asdf = 'asdf';
    doc._rev = res.body.rev;
    setTimeout(function () {
      couchr.get(url, {conflicts: true}).apply(function (res) {
        var newdoc = res.body;
        t.equal(newdoc._rev.substr(0, 2), '1-', '_rev should only be one');
        t.equal(
          newdoc._conflicts && newdoc._conflicts.length, 1,
          'there should be 1 conflict'
        );
        t.equal(migrate_calls.length, 2, 'two calls to migrate function');
        worker.stop();
        t.end();
      });
    }, 3000);
  });
});

test('resume changes processing from last processed seq id', function (t) {
  t.plan(2);

  var config = {
    name: 'couch-worker-example',
    database: COUCH_URL + '/example',
    log_database: COUCH_URL + '/errors'
  };

  // extracted here so we can modify after creating a worker
  var predicate = function (doc) {
    return doc.migrated;
  };
  var migrate = function (doc) {
    doc.migrated = true;
    return doc;
  };

  var migrate_calls = [];
  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return false;
    };
    api.migrated = function (doc) {
      return predicate(doc);
    };
    api.migrate = function (doc, callback) {
      migrate_calls.push(doc._id);
      return callback(null, migrate(doc));
    };
    return api;
  });

  var url = COUCH_URL + '/example';

  var tasksA = _([
    couchr.post(url, {_id: 'a'}),
    couchr.post(url, {_id: 'b'}),
    couchr.post(url, {_id: 'c'})
  ]);

  var tasksB = _([
    couchr.post(url, {_id: 'd'}),
    couchr.post(url, {_id: 'e'}),
    couchr.post(url, {_id: 'f'})
  ]);

  var w = tmpworker.start(config);

  tasksA.series().apply(function (a, b, c) {
    setTimeout(function () {
      t.deepEqual(migrate_calls, ['a','b','c']);
      // stop listening to changes
      w.stop(function () {
        // change predicate and migrate function so it'll re-run on a,b,c if
        // it encounters them
        predicate = function (doc) {
          return doc.migrated2;
        };
        migrate = function (doc) {
          doc.migrated2 = true;
          return doc;
        };
        // add some more docs
        tasksB.series().apply(function (d, e, f) {
          // resume listening to changes
          var w2 = tmpworker.start(config);
          setTimeout(function () {
            // check we didn't repeat 'migrated' checks for a,b,c
            t.deepEqual(
              migrate_calls, ['a','b','c','d','e','f'],
              'no migrations repeated'
            );
            w2.stop();
            t.end();
          }, 2000);
        });
      });
    }, 2000);
  });

});

test('log errors to separate db', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: COUCH_URL + '/example',
    log_database: COUCH_URL + '/errors'
  };

  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return false;
    };
    api.migrated = function (doc) {
      return doc.migrated;
    };
    api.migrate = function (doc, callback) {
      doc.migrated = true;
      var e = new Error('Fail!');
      e.stack = '<stacktrace>';
      e.custom = 123;
      return callback(e);
    };
    return api;
  });

  var os = require('os');
  var _hostname = os.hostname;
  var _platform = os.platform;
  var _arch = os.arch;
  var _networkInterfaces = os.networkInterfaces;
  var _version = process.version;
  os.hostname = function () { return 'fakehostname'; };
  os.platform = function () { return 'linux'; };
  os.arch = function () { return 'ia32'; };
  os.networkInterfaces = function () {
    return {
      'lo': [
        {
          address: '127.0.0.1',
          family: 'IPv4',
          internal: true
        },
        {
          address: '::1',
          family: 'IPv6',
          internal: true
        }
      ],
      'wlan0': [
        {
          address: '10.1.4.133',
          family: 'IPv4',
          internal: false
        },
        {
          address: 'fe80::221:6aff:fe41:a8d6',
          family: 'IPv6',
          internal: false
        }
      ]
    }
  };
  process.version = 'v0.10.30';

  var w = tmpworker.start(config);
  var doc = {_id: 'a'};
  couchr.post(COUCH_URL + '/example', doc).apply(function (res) {
    doc._rev = res.body.rev;
    setTimeout(function () {
      var logurl = COUCH_URL + '/errors/_all_docs';
      couchr.get(logurl, {include_docs: true}).apply(function (res) {
        var rows = res.body.rows;
        t.equal(rows.length, 1);
        var logdoc = rows[0].doc;
        delete logdoc._rev;
        delete logdoc._id;
        t.ok(logdoc.time, 'log doc has time property');
        var timediff = Math.abs(
          new Date(logdoc.time).getTime() - new Date().getTime()
        );
        t.ok(timediff < 1000*60*60*24, 'error logged some time today');
        // delete time from doc for easier comparison
        delete logdoc.time;
        t.deepEqual(logdoc, {
          worker: {
            name: 'couch-worker-example',
            hostname: 'fakehostname',
            platform: 'linux',
            node_version: 'v0.10.30',
            arch: 'ia32',
            addresses: [
              '10.1.4.133',
              'fe80::221:6aff:fe41:a8d6'
            ]
          },
          //time: '2009-02-13T23:31:30.123Z',
          database: COUCH_URL + '/example',
          seq: 1,
          error: {
            message: 'Fail!',
            stack: '<stacktrace>',
            custom: 123
          },
          doc: doc
        });
        os.hostname = _hostname;
        os.platform = _platform;
        os.arch = _arch;
        os.networkInterfaces = _networkInterfaces;
        process.version = _version;
        w.stop();
        t.end();
      });
    }, 2000);
  });

});

test('migrate function must return current doc', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: COUCH_URL + '/example',
      log_database: COUCH_URL + '/errors'
    };

    var tmpworker = createWorker(function (config) {
        var api = {};
        api.ignored = function (doc) {
          return false;
        };
        api.migrated = function (doc) {
          return doc.migrated;
        };
        api.migrate = function (doc, callback) {
          callback(null, [{_id: 'otherdoc', foo: 'bar'}]);
        };
        return api;
    });

    var w = tmpworker.start(config);
    var doc = {
      _id: 'testdoc',
      abc: 123
    };

    couchr.post(config.database, doc).apply(function (res) {
      setTimeout(function () {
        var q = {
          include_docs: true
        };
        couchr.get(config.log_database + '/_all_docs', q).apply(function (res) {
          var rows = res.body.rows;
          t.equal(rows.length, 1);
          t.equal(rows[0].doc.error.message,
            'Migrate function did not return original document'
          );
          w.stop();
          t.end();
        });
      }, 2000);
    });
});

test('migrate result must match migrated predicate', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: COUCH_URL + '/example',
      log_database: COUCH_URL + '/errors'
    };

    var tmpworker = createWorker(function (config) {
        var api = {};
        api.ignored = function (doc) {
          return false;
        };
        api.migrated = function (doc) {
          return doc.migrated;
        };
        api.migrate = function (doc, callback) {
          doc.migrated = false;
          callback(null, doc);
        };
        return api;
    });

    var w = tmpworker.start(config);
    var doc = {
      _id: 'testdoc',
      abc: 123
    };

    couchr.post(config.database, doc).apply(function (res) {
      setTimeout(function () {
        var q = {
          include_docs: true
        };
        couchr.get(config.log_database + '/_all_docs', q).apply(function (res) {
          var rows = res.body.rows;
          t.equal(rows.length, 1);
          t.equal(rows[0].doc.error.message,
            'Migrate result did not match migrated or ignored predicates'
          );
          w.stop();
          t.end();
        });
      }, 2000);
    });
});

test('migrate result can be ignored (instead of matching migrated predicate)', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: COUCH_URL + '/example',
      log_database: COUCH_URL + '/errors'
    };

    var tmpworker = createWorker(function (config) {
        var api = {};
        api.ignored = function (doc) {
          return doc.ignored;
        };
        api.migrated = function (doc) {
          return doc.migrated;
        };
        api.migrate = function (doc, callback) {
          doc.migrated = false;
          doc.ignored = true;
          callback(null, doc);
        };
        return api;
    });

    var w = tmpworker.start(config);
    var doc = {
      _id: 'testdoc',
      abc: 123
    };

    couchr.post(config.database, doc).apply(function (res) {
      setTimeout(function () {
        var q = {
          include_docs: true
        };
        couchr.get(config.log_database + '/_all_docs', q).apply(function (res) {
          var rows = res.body.rows;
          t.equal(rows.length, 0, 'no errors logged');
          couchr.get(config.database + '/testdoc', {}).apply(function (res) {
            t.equal(res.body.ignored, true, 'doc was successfully updated');
            w.stop();
            t.end();
          });
        });
      }, 2000);
    });
});
