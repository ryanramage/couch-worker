var createWorker = require('../index').createWorker;
var fulltest = require('couch-worker-test-harness');
var worker = require('../index');
var test = require('tape');


var tmpworker = createWorker(function (config) {
  var api = {};
  api.ignored = function (doc) {
    return doc._id[0] === '_';
  };
  api.migrated = function (doc) {
    return doc.migrated;
  };
  api.migrate = function (doc, callback) {
    doc.migrated = true;
    return callback(null, [doc]);
  };
  return api;
});

var config = {
  name: 'example-worker',
  database: fulltest.COUCH_URL + '/example',
  log_database: fulltest.COUCH_URL + '/errors'
};

fulltest('readConfig is called synchronously during setup', function (t) {
  t.plan(2);
  var err = new Error('fail');
  var _readConfig = worker.readConfig;
  worker.readConfig = function (cfg) {
    t.deepEqual(config, cfg, 'config object passed to readConfig');
    throw err;
  };
  try {
    var w = tmpworker.start(config);
  }
  catch (e) {
    t.equal(err, e, 'Error thrown from readConfig are exposed by start()');
  }
  worker.readConfig = _readConfig;
  t.end();
});

test('name is a required property', function (t) {
  t.throws(function () {
    worker.readConfig({
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors'
    });
  });
  t.end();
});

test('database is a required property', function (t) {
  t.throws(function () {
    worker.readConfig({
      name: 'example-worker',
      log_database: test.COUCH_URL + '/errors'
    });
  });
  t.end();
});

test('log_database is a required property', function (t) {
  t.throws(function () {
    worker.readConfig({
      name: 'example-worker',
      database: test.COUCH_URL + '/example'
    });
  });
  t.end();
});
