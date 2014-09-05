var createWorker = require('../index').createWorker;
var test = require('couch-worker-test-harness');


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


test('name is a required property', function (t) {
  var config = {
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors'
  };
  t.throws(function () {
    tmpworker.start(config);
  }, "Expected config.name to be a string");
  t.end();
});

test('database is a required property', function (t) {
  var config = {
    name: 'example-worker',
    log_database: test.COUCH_URL + '/errors'
  };
  t.throws(function () {
    tmpworker.start(config);
  }, "Expected config.database to be a string");
  t.end();
});

test('log_database is a required property', function (t) {
  var config = {
    name: 'example-worker',
    database: test.COUCH_URL + '/example'
  };
  t.throws(function () {
    tmpworker.start(config);
  }, "Expected config.log_database to be a string");
  t.end();
});
