var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');


test('migrate function must return current doc', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors'
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
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors'
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
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors'
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
