var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');


test('retry migration until successful', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors',
      retry_attempts: 3,
      retry_interval: 500
    };

    var attempts = 0;
    var tmpworker = createWorker(function (config) {
      var api = {};
      api.ignored = function (doc) {
        return doc._id[0] === '_' || doc.ignored;
      };
      api.migrated = function (doc) {
        return doc.migrated;
      };
      api.migrate = function (doc, callback) {
        attempts++;
        if (attempts === 2) {
          doc.migrated = true;
          return callback(null, doc);
        }
        else {
          return callback(new Error('not yet'));
        }
      };
      return api;
    });

    var w = tmpworker.start(config);
    var url = test.COUCH_URL + '/example/testdoc';
    var doc = {
      _id: 'testdoc',
      foo: 'bar'
    };
    couchr.put(url, doc).apply(function (res) {
      setTimeout(function () {
        t.equal(attempts, 2, 'migrate called 2 times');
        couchr.get(url, {}).apply(function (res) {
          t.equal(res.body.migrated, true, 'doc updated successfully');
          var logurl = test.COUCH_URL + '/errors/_all_docs';
          couchr.get(logurl, {include_docs: true}).apply(function (res) {
            var rows = res.body.rows.filter(function (x) {
              return x.doc.type === 'error';
            });
            t.equal(rows.length, 0, 'no errors logged');
            w.stop();
            t.end();
          });
        });
      }, 6000);
    });
});

test('retry migration until run out of attempts', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors',
      retry_attempts: 3,
      retry_interval: 500
    };

    var attempts = 0;
    var tmpworker = createWorker(function (config) {
      var api = {};
      api.ignored = function (doc) {
        return doc._id[0] === '_' || doc.ignored;
      };
      api.migrated = function (doc) {
        return doc.migrated;
      };
      api.migrate = function (doc, callback) {
        attempts++;
        return callback(new Error('not yet'));
      };
      return api;
    });

    var w = tmpworker.start(config);
    var url = test.COUCH_URL + '/example/testdoc';
    var doc = {
      _id: 'testdoc',
      foo: 'bar'
    };
    couchr.put(url, doc).apply(function (res) {
      setTimeout(function () {
        t.equal(attempts, 3, 'migrate called 3 times');
        couchr.get(url, {}).apply(function (res) {
          t.equal(res.body.migrated, undefined, 'doc not updated');
          var logurl = test.COUCH_URL + '/errors/_all_docs';
          couchr.get(logurl, {include_docs: true}).apply(function (res) {
            var rows = res.body.rows.filter(function (x) {
              return x.doc.type === 'error';
            });
            t.equal(rows.length, 1, 'one error logged');
            t.equal(
              rows[0].doc.error.message,
              'not yet',
              'migrate error included in log'
            );
            w.stop();
            t.end();
          });
        });
      }, 6000);
    });
});

test('retries happen sequentially with interval', function (t) {
  t.plan(4);

  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    retry_attempts: 3,
    retry_interval: 500
  };

  var attempts = 0;
  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return doc._id[0] === '_' || doc.ignored;
    };
    api.migrated = function (doc) {
      return doc.migrated;
    };
    api.migrate = function (doc, callback) {
      attempts++;
      if (attempts === 2) {
        setTimeout(function () {
          doc.migrated = true;
          return callback(null, doc);
        }, 500);
      }
      else {
        setTimeout(function () {
          t.equal(attempts, 1, 'no new attempt until this one returns');
        }, 750);
        setTimeout(function () {
          return callback(new Error('not yet'));
        }, 500);
      }
    };
    return api;
  });

  var w = tmpworker.start(config);
  var url = test.COUCH_URL + '/example/testdoc';
  var doc = {
    _id: 'testdoc',
    foo: 'bar'
  };
  couchr.put(url, doc).apply(function (res) {
    setTimeout(function () {
      t.equal(attempts, 2, 'migrate called 2 times');
      couchr.get(url, {}).apply(function (res) {
        t.equal(res.body.migrated, true, 'doc updated successfully');
        var logurl = test.COUCH_URL + '/errors/_all_docs';
        couchr.get(logurl, {include_docs: true}).apply(function (res) {
          var rows = res.body.rows.filter(function (x) {
            return x.doc.type === 'error';
          });
          t.equal(rows.length, 0, 'no errors logged');
          w.stop();
          t.end();
        });
      });
    }, 6000);
  });
});
