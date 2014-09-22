var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');

test('skip change events for docs with in-progress migrations', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    concurrency: 5
  };

  var migrate_calls = [];
  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return doc._id[0] === '_';
    };
    api.migrated = function (doc) {
      return doc.migrated;
    };
    api.migrate = function (doc, callback) {
      migrate_calls.push(doc._rev);
      setTimeout(function () {
        //delete doc._rev;
        doc.migrated = true;
        return callback(null, doc);
      }, 2000);
    };
    return api;
  });

  var worker = tmpworker.start(config);
  var doc = {_id: 'testdoc', foo: 'bar'};

  var url = test.COUCH_URL + '/example/testdoc';

  couchr.put(url, doc).apply(function (res) {
    doc.asdf = 'asdf';
    doc._rev = res.body.rev;
    setTimeout(function () {
      couchr.put(url, doc).apply(function (res) {
        setTimeout(function () {
          couchr.get(url, {conflicts: true}).apply(function (res) {
            var newdoc = res.body;
            t.ok(!newdoc.asdf, 'asdf not set');
            t.equal(newdoc._rev.substr(0, 2), '2-', '_rev should only be two');
            t.equal(
              newdoc._conflicts && newdoc._conflicts.length, 1,
              'there should be 1 conflict'
            );
            t.equal(migrate_calls.length, 1, 'one call to migrate function');
            worker.stop();
            t.end();
          });
        }, 6000);
      });
    }, 500);
  });
});
