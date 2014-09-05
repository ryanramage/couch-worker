var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');

test('pick up non-migrated documents from couchdb', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors'
  };

  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return !doc.hasOwnProperty('a') || !doc.hasOwnProperty('b');
    };
    api.migrated = function (doc) {
      return doc.hasOwnProperty('total');
    };
    api.migrate = function (doc, callback) {
      doc.total = doc.a + doc.b;
      return callback(null, doc);
    };
    return api;
  });

  var w = tmpworker.start(config);

  var migrated_doc = require('./fixtures/migrated.json').doc;
  var notmigrated_doc = require('./fixtures/notmigrated.json').doc;
  delete notmigrated_doc._rev;

  var url = test.COUCH_URL + '/example/' + notmigrated_doc._id;

  couchr.put(url, notmigrated_doc).apply(function () {
    setTimeout(function () {
      couchr.get(url, {}).apply(function (res) {
        var doc = res.body;
        t.equal(doc._rev.substr(0, 2), '2-', '_rev should only be two');
        delete doc._rev;
        delete migrated_doc._rev;
        t.deepEqual(doc, migrated_doc);
        w.stop();
        t.end();
      });
    }, 2000);
  });
});
