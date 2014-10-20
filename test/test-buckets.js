var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');


test('process only docs in workers bucket range', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    bucket: {
      start: '8',
      end: 'c'
    }
  };

  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return (
        doc._id[0] === '_' ||
        !doc.hasOwnProperty('a') ||
        !doc.hasOwnProperty('b')
      );
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

  var doc1 = {a: 1, b: 2};
  var doc2 = {a: 3, b: 4};

  var puts = _([
    couchr.put(config.database + '/foo', doc1), // md5 of 'foo' = acbd18...
    couchr.put(config.database + '/bar', doc2)  // md5 of 'bar' = 37b51d...
  ]);

  puts.parallel(2).apply(function (res1, res2) {
    setTimeout(function () {
      couchr.get(config.database + '/foo', {}).apply(function (res) {
        var doc = res.body;
        t.equal(doc._rev.substr(0, 2), '2-', '_rev should be two');
        t.equal(doc.total, 3);
        couchr.get(config.database + '/bar', {}).apply(function (res) {
          var doc = res.body;
          t.equal(doc._rev.substr(0, 2), '1-', '_rev should be one');
          t.ok(!doc.total);
          w.stop();
          t.end();
        });
      });
    }, 2000);
  });
});
