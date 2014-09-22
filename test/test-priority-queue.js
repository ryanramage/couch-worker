var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');


test('process docs from priority queue', function (t) {
  t.plan(5);

  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    concurrency: 2
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
      migrate_calls.push(doc._id);
      doc.migrated = true;
      setTimeout(function () {
        return callback(null, doc);
      }, 2000);
    };
    return api;
  });

  var docs = _([
    {_id: 'a'},
    {_id: 'b'},
    {_id: 'c'},
    {_id: 'd'},
    {_id: 'e'},
    {_id: 'f'}
  ]);

  // post all docs to couchdb
  docs.map(couchr.post(test.COUCH_URL + '/example')).series()
    .toArray(function (responses) {
      // add 'd' and 'f' to priority queue
      var pdoc1 = {
        _id: 'pdoc1',
        type: 'priority',
        worker: 'couch-worker-example',
        id: 'd'
      };
      var pdoc2 = {
        _id: 'pdoc2',
        type: 'priority',
        worker: 'couch-worker-example',
        id: 'f'
      };
      var w = tmpworker.start(config);
      // give the worker time to start processing
      setTimeout(function () {
        var posts = _([
          couchr.post(test.COUCH_URL + '/errors', pdoc1),
          couchr.post(test.COUCH_URL + '/errors', pdoc2)
        ]);
        t.deepEqual(migrate_calls, ['a','b']);
        posts.series().apply(function (res1, res2) {
          setTimeout(function () {
            t.deepEqual(migrate_calls, ['a','b','d','c']);
            setTimeout(function () {
              t.deepEqual(migrate_calls.slice(0,5), ['a','b','d','c','f']);
              // make sure priority queue docs are cleaned up
              _([
                couchr.get(test.COUCH_URL + '/errors/' + pdoc1._id, {}),
                couchr.get(test.COUCH_URL + '/errors/' + pdoc2._id, {})
              ])
              .series()
              .errors(function (err) {
                // NOTE: this should be called twice, once for each doc
                // see the t.plan() call at the top of this test
                t.equal(
                  err.error, 'not_found',
                  'priority doc should not be found'
                );
              })
              .apply(function () {
                w.stop();
                t.end();
              })
            }, 4500);
          }, 2500);
        });
      }, 1500);
    });

});
