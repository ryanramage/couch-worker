var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');
var fs = require('fs');


test('process docs from priority queue', function (t) {
  t.plan(5);

  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    concurrency: 2,
    tmpfile: __dirname + '/priority-queue.tmp'
  };

  var tmpworker = createWorker(__dirname + '/priority-queue-worker.js');

  var docs = _([
    {_id: 'a'},
    {_id: 'b'},
    {_id: 'c'},
    {_id: 'd'},
    {_id: 'e'},
    {_id: 'f'}
  ]);

  var getMigrateCalls = function () {
    // should have just started processing priority doc
    var migrate_calls = fs.readFileSync(config.tmpfile).toString().split('\n');
    // remove last newline
    migrate_calls.pop();
    return migrate_calls;
  };

  // post all docs to couchdb
  docs.map(couchr.post(test.COUCH_URL + '/example')).series()
    .toArray(function (responses) {
      // add 'd' and 'f' to priority queue
      var pdoc1 = {
        _id: 'pdoc1',
        type: 'priority',
        worker: 'couch-worker-example',
        database: test.COUCH_URL + '/example',
        id: 'd'
      };
      var pdoc2 = {
        _id: 'pdoc2',
        type: 'priority',
        worker: 'couch-worker-example',
        database: test.COUCH_URL + '/example',
        id: 'f'
      };
      var w = tmpworker.start(config);
      // give the worker time to start processing
      setTimeout(function () {
        var posts = _([
          couchr.post(test.COUCH_URL + '/errors', pdoc1),
          couchr.post(test.COUCH_URL + '/errors', pdoc2)
        ]);
        t.deepEqual(getMigrateCalls(), ['a','b']);
        posts.series().apply(function (res1, res2) {
          setTimeout(function () {
            t.deepEqual(getMigrateCalls(), ['a','b','d','c']);
            setTimeout(function () {
              t.deepEqual(getMigrateCalls().slice(0,5), ['a','b','d','c','f']);
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
