var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');
var fs = require('fs');


test('docs from priority queue never cause a checkpoint', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    checkpoint_size: 1,
    concurrency: 1,
    tmpfile: __dirname + '/test-priority-no-checkpoint.tmp'
  };

  var tmpworker = createWorker(
    __dirname + '/test-priority-no-checkpoint-worker.js'
  );

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
      var pdoc = {
        _id: 'pdoc',
        type: 'priority',
        worker: 'couch-worker-example',
        database: test.COUCH_URL + '/example',
        id: 'f'
      };
      var w = tmpworker.start(config);
      setTimeout(function () {
        // give the worker time to start processing
        couchr.post(test.COUCH_URL + '/errors', pdoc).apply(function (res) {
          // wait until doc 'a' done and checkpointed
          setTimeout(function () {
            t.deepEqual(getMigrateCalls(), ['a','b','f']);
            var local = test.COUCH_URL + '/example/_local/couch-worker-example';
            couchr.get(local, {}).apply(function (res) {
              t.equal(res.body.seq, 2);
              setTimeout(function () {
                couchr.get(local, {}).apply(function (res) {
                  t.deepEqual(getMigrateCalls(), ['a','b','f','c','d']);
                  // make sure priority migration didn't cause checkpoint update
                  t.equal(res.body.seq, 3);
                  w.stop();
                  t.end();
                });
              }, 3500);
            });
          }, 2500);
        });
      }, 2500);
    });

});
