var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');


test('docs from priority queue never cause a checkpoint', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    checkpoint_size: 1,
    concurrency: 1
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
            // should have just started processing priority doc
            t.deepEqual(migrate_calls, ['a','b','f']);
            var local = test.COUCH_URL + '/example/_local/couch-worker-example';
            console.error(local);
            couchr.get(local, {}).apply(function (res) {
              console.error(res.body);
              t.equal(res.body.seq, 2);
              setTimeout(function () {
                couchr.get(local, {}).apply(function (res) {
                  console.error(res.body);
                  t.deepEqual(migrate_calls, ['a','b','f','c','d']);
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
