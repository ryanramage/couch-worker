var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var fs = require('fs');


test('include _conflicts in documents provided to workers', function (t) {
  t.plan(2);
  // worker variable populated later, defined here for use inside api.migrate
  var worker;
  var conflictworker = createWorker(
    __dirname + '/provide-conflicts-worker.js'
  );

  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    tmpfile: __dirname + '/provide-conflicts.tmp'
  };

  var a = {_id: 'testdoc', a: 1};
  var b = {_id: 'testdoc', b: 2};

  var getNumConflicts = function () {
    return Number(fs.readFileSync(config.tmpfile).toString());
  };

  couchr.put(config.database + '/testdoc', a).apply(function (res) {
    // start listening to changes
    worker = conflictworker.start(config);
    // create a conflicting revision
    var opt = {all_or_nothing: true, docs: [b]};
    couchr.post(config.database + '/_bulk_docs', opt).apply(function (res) {
      t.ok(res.body[0].ok, 'put conflicting revision');
      setTimeout(function () {
        t.equal(getNumConflicts(), 1);
        worker.stop();
        t.end();
      }, 1000);
    });
  });
});
