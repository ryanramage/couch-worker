var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');
var fs = require('fs');


test('resume changes processing from last processed seq id', function (t) {
  t.plan(2);

  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    checkpoint_size: 1,
    concurrency: 1,
    tmpfile: __dirname + '/resume-processing.tmp'
  };

  // extracted here so we can modify after creating a worker
  //var predicate = function (doc) {
  //  return doc.migrated;
  //};
  //var migrate = function (doc) {
  //  doc.migrated = true;
  //  return doc;
  //};

  var tmpworker = createWorker(__dirname + '/resume-processing-worker1.js');
  var tmpworker2 = createWorker(__dirname + '/resume-processing-worker2.js');

  var getMigrateCalls = function () {
    var calls = fs.readFileSync(config.tmpfile).toString().split('\n');
    calls.pop();
    return calls;
  };

  var url = test.COUCH_URL + '/example';

  var delay = function () {
    return _(function (push, next) {
      setTimeout(function () { push(null, _.nil); }, 1000);
    });
  };

  var tasksA = _([
    couchr.post(url, {_id: 'a'}),
    delay(),
    couchr.post(url, {_id: 'b'}),
    delay(),
    couchr.post(url, {_id: 'c'}),
    delay()
  ]);

  var tasksB = _([
    couchr.post(url, {_id: 'd'}),
    couchr.post(url, {_id: 'e'}),
    couchr.post(url, {_id: 'f'})
  ]);

  var w = tmpworker.start(config);

  tasksA.series().apply(function (a, b, c) {
    setTimeout(function () {
      t.deepEqual(getMigrateCalls(), ['a','b','c']);
      // stop listening to changes
      w.stop(function () {
        // add some more docs
        tasksB.series().apply(function (d, e, f) {
          // resume listening to changes
          var w2 = tmpworker2.start(config);
          setTimeout(function () {
            // check we didn't repeat 'migrated' checks for a and b,
            // c will be repeated since it's updated seq id will have been
            // filtered out of changes feed for this worker
            // (since it's already migrated)
            t.deepEqual(
              getMigrateCalls(), ['a','b','c','c','d','e','f'],
              'no migrations repeated'
            );
            w2.stop();
            t.end();
          }, 2000);
        });
      });
    }, 2000);
  });

});
