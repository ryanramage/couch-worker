var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var fs = require('fs');


test('retries happen sequentially with interval', function (t) {
  t.plan(4);

  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    concurrency: 1,
    retry_attempts: 3,
    retry_interval: 500,
    tmpfile: __dirname + '/test-retry-migration-interval.tmp'
  };

  var tmpworker = createWorker(
    __dirname + '/test-retry-migration-interval-worker.js'
  );

  var getAttempts = function () {
    return getTimes().length;
  };
  var getTimes = function () {
    var lines = fs.readFileSync(config.tmpfile).toString().split('\n');
    lines.pop();
    return lines.map(Number);
  };

  var w = tmpworker.start(config);
  var url = test.COUCH_URL + '/example/testdoc';
  var doc = {
    _id: 'testdoc',
    foo: 'bar'
  };
  couchr.put(url, doc).apply(function (res) {
    setTimeout(function () {
      t.equal(getAttempts(), 2, 'migrate called 2 times');
      couchr.get(url, {}).apply(function (res) {
        t.equal(res.body.migrated, true, 'doc updated successfully');
        var logurl = test.COUCH_URL + '/errors/_all_docs';
        couchr.get(logurl, {include_docs: true}).apply(function (res) {
          var rows = res.body.rows.filter(function (x) {
            return x.doc.type === 'error';
          });
          t.equal(rows.length, 0, 'no errors logged');
          var times = getTimes();
          t.ok(
            times[1] - times[0] >= config.retry_interval,
            'time between migration attempts respects retry interval'
          );
          w.stop();
          t.end();
        });
      });
    }, 6000);
  });
});
