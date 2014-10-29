var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');


test('eventual results from timed out calls are discarded', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    retry_attempts: 3,
    retry_interval: 2000,
    timeout: 1000
  };

  var tmpworker = createWorker(
    __dirname + '/test-worker-timeouts-retries-worker.js'
  );

  var w = tmpworker.start(config);
  var doc = {_id: 'a'};

  couchr.post(test.COUCH_URL + '/example', doc).apply(function (res) {
    doc._rev = res.body.rev;
    setTimeout(function () {
      var logurl = test.COUCH_URL + '/errors/_all_docs';
      var q = {include_docs: true};
      couchr.get(logurl, q).apply(function (res) {
        var rows = res.body.rows;
        // no errors logged
        var errors = rows.filter(function (x) {
          return x.doc.type === 'error';
        });
        t.equal(errors.length, 0);
        couchr.get(test.COUCH_URL + '/example/a', {}).apply(function (res) {
          t.equal(res.body.migrated, 3, 'third call is successful');
          w.stop();
          t.end();
        });
      });
    }, 8000);
  });

});
