var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var fs = require('fs');


test('retry migration until successful', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors',
      retry_attempts: 3,
      retry_interval: 500,
      tmpfile: __dirname + '/test-retry-migration-success.tmp'
    };

    var tmpworker = createWorker(__dirname + '/test-retry-migration-success-worker.js');

    var getAttempts = function () {
      return Number(fs.readFileSync(config.tmpfile).toString());
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
            w.stop();
            t.end();
          });
        });
      }, 6000);
    });
});
