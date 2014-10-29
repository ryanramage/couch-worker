var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var fs = require('fs');


test('retry migration until run out of attempts', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors',
      retry_attempts: 3,
      retry_interval: 500,
      tmpfile: __dirname + '/retry-migration-fail.tmp'
    };

    var tmpworker = createWorker(
      __dirname + '/retry-migration-fail-worker.js'
    );

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
        t.equal(getAttempts(), 3, 'migrate called 3 times');
        couchr.get(url, {}).apply(function (res) {
          t.equal(res.body.migrated, undefined, 'doc not updated');
          var logurl = test.COUCH_URL + '/errors/_all_docs';
          couchr.get(logurl, {include_docs: true}).apply(function (res) {
            var rows = res.body.rows.filter(function (x) {
              return x.doc.type === 'error';
            });
            t.equal(rows.length, 1, 'one error logged');
            t.equal(
              rows[0].doc.error.message,
              'not yet',
              'migrate error included in log'
            );
            w.stop();
            t.end();
          });
        });
      }, 6000);
    });
});
