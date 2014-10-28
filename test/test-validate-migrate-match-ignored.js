var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');


test('migrate result can be ignored (instead of matching migrated predicate)', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors'
    };

    var tmpworker = createWorker(
      __dirname + '/test-validate-migrate-match-ignored-worker.js'
    );

    var w = tmpworker.start(config);
    var doc = {
      _id: 'testdoc',
      abc: 123
    };

    couchr.post(config.database, doc).apply(function (res) {
      setTimeout(function () {
        var q = {
          include_docs: true
        };
        couchr.get(config.log_database + '/_all_docs', q).apply(function (res) {
          var rows = res.body.rows.filter(function (x) {
            return x.doc.type === 'error';
          });
          t.equal(rows.length, 0, 'no errors logged');
          couchr.get(config.database + '/testdoc', {}).apply(function (res) {
            t.equal(res.body.ignored, true, 'doc was successfully updated');
            w.stop();
            t.end();
          });
        });
      }, 3000);
    });
});
