var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');


test('migrate result must match migrated predicate', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors'
    };

    var tmpworker = createWorker(
      __dirname + '/validate-migrate-match-migrated-predicate-worker.js'
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
          t.equal(rows.length, 1, 'one error log');
          t.equal(rows[0].doc.error.message,
            'Migrate result did not match migrated or ignored predicates'
          );
          w.stop();
          t.end();
        });
      }, 4000);
    });
});
