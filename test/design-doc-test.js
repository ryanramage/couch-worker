var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');


test('send design doc for progress views', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    concurrency: 5
  };

  var migrate_calls = [];
  var tmpworker = createWorker(__dirname + '/design-doc-worker.js');

  var w = tmpworker.start(config);
  var url = config.database + '/_design/' +
    encodeURIComponent('worker:couch-worker-example');

  var docs = _([
    {_id: 'a', migrated: true},
    {_id: 'b', migrated: true},
    {_id: 'c', migrated: true},
    {_id: 'd'},
    {_id: 'e'},
    {_id: 'f', ignored: true, migrated: true},
    {_id: 'g', ignored: true}
  ]);

  var post_docs = docs.map(
    couchr.post(config.database)
  );

  setTimeout(function () {
    couchr.get(url, {}).apply(function (res) {
      t.equal(res.statusCode, 200);
      post_docs
        .parallel(5)
        .toArray(function (repsonses) {
          var ddoc = config.database + '/_design/' +
            encodeURIComponent('worker:' + config.name);

          var views = _([
            couchr.get(ddoc + '/_view/migrated', {reduce: true}),
            couchr.get(ddoc + '/_view/not_migrated', {reduce: true})
          ])
          views.parallel(2).toArray(function (responses) {
            var migrated = responses[0].body.rows[0].value;
            var not_migrated = responses[1].body.rows[0].value;
            t.equal(migrated, 3, 'three not-ignored migrated docs');
            t.equal(not_migrated, 2, 'two not-ignored not-migrated docs');
            w.stop();
            t.end();
          });
        });
    });
  }, 2000);
});
