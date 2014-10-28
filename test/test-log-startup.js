var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');


test('create log document on startup', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    concurrency: 5
  };

  var migrate_calls = [];
  var tmpworker = createWorker(__dirname + '/test-log-startup-worker.js');

  var w = tmpworker.start(config);
  var url = config.log_database + '/_all_docs/';

  setTimeout(function () {
    couchr.get(url, {include_docs: true}).apply(function (res) {
      t.equal(res.statusCode, 200);
      var rows = res.body.rows;
      t.equal(rows.length, 1, 'document added to log db');
      var doc = rows[0].doc;
      t.ok(doc.time, 'doc has a time property');
      t.equal(doc.type, 'started');
      t.equal(doc.name, config.name);
      t.equal(doc.database, test.COUCH_URL_NO_AUTH + '/example');
      w.stop();
      t.end();
    });
  }, 2000);
});
