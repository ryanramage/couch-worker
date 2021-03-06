var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');

test('enable conflicts when writing documents back to couchdb', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors'
  };

  var tmpworker = createWorker(__dirname + '/enable-conflicts-worker.js');
  var w = tmpworker.start(config);

  var migrated_doc = require('./fixtures/migrated.json').doc;
  var notmigrated_doc = require('./fixtures/notmigrated.json').doc;
  delete notmigrated_doc._rev;

  var url = test.COUCH_URL + '/example/' + notmigrated_doc._id;

  couchr.put(url, notmigrated_doc).apply(function (res) {
    setTimeout(function () {
        var conflict_doc = require('./fixtures/conflict.json');
        conflict_doc._rev = res.body.rev;
        couchr.put(url, conflict_doc).apply(function () {
            setTimeout(function () {
              couchr.get(url, {conflicts: true}).apply(function (res) {
                var doc = res.body;
                t.equal(doc._rev.substr(0, 2), '2-', '_rev should only be two');
                t.equal(
                  doc._conflicts && doc._conflicts.length, 1,
                  'there should be 1 conflict'
                );
                w.stop();
                t.end();
              });
            }, 2000);
        });
    }, 1000);
  });
});
