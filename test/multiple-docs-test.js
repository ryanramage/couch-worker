var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');


test('return multiple docs from worker', function (t) {
    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors'
    };

    var tmpworker = createWorker(__dirname + '/multiple-docs-worker.js');
    var w = tmpworker.start(config);

    var notmigrated_doc = require('./fixtures/notmigrated.json').doc;
    delete notmigrated_doc._rev;

    var url = test.COUCH_URL + '/example/' + notmigrated_doc._id;

    // TODO: move this to highland.js
    function delay(ms) {
      return _(function (push) {
        setTimeout(function () { push(null, _.nil); }, 2000);
      });
    }

    var tasks = _([
      couchr.put(url, notmigrated_doc),
      delay(2000),
      couchr.get(url, {}),
      couchr.get(test.COUCH_URL + '/example/a', {}),
      couchr.get(test.COUCH_URL + '/example/b', {})
    ]);

    tasks.series().apply(function (put, original, a, b) {
      var doc = original.body;
      t.equal(doc._rev.substr(0, 2), '2-', '_rev should only be two');
      delete doc._rev;
      delete doc._id;
      t.deepEqual(doc, {
          a: notmigrated_doc.a,
          b: notmigrated_doc.b,
          split: true
      });
      // get other documents returned from migration step
      delete a.body._rev;
      t.deepEqual(a.body, {_id: 'a', a: notmigrated_doc.a});
      delete b.body._rev;
      t.deepEqual(b.body, {_id: 'b', b: notmigrated_doc.b});
      w.stop();
      t.end();
    });
});
