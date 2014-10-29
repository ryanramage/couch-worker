var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');


test('log errors to separate db', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors'
  };

  var tmpworker = createWorker(__dirname + '/log-processed-worker.js');

  var w = tmpworker.start(config);
  var doc = {_id: 'a'};
  couchr.post(test.COUCH_URL + '/example', doc).apply(function (res) {
    doc._rev = res.body.rev;
    setTimeout(function () {
      var logurl = test.COUCH_URL + '/errors/_all_docs';
      var q = {include_docs: true};
      couchr.get(logurl, q).apply(function (res) {
        var rows = res.body.rows;
        var logrow = rows.filter(function (x) {
          return x.doc.type === 'success';
        })[0];
        var logdoc = logrow.doc;
        delete logdoc._rev;
        delete logdoc._id;
        t.ok(logdoc.time, 'log doc has time property');
        var timediff = Math.abs(
          new Date(logdoc.time).getTime() - new Date().getTime()
        );
        t.ok(timediff < 1000*60*60*24, 'logged some time today');
        // delete time from doc for easier comparison
        delete logdoc.time;
        t.deepEqual(logdoc, {
          type: 'success',
          worker: 'couch-worker-example',
          //time: '2009-02-13T23:31:30.123Z',
          database: test.COUCH_URL_NO_AUTH + '/example',
          seq: 1,
          docid: doc._id
        });
        w.stop();
        t.end();
      });
    }, 2000);
  });

});
