var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var fs = require('fs');


test('skip change events for docs with in-progress migrations', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    concurrency: 5,
    tmpfile: __dirname + '/test-skip-in-progress.tmp'
  };

  var tmpworker = createWorker(__dirname + '/test-skip-in-progress-worker.js');
  var getMigrateCalls = function () {
    var lines = fs.readFileSync(config.tmpfile).toString().split('\n');
    lines.pop();
    return lines;
  };

  var worker = tmpworker.start(config);
  var doc = {_id: 'testdoc', foo: 'bar'};

  var url = test.COUCH_URL + '/example/testdoc';

  couchr.put(url, doc).apply(function (res) {
    doc.asdf = 'asdf';
    doc._rev = res.body.rev;
    setTimeout(function () {
      couchr.put(url, doc).apply(function (res) {
        setTimeout(function () {
          couchr.get(url, {conflicts: true}).apply(function (res) {
            var newdoc = res.body;
            t.ok(!newdoc.asdf, 'asdf not set');
            t.equal(newdoc._rev.substr(0, 2), '2-', '_rev should only be two');
            t.equal(
              newdoc._conflicts && newdoc._conflicts.length, 1,
              'there should be 1 conflict'
            );
            t.equal(getMigrateCalls().length, 1, 'one call to migrate function');
            worker.stop();
            t.end();
          });
        }, 6000);
      });
    }, 500);
  });
});
