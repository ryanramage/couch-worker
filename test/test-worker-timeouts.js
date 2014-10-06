var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');


test('log timeout errors', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    timeout: 1000
  };

  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return doc._id[0] === '_';
    };
    api.migrated = function (doc) {
      return doc.migrated;
    };
    api.migrate = function (doc, callback) {
      doc.migrated = true;
      setTimeout(function () {
        return callback(null, doc);
      }, 2000);
    };
    return api;
  });

  var w = tmpworker.start(config);
  var doc = {_id: 'a'};
  couchr.post(test.COUCH_URL + '/example', doc).apply(function (res) {
    doc._rev = res.body.rev;
    setTimeout(function () {
      var logurl = test.COUCH_URL + '/errors/_all_docs';
      var q = {include_docs: true};
      couchr.get(logurl, q).apply(function (res) {
        var rows = res.body.rows;
        t.equal(rows.length, 2);
        var logrow = rows.filter(function (x) {
          return x.doc.type === 'error';
        })[0];
        var logdoc = logrow.doc;
        delete logdoc._rev;
        delete logdoc._id;
        t.ok(/timed out/i.test(logdoc.error.message), 'timeout error logged');
        w.stop();
        t.end();
      });
    }, 2000);
  });

});
