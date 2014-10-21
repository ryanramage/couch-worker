var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('couch-worker-test-harness');
var _ = require('highland');


test('resume changes processing from last processed seq id', function (t) {
  t.plan(2);

  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors',
    checkpoint_size: 1,
    concurrency: 1
  };

  // extracted here so we can modify after creating a worker
  //var predicate = function (doc) {
  //  return doc.migrated;
  //};
  //var migrate = function (doc) {
  //  doc.migrated = true;
  //  return doc;
  //};

  var migrate_calls = [];
  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return doc._id[0] === '_';
    };
    api.migrated = function (doc) {
      return doc.migrated;
    };
    api.migrate = function (doc, callback) {
      migrate_calls.push(doc._id);
      doc.migrated = true;
      return callback(null, doc);
    };
    return api;
  });
  var tmpworker2 = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return doc._id[0] === '_';
    };
    api.migrated = function (doc) {
      return doc.migrated2;
    };
    api.migrate = function (doc, callback) {
      migrate_calls.push(doc._id);
      doc.migrated2 = true;
      return callback(null, doc);
    };
    return api;
  });

  var url = test.COUCH_URL + '/example';

  var delay = function () {
    return _(function (push, next) {
      setTimeout(function () { push(null, _.nil); }, 1000);
    });
  };

  var tasksA = _([
    couchr.post(url, {_id: 'a'}),
    delay(),
    couchr.post(url, {_id: 'b'}),
    delay(),
    couchr.post(url, {_id: 'c'}),
    delay()
  ]);

  var tasksB = _([
    couchr.post(url, {_id: 'd'}),
    couchr.post(url, {_id: 'e'}),
    couchr.post(url, {_id: 'f'})
  ]);

  var w = tmpworker.start(config);

  tasksA.series().apply(function (a, b, c) {
    setTimeout(function () {
      t.deepEqual(migrate_calls, ['a','b','c']);
      // stop listening to changes
      w.stop(function () {
        // add some more docs
        tasksB.series().apply(function (d, e, f) {
          // resume listening to changes
          var w2 = tmpworker2.start(config);
          setTimeout(function () {
            // check we didn't repeat 'migrated' checks for a and b,
            // c will be repeated since it's updated seq id will have been
            // filtered out of changes feed for this worker
            // (since it's already migrated)
            t.deepEqual(
              migrate_calls, ['a','b','c','c','d','e','f'],
              'no migrations repeated'
            );
            w2.stop();
            t.end();
          }, 2000);
        });
      });
    }, 2000);
  });

});
