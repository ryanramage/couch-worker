var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('./harness');


test('include _conflicts in documents provided to workers', function (t) {
    t.plan(4);
    // worker variable populated later, defined here for use inside api.migrate
    var worker;

    var conflictworker = createWorker(function (config) {
        var api = {};
        api.ignored = function (doc) {
            t.equal(
                doc._conflicts && doc._conflicts.length, 1,
                'should have one _conflict revision'
            );
            return false;
        };
        api.migrated = function (doc) {
            t.equal(
                doc._conflicts && doc._conflicts.length, 1,
                'should have one _conflict revision'
            );
            return false;
        };
        api.migrate = function (doc, callback) {
            t.equal(
                doc._conflicts && doc._conflicts.length, 1,
                'should have one _conflict revision'
            );
            worker.stop();
            t.end();
        };
        return api;
    });

    var config = {
      name: 'couch-worker-example',
      database: test.COUCH_URL + '/example',
      log_database: test.COUCH_URL + '/errors'
    };

    var a = {_id: 'testdoc', a: 1};
    var b = {_id: 'testdoc', b: 2};

    couchr.put(config.database + '/testdoc', a).apply(function (res) {
        // start listening to changes
        worker = conflictworker.start(config);
        // create a conflicting revision
        var opt = {all_or_nothing: true, docs: [b]};
        couchr.post(config.database + '/_bulk_docs', opt).apply(function (res) {
            t.ok(res.body[0].ok, 'put conflicting revision');
        });
    });
});
