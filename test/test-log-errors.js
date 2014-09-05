var createWorker = require('../index').createWorker;
var couchr = require('highland-couchr');
var test = require('./harness');


test('log errors to separate db', function (t) {
  var config = {
    name: 'couch-worker-example',
    database: test.COUCH_URL + '/example',
    log_database: test.COUCH_URL + '/errors'
  };

  var tmpworker = createWorker(function (config) {
    var api = {};
    api.ignored = function (doc) {
      return false;
    };
    api.migrated = function (doc) {
      return doc.migrated;
    };
    api.migrate = function (doc, callback) {
      doc.migrated = true;
      var e = new Error('Fail!');
      e.stack = '<stacktrace>';
      e.custom = 123;
      return callback(e);
    };
    return api;
  });

  var os = require('os');
  var _hostname = os.hostname;
  var _platform = os.platform;
  var _arch = os.arch;
  var _networkInterfaces = os.networkInterfaces;
  var _version = process.version;
  os.hostname = function () { return 'fakehostname'; };
  os.platform = function () { return 'linux'; };
  os.arch = function () { return 'ia32'; };
  os.networkInterfaces = function () {
    return {
      'lo': [
        {
          address: '127.0.0.1',
          family: 'IPv4',
          internal: true
        },
        {
          address: '::1',
          family: 'IPv6',
          internal: true
        }
      ],
      'wlan0': [
        {
          address: '10.1.4.133',
          family: 'IPv4',
          internal: false
        },
        {
          address: 'fe80::221:6aff:fe41:a8d6',
          family: 'IPv6',
          internal: false
        }
      ]
    }
  };
  process.version = 'v0.10.30';

  var w = tmpworker.start(config);
  var doc = {_id: 'a'};
  couchr.post(test.COUCH_URL + '/example', doc).apply(function (res) {
    doc._rev = res.body.rev;
    setTimeout(function () {
      var logurl = test.COUCH_URL + '/errors/_all_docs';
      couchr.get(logurl, {include_docs: true}).apply(function (res) {
        var rows = res.body.rows;
        t.equal(rows.length, 1);
        var logdoc = rows[0].doc;
        delete logdoc._rev;
        delete logdoc._id;
        t.ok(logdoc.time, 'log doc has time property');
        var timediff = Math.abs(
          new Date(logdoc.time).getTime() - new Date().getTime()
        );
        t.ok(timediff < 1000*60*60*24, 'error logged some time today');
        // delete time from doc for easier comparison
        delete logdoc.time;
        t.deepEqual(logdoc, {
          worker: {
            name: 'couch-worker-example',
            hostname: 'fakehostname',
            platform: 'linux',
            node_version: 'v0.10.30',
            arch: 'ia32',
            addresses: [
              '10.1.4.133',
              'fe80::221:6aff:fe41:a8d6'
            ]
          },
          //time: '2009-02-13T23:31:30.123Z',
          database: test.COUCH_URL + '/example',
          seq: 1,
          error: {
            message: 'Fail!',
            stack: '<stacktrace>',
            custom: 123
          },
          doc: doc
        });
        os.hostname = _hostname;
        os.platform = _platform;
        os.arch = _arch;
        os.networkInterfaces = _networkInterfaces;
        process.version = _version;
        w.stop();
        t.end();
      });
    }, 2000);
  });

});
