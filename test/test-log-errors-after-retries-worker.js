var fs = require('fs');

module.exports = function (config) {
  var api = {};

  fs.writeFileSync(config.tmpfile, '0');

  api.ignored = function (doc) {
    return doc._id[0] === '_';
  };

  api.migrated = function (doc) {
    return doc.migrated;
  };

  api.migrate = function (doc, callback) {
    fs.writeFileSync(config.tmpfile,
      (Number(fs.readFileSync(config.tmpfile).toString()) + 1).toString()
    );
    setTimeout(function () {
      doc.migrated = true;
      var e = new Error('Fail!');
      e.stack = '<stacktrace>';
      e.custom = 123;
      return callback(e);
    }, 100);
  };

  return api;
};
