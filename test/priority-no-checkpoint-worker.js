var fs = require('fs');

module.exports = function (config) {
  var api = {};

  fs.writeFileSync(config.tmpfile, '');

  api.ignored = function (doc) {
    return doc._id[0] === '_';
  };

  api.migrated = function (doc) {
    return doc.migrated;
  };

  api.migrate = function (doc, callback) {
    fs.writeFileSync(config.tmpfile,
      fs.readFileSync(config.tmpfile) + doc._id + '\n'
    );
    doc.migrated = true;
    setTimeout(function () {
      return callback(null, doc);
    }, 2000);
  };

  return api;
};
