var fs = require('fs');


module.exports = function (config) {
  var api = {};

  fs.writeFileSync(config.tmpfile, '0');

  api.ignored = function (doc) {
    return doc._id[0] === '_' || doc.ignored;
  };

  api.migrated = function (doc) {
    return doc.migrated;
  };

  api.migrate = function (doc, callback) {
    var attempts = Number(fs.readFileSync(config.tmpfile).toString()) + 1;
    fs.writeFileSync(config.tmpfile, attempts.toString());
    if (attempts === 2) {
      doc.migrated = true;
      return callback(null, doc);
    }
    else {
      return callback(new Error('not yet'));
    }
  };

  return api;
};
