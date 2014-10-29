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
    fs.writeFileSync(config.tmpfile,
      (Number(fs.readFileSync(config.tmpfile).toString()) + 1).toString()
    );
    return callback(new Error('not yet'));
  };

  return api;
};
