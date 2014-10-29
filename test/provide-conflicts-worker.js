var fs = require('fs');


module.exports = function (config) {
  var api = {};

  fs.writeFileSync(config.tmpfile, '0');

  api.ignored = function (doc) {
    return false;
  };

  api.migrated = function (doc) {
    return false;
  };

  api.migrate = function (doc, callback) {
    if (doc._conflicts) {
      fs.writeFileSync(config.tmpfile, doc._conflicts.length.toString());
    }
  };

  return api;
};
