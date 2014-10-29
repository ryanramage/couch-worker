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
      fs.readFileSync(config.tmpfile) + doc._rev + '\n'
    );
    setTimeout(function () {
      //delete doc._rev;
      doc.migrated = true;
      return callback(null, doc);
    }, 2000);
  };

  return api;
};
