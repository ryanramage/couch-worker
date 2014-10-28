var fs = require('fs');


module.exports = function (config) {
  var api = {};

  api.ignored = function (doc) {
    return doc._id[0] === '_';
  };

  api.migrated = function (doc) {
    return doc.migrated2;
  };

  api.migrate = function (doc, callback) {
    fs.writeFileSync(config.tmpfile,
      fs.readFileSync(config.tmpfile).toString() + doc._id + '\n'
    );
    doc.migrated2 = true;
    return callback(null, doc);
  };

  return api;
};
