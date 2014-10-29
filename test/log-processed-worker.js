module.exports = function (config) {
  var api = {};

  api.ignored = function (doc) {
    return doc._id[0] === '_';
  };

  api.migrated = function (doc) {
    return doc.migrated;
  };

  api.migrate = function (doc, callback) {
    doc.migrated = true;
    return callback(null, doc);
  };

  return api;
};
