module.exports = function (config) {
  var api = {};

  api.ignored = function (doc) {
    return doc._id[0] === '_' || doc.ignored;
  };

  api.migrated = function (doc) {
    return doc.migrated;
  };

  api.migrate = function (doc, callback) {
    doc.migrated = false;
    doc.ignored = true;
    callback(null, doc);
  };

  return api;
};
