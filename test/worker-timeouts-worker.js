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
    setTimeout(function () {
      return callback(null, doc);
    }, 2000);
  };

  return api;
};
