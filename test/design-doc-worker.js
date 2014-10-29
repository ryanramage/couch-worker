module.exports = function (config) {
  var api = {};

  api.ignored = function (doc) {
    return doc._id[0] === '_' || doc.ignored;
  };

  api.migrated = function (doc) {
    return doc.migrated;
  };

  api.migrate = function (doc, callback) {
    // never return from migrate call, so we can inspect vievs
    // doc.migrated = true;
    //return callback(null, doc);
  };

  return api;
};
