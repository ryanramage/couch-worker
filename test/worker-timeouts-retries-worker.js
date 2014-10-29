var migrate_calls = 0;


module.exports = function (config) {
  var api = {};

  api.ignored = function (doc) {
    return doc._id[0] === '_';
  };

  api.migrated = function (doc) {
    return doc.migrated;
  };

  api.migrate = function (doc, callback) {
    migrate_calls++;
    doc.migrated = migrate_calls;
    if (migrate_calls === 3) {
      return callback(null, doc);
    }
    else {
      setTimeout(function () {
        return callback(null, doc);
      }, 1500);
    }
  };

  return api;
};
