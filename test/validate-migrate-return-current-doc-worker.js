module.exports = function (config) {
    var api = {};

    api.ignored = function (doc) {
      return doc._id[0] === '_';
    };

    api.migrated = function (doc) {
      return doc.migrated;
    };

    api.migrate = function (doc, callback) {
      callback(null, [{_id: 'otherdoc', foo: 'bar'}]);
    };

    return api;
};
