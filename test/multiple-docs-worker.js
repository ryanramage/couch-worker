module.exports = function (config) {
  var api = {};

  api.ignored = function (doc) {
    return (
      doc._id[0] === '_' ||
      !doc.hasOwnProperty('a') ||
      !doc.hasOwnProperty('b')
    );
  };

  api.migrated = function (doc) {
    return doc.split;
  };

  api.migrate = function (doc, callback) {
    doc.split = true;
    return callback(null, [
      {_id: 'a', a: doc.a},
      {_id: 'b', b: doc.b},
      doc
    ]);
  };

  return api;
};
