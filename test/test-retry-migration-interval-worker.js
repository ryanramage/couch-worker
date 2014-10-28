var fs = require('fs');


module.exports = function (config) {
  var api = {};

  fs.writeFileSync(config.tmpfile, '');

  api.ignored = function (doc) {
    return doc._id[0] === '_' || doc.ignored;
  };

  api.migrated = function (doc) {
    return doc.migrated;
  };

  api.migrate = function (doc, callback) {
    fs.writeFileSync(config.tmpfile,
      fs.readFileSync(config.tmpfile).toString() +
      (new Date().getTime()) + '\n'
    );
    var lines = fs.readFileSync(config.tmpfile).toString().split('\n');
    var attempts = lines.length - 1;
    if (attempts === 2) {
      setTimeout(function () {
        doc.migrated = true;
        return callback(null, doc);
      }, 500);
    }
    else {
      //setTimeout(function () {
      //  t.equal(attempts, 1, 'no new attempt until this one returns');
      //}, 750);
      setTimeout(function () {
        return callback(new Error('not yet'));
      }, 500);
    }
  };

  return api;
};
