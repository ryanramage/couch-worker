var path = process.argv[2];
var worker;

console.log('Loading module: ' + path);
var m = require(path);
if (typeof m !== 'function') {
    throw new Error('Worker module should expose a function as module.exports');
}

exports.getSourceDoc = function (id, result) {
  var r = (Array.isArray(result) ? result: [result]);
  for (var i = 0; i < r.length; i++) {
    if (r[i]._id === id) {
      return r[i];
    }
  }
  return null;
};

process.on('message', function (msg) {
  if (msg.type === 'init') {
    worker = m(msg.data);
    function required(prop, type) {
        if (!worker[prop] || typeof worker[prop] !== type) {
            throw new Error(
                'Worker should expose "' + prop + '" property as ' + type
            );
        }
    }
    required('ignored', 'function');
    required('migrated', 'function');
    required('migrate', 'function');
  }
  else if (msg.type === 'ignored') {
    process.send({id: msg.id, result: worker.ignored(msg.data)});
  }
  else if (msg.type === 'migrated') {
    process.send({id: msg.id, result: worker.migrated(msg.data)});
  }
  else if (msg.type === 'migrate') {
    worker.migrate(msg.data, function (err, result) {
      var e = null;
      if (err) {
        e = JSON.parse(JSON.stringify(err));
        // useful properties not serialized by json.stringify:
        e.message = err.message;
        e.stack = err.stack;
      }
      else {
        var source_doc = exports.getSourceDoc(msg.data._id, result);
        // check the original document will be returned
        if (!source_doc) {
          e = {message: 'Migrate function did not return original document'};
        }
        // check we match either the migrated or ignored predicate
        else if (!worker.migrated(source_doc) && !worker.ignored(source_doc)) {
          e = {message: 'Migrate result did not match migrated or ignored predicates'};
        }
      }
      process.send({id: msg.id, error: e, result: result});
    });
  }
});
