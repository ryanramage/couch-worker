var path = process.argv[2];
var worker;

console.log('Loading module: ' + path);
var m = require(path);
if (typeof m !== 'function') {
    throw new Error('Worker module should expose a function as module.exports');
}

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
    process.send({result: worker.ignored(msg.data)});
  }
  else if (msg.type === 'migrated') {
    process.send({result: worker.migrated(msg.data)});
  }
  else if (msg.type === 'migrate') {
    worker.migrate(msg.data, function (err, result) {
      process.send({error: err, result: result});
    });
  }
});
