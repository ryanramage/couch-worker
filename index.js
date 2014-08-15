var couchr = require('highland-couchr');
var _ = require('highland');


/**
 * Use this to create a worker instance with a .start() method that listens
 * to CouchDB for changes. Your worker should be a function which accepts a
 * config object which returns an object with ignored, migrated and migrate
 * properties as functions.
 */

exports.createWorker = function (worker) {
    return {start: exports.start(worker)};
};

/**
 * Load worker and start listening to CouchDB changes.
 */

exports.start = _.curry(function (worker, db_url, config) {
  // checks for required properties on config object
  config = exports.readConfig(config);

  // initialize worker
  var w = exports.loadWorker(worker, config);

  // start listening to changes feed
  var opts = config.follow || {};
  opts.include_docs = true;
  var changes = couchr.changes(db_url, opts);

  // find un-migrated docs
  var dirty = changes.pluck('doc')
    .reject(w.ignored)
    .reject(w.migrated);

  // migrate dirty docs
  var migrate = _.wrapCallback(w.migrate);
  var updated = dirty.map(migrate).parallel(config.concurrency);

  // write updates to couchdb
  var writes = updated.flatMap(couchr.post(db_url + '/'));

  // output results
  writes
    .pluck('body')
    .errors(exports.logError(config))
    .each(exports.logPut(config));

  return {
      stop: changes.stop.bind('changes')
  };
});

/**
 * Checks config object for required properties and sets defaults
 */

exports.readConfig = function (config) {
    if (typeof config.name !== 'string') {
        throw new Error('Expected config.name to be a string');
    }
    // default to processing 4 docs at once
    config.concurrency = config.concurrency || 4;
    return config;
};

/**
 * Initializes worker and checks that it exports the required properties
 */

exports.loadWorker = function (worker, config) {
    if (typeof worker !== 'function') {
        throw new Error('Worker should expose a function as module.exports');
    }
    var w = worker(config);
    function required(prop, type) {
        if (!w[prop] || typeof w[prop] !== type) {
            throw new Error(
                'Worker should expose "' + prop + '" property as ' + type
            );
        }
    }
    required('ignored', 'function');
    required('migrated', 'function');
    required('migrate', 'function');
    return w;
};

/**
 * Outputs errors to console
 */

exports.logError = _.curry(function (config, err) {
    console.error('[' + config.name + '] ' + err.stack);
});

/**
 * Outputs PUTs to console
 */

exports.logPut = _.curry(function (config, res) {
    console.log('[' + config.name + '] PUT ' + res.id + ' rev:' + res.rev);
});
