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

exports.start = _.curry(function (worker, config) {
  // checks for required properties on config object
  config = exports.readConfig(config);

  // initialize worker
  var w = exports.loadWorker(worker, config);

  // start listening to changes feed
  var opts = config.follow || {};
  opts.include_docs = true;
  var changes = couchr.changes(config.database, opts);

  // find un-migrated docs
  var dirty = changes.pluck('doc')
    .reject(w.ignored)
    .reject(w.migrated);

  // force migrate function to return a stream
  var migrate = _.wrapCallback(w.migrate);

  // migrate dirty docs
  var updated = dirty
      .doto(exports.logMigrating(config))
      .map(migrate)
      .parallel(config.concurrency);

  // write updates to couchdb
  var writes = updated.flatMap(exports.writeBatch(config.database));

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
 * Writes a batch to couchdb with updates from a migration
 */

exports.writeBatch = _.curry(function (database, doc) {
    return couchr.post(database + '/_bulk_docs', {
        all_or_nothing: true, // write conflicts to db
        docs: [doc]
    });
});

/**
 * Checks config object for required properties and sets defaults
 */

exports.readConfig = function (config) {
    if (typeof config.name !== 'string') {
        throw new Error('Expected config.name to be a string');
    }
    if (typeof config.database !== 'string') {
        throw new Error('Expected config.database to be a URL');
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
 * Outputs log info for documents about to be migrated
 */

exports.logMigrating = _.curry(function (config, doc) {
    console.log(
      '[' + config.name + '] Migrating ' + doc._id + ' rev:' + doc._rev
    );
});

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
