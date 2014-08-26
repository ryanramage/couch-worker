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

  // force migrate function to return a stream
  var f = _.wrapCallback(w.migrate);

  // start listening to changes feed
  var changes = exports.listen(config);

  // create a stream of migration events for docs that are not ignored
  // and failed the migrated test provided by the worker
  var dirty = exports.getDirty(w, changes);

  // keep a list of in-progress migrations so we can avoid
  // migrating multiple revisions of the same document in parallel
  var inprogress = [];

  // migrate dirty docs
  var updated = dirty
      .reject(exports.inProgress(inprogress))
      .doto(exports.addInProgress(inprogress))
      .doto(exports.logMigrating(config))
      .map(exports.migrate(f))
      .parallel(config.concurrency);

  // write updates to couchdb
  var writes = updated
    .flatMap(exports.writeBatch(config.database))
    .doto(exports.removeInProgress(inprogress));

  // output results
  writes
    .errors(exports.logError(config))
    .each(exports.logBatch(config));

  return {
      stop: changes.stop.bind('changes')
  };
});

/**
 * Returns a stream of migration events filtered to exclude ignored and
 * already-migrated docs for the worker.
 */

exports.getDirty = function (worker, changes) {
  return changes.pluck('doc')
    .reject(worker.ignored)
    .reject(worker.migrated)
    .map(function (doc) {
      return {original: doc};
    });
};

/**
 * Starts listening to the CouchDB changes feed, returns a
 * stream of change events
 */

exports.listen = function (config) {
  var opts = config.follow || {};
  opts.include_docs = true;
  opts.conflicts = true;
  return couchr.changes(config.database, opts);
};

/**
 * Applies a migration function (which should return a stream) to a change
 * object and updates the change object with the result.
 */

exports.migrate = _.curry(function (f, change) {
  return f(change.original).map(function (result) {
    change.result = result;
    return change;
  });
});

/**
 * Writes a batch to couchdb with updates from a migration
 */

exports.writeBatch = _.curry(function (database, migration) {
    var result = migration.result;
    migration.writes = Array.isArray(result) ? result : [result];
    var batch = couchr.post(database + '/_bulk_docs', {
        all_or_nothing: true, // write conflicts to db
        docs: migration.writes
    });
    // return original migration on success
    return batch.map(function (res) {
      migration.response = res.body;
      return migration;
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

exports.logMigrating = _.curry(function (config, migration) {
    var doc = migration.original;
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
 * Outputs batch PUTs to console
 */

exports.logBatch = _.curry(function (config, migration) {
    var writes = migration.writes.map(function (doc) {
        return doc._id + (doc._rev ? ' rev:' + doc._rev: '');
    });
    console.log('[' + config.name + '] Written:\n  ' + writes.join('\n  '));
});

/**
 * Returns true for documents that have a migration running
 */

exports.inProgress = _.curry(function (inprogress, migration) {
  return inprogress.indexOf(migration.original._id) !== -1;
});

/**
 * Adds a document to the in-progress list - mutates the original array!
 */

exports.addInProgress = _.curry(function (inprogress, migration) {
  inprogress.push(migration.original._id);
});

/**
 * Removes a document from the in-progress list - mutates the original array!
 */

exports.removeInProgress = _.curry(function (inprogress, migration) {
  var i;
  while ((i = inprogress.indexOf(migration.original._id)) !== -1) {
    inprogress.splice(i, 1);
  }
});
