var couchr = require('highland-couchr');
var moment = require('moment');
var os = require('os');
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
  // worker api
  var api = {
    stopped: false,
    stop: function (cb) {
      w.stopped = true;
      if (cb) {
        return cb();
      }
    }
  };
  // checks for required properties on config object
  config = exports.readConfig(config);

  // make sure the log database exists
  exports.ensureDB(config.log_database).apply(function (res) {

    // get the checkpoint document from couchdb
    exports.getCheckpoint(config).apply(function (checkpoint) {
      if (api.stopped) {
        // worker already called stop(), don't start listening
        return;
      }
      // store checkpoint in config for use when putting batches
      config.checkpoint_rev = checkpoint._rev;

      // initialize worker
      var w = exports.loadWorker(worker, config);

      // start listening to changes feed
      var changes = exports.listen(checkpoint.seq, config);

      // update worker stop() function to stop changes feed
      api.stop = function (cb) {
        api.stopped = true;
        changes.stop(cb);
      };

      // create a stream of migration events for docs that are not ignored
      // and failed the migrated test provided by the worker
      var updated = exports.process(w, config, changes)
        .parallel(config.concurrency);

      // write updates to couchdb
      var writes = updated
        .flatMap(exports.writeBatch(config))
        .doto(exports.removeInProgress(config));

      // output results
      writes
        .errors(exports.logError(config))
        .each(exports.logBatch(config));

    });

  });
  // return worker object
  return api;
});

/**
 * Makes sure the database at the given location exists
 */

exports.ensureDB = function (url) {
  return couchr.get(url, {}).consume(function (err, x, push, next) {
    if (err) {
      if (err.error === 'not_found') {
        next(couchr.put(url, {}));
      }
      else {
        push(err);
        next();
      }
    }
    if (x === _.nil) {
      push(null, _.nil);
    }
    else {
      push(null, x);
      next();
    }
  });
};

/**
 * Process a changes stream returning a stream of update arrays
 */

exports.process = function (worker, config, changes) {
  // force migrate function to return a stream
  var f = _.wrapCallback(worker.migrate);

  // create a stream of migration events
  var migrations = changes.map(function (change) {
    return {
      original: change.doc,
      seq: change.seq
    };
  });

  // keep a list of in-progress migrations so we can avoid
  // migrating multiple revisions of the same document in parallel
  config.inprogress = [];

  // return a stream of updates (emits an empty array if nothing to do
  // for this doc) an array of doc updates otherwise
  return migrations.map(function (migration) {
    if (worker.ignored(migration.original)) {
      // doc ignored, do nothing but update checkpoint
      migration.result = [];
      return _([migration]);
    }
    else if (worker.migrated(migration.original)) {
      // doc already migrated, do nothing but update checkpoint
      migration.result = [];
      return _([migration]);
    }
    else if (exports.inProgress(config, migration)) {
      // doc already being migrated (due to earlier change event), skip seq id
      migration.result = [];
      return _([migration]);
    }
    else {
      // migrate document
      // console
      exports.addInProgress(config, migration);
      exports.logMigrating(config, migration);
      var result = exports.migrate(f, migration);
      return result.consume(function (err, x, push, next) {
        if (err) {
          return next(
            // write to log database
            exports.writeErrorLog(config, err, migration).map(function (res) {
              // write checkpoint back to source db so we can continue
              // processing changes
              migration.result = [];
              return migration;
            })
          );
        }
        // end of data
        else if (x === _.nil) {
          push(null, _.nil);
        }
        else {
          var source_doc = exports.getSourceDoc(migration, x.result);
          // check if we got the original document back
          if (!source_doc) {
            var e = new Error(
              'Migrate function did not return original document'
            );
            return next(
              // write to log database
              exports.writeErrorLog(config, e, migration).map(function (res) {
                // write checkpoint back to source db so we can continue
                // processing changes
                migration.result = [];
                return migration;
              })
            );
          }
          else if (!worker.migrated(source_doc) && !worker.ignored(source_doc)) {
            var e2 = new Error(
              'Migrate result did not match migrated or ignored predicates'
            );
            return next(
              // write to log database
              exports.writeErrorLog(config, e2, migration).map(function (res) {
                // write checkpoint back to source db so we can continue
                // processing changes
                migration.result = [];
                return migration;
              })
            );
          }
          else {
            push(null, x);
            next();
          }
        }
      });
    }
  });
};

/**
 * Looks for the original document in the results of the migrate function
 * call, and returns it. Returns null if not found.
 */

exports.getSourceDoc = function (migration, result) {
  var r = (Array.isArray(result) ? result: [result]);
  for (var i = 0; i < r.length; i++) {
    if (r[i]._id === migration.original._id) {
      return r[i];
    }
  }
  return null;
};

/**
 * Writes an error message to the log database, including information
 * about the error, time, system, original document, source database etc.
 */

exports.writeErrorLog = function (config, err, migration) {
  var logdoc = {
    worker: {
      name: config.name,
      hostname: os.hostname(),
      platform: os.platform(),
      node_version: process.version,
      arch: os.arch(),
      addresses: exports.getAddresses()
    },
    error: exports.errorToJSON(err),
    time: moment().toISOString(),
    database: config.database,
    seq: migration.seq,
    doc: migration.original
  };
  console.error(
    'ERROR: ' + logdoc.error.message +
    '\n  for document: ' + logdoc.doc._id + ' rev:' + logdoc.doc._rev
  );
  return couchr.post(config.log_database, logdoc);
};

/**
 * Converts an error object to a JSON-compatible representation
 */

exports.errorToJSON = function (err) {
  var e = {};
  if (err.message) {
    e.message = err.message;
  }
  if (err.stack) {
    e.stack = err.stack;
  }
  for (var k in err) {
    if (err.hasOwnProperty(k)) {
      var val = err[k];
      // make sure we can serialize it (JSON.stringify will
      // return undefined if not)
      if (JSON.stringify(val)) {
        e[k] = val;
      }
    }
  }
  return e;
};

exports.getAddresses = function () {
  var interfaces = os.networkInterfaces();
  var results = [];
  for (var k in interfaces) {
    var xs = interfaces[k];
    for (var i = 0; i < xs.length; i++) {
      var x = xs[i];
      if (!x.internal) {
        results.push(x.address);
      }
    }
  }
  return results;
};

/**
 * Find last processed sequence id from checkpoint document, or return 0
 */

exports.getCheckpoint = function (config) {
  return _(function (push, next) {
    // where to store checkpoint / sequence id during processing
    var checkpoint_url = config.database +
      '/_local/' + encodeURIComponent(config.name);

    var errored = false;
    couchr.get(checkpoint_url, {})
      .stopOnError(function (err, rethrow) {
        errored = true;
        if (err.error === 'not_found') {
          push(null, {seq: 0});
          push(null, _.nil);
        }
        else {
          rethrow(err);
        }
      })
      .apply(function (res) {
        if (!errored) {
          // TODO: add a version check here?
          // so it restarts from 0 if we've changed the worker version
          push(null, res.body);
          push(null, _.nil);
        }
      });
  });
};

/**
 * Returns a stream of migration events filtered to exclude ignored and
 * already-migrated docs for the worker.
 */

exports.getDirty = function (worker, changes) {
  return changes
    .reject(function (change) {
      return worker.ignored(change.doc);
    })
    .reject(function (change) {
      return worker.migrated(change.doc);
    })
    .map(function (change) {
      return {
        original: change.doc,
        seq: change.seq
      };
    });
};

/**
 * Starts listening to the CouchDB changes feed, returns a
 * stream of change events
 */

exports.listen = function (since, config) {
  var opts = config.follow || {};
  opts.include_docs = true;
  opts.conflicts = true;
  opts.since = since;
  // TODO: add test for overriding since in config
  if (config.follow && config.follow.since) {
    opts.since = config.follow.since;
  }
  return couchr.changes(config.database, opts);
};

/**
 * Applies a migration function (which should return a stream) to a change
 * object and updates the change object with the result.
 */

exports.migrate = _.curry(function (f, migration) {
  var doc = exports.cloneJSON(migration.original);
  return f(doc).map(function (result) {
    migration.result = result;
    return migration;
  });
});

/**
 * Performs a deep clone of a JSON compatible object
 */

exports.cloneJSON = function (doc) {
  return JSON.parse(JSON.stringify(doc));
};

/**
 * Writes a batch to couchdb with updates from a migration
 */

exports.writeBatch = _.curry(function (config, migration) {
    var result = migration.result;
    migration.writes = Array.isArray(result) ? result : [result];
    var checkpoint = {
      _id: '_local/' + config.name,
      seq: migration.seq
    };
    if (config.checkpoint_rev) {
      checkpoint._rev = config.checkpoint_rev;
    }
    var docs = migration.writes.concat([checkpoint]);
    var batch = couchr.post(config.database + '/_bulk_docs', {
        all_or_nothing: true, // write conflicts to db
        docs: docs
    });
    // return original migration on success
    return batch.map(function (res) {
      migration.response = res.body;
      // checkpoint is last doc we sent
      config.checkpoint_rev = res.body[res.body.length - 1].rev;
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
    if (typeof config.log_database !== 'string') {
        throw new Error('Expected config.log_database to be a URL');
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

exports.inProgress = _.curry(function (config, migration) {
  return config.inprogress.indexOf(migration.original._id) !== -1;
});

/**
 * Adds a document to the in-progress list - mutates the original array!
 */

exports.addInProgress = _.curry(function (config, migration) {
  config.inprogress.push(migration.original._id);
});

/**
 * Removes a document from the in-progress list - mutates the original array!
 */

exports.removeInProgress = _.curry(function (config, migration) {
  var i;
  while ((i = config.inprogress.indexOf(migration.original._id)) !== -1) {
    config.inprogress.splice(i, 1);
  }
});
