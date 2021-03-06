var child_process = require('child_process');
var couchr = require('highland-couchr');
var moment = require('moment');
var crypto = require('crypto');
var url = require('url');
var os = require('os');
var _ = require('highland');


/**
 * Use this to create a worker instance with a .start() method that listens
 * to CouchDB for changes. Your worker should be a function which accepts a
 * config object which returns an object with ignored, migrated and migrate
 * properties as functions.
 */

exports.createWorker = function (path) {
    return {start: exports.start(path)};
};

exports.makeWorker = function (path, config) {
  var callbacks = {};
  function fork() {
    return child_process.fork(
      __dirname + '/subprocess.js',
      [path],
      {silent: false}
    );
  }
  var sub = fork();
  var errlog = '';
  // sub.stderr.on('data', function (data) {
  //   var str = data.toString();
  //   console.log(str);
  //   errlog += str
  //   // limit errlog to 2000 chars
  //   errlog = errlog.slice(-2000);
  // });
  sub.on('close', function (code) {
    var cbs = callbacks;
    callbacks = {};
    var e = {message: 'Child process died', code: code};
    if (errlog.length) {
      e.stack = errlog;
    }
    for (var k in cbs) {
      cbs[k](e);
    }
    // restart sub process
    sub = fork();
  });
  sub.send({type: 'init', data: config});
  sub.on('message', function (msg) {
    var cb = callbacks[msg.id];
    delete callbacks[msg.id];
    cb(msg.error, msg.result);
  });

  var channel_errors = function(e){
    console.log('channel errors', e);
    sub.kill();
    console.log('exiting do to a problem child.');
    process.exit(1);
    //sub = fork();
  };

  return {
    stop: function (callback) {
      sub.removeAllListeners('close');
      if (callback) {
        sub.once('close', function (code) {
          callback();
        });
      }
      sub.kill();
    },
    ignored: function (doc, callback) {
      var id = 'ignored:' + doc._id;
      callbacks[id] = callback;
      try {
        sub.send({id: id, type: 'ignored', data: doc});
      } catch(e) {  channel_errors(e); }
    },
    migrated: function (doc, callback) {
      var id = 'migrated:' + doc._id;
      callbacks[id] = callback;
      try {
        sub.send({id: id, type: 'migrated', data: doc});
      } catch(e) {  channel_errors(e); }
    },
    migrate: function (doc, callback) {
      var id = 'migrate:' + doc._id;
      callbacks[id] = callback;
      try {
        sub.send({id: id, type: 'migrate', data: doc});
      } catch(e) {  channel_errors(e); }
    }
  };
};

/**
 * Load worker and start listening to CouchDB changes.
 */

exports.start = _.curry(function (path, config) {
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

  // initialize worker
  var w = exports.makeWorker(path, config);

  // make sure the log database exists and push design doc to target db
  exports.prepareDBs(config, path).apply(function () {

    // get the checkpoint document from couchdb
    exports.getCheckpoint(config).apply(function (checkpoint) {
      if (api.stopped) {
        // worker already called stop(), don't start listening
        return;
      }
      // store checkpoint in config for use when putting batches
      config.checkpoint_rev = checkpoint._rev;
      // reset checkpoint counter, when this hits checkpoint_size we send
      // a new _local doc in the write batch with the current seq id
      config.checkpoint_counter = 0;

      // start listening to changes feed
      var changes = exports.listen(checkpoint.seq, config);

      // announce worker has started
      exports.logStart(config, checkpoint.seq);

      // update worker stop() function to stop changes feed
      api.stop = function (cb) {
        api.stopped = true;
        changes.stop(function () {
          w.stop(cb);
        });
      };

      // create a stream of migration events for docs that are not ignored
      // and failed the migrated test provided by the worker
      var updated = exports.process(w, config, changes)
        .parallel(config.concurrency);

      // write updates to couchdb
      var writes = updated
        .map(exports.writeBatch(config)).parallel(config.concurrency)
        .map(exports.clearPriority(config)).parallel(config.concurrency)
        .map(exports.logSuccess(config)).parallel(config.concurrency)
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
 * Creates the log database if it doesn't exist, and sets up appropriate
 * design documents
 */

exports.prepareDBs = function (config, path) {
  return _([
    exports.ensureLogDB(config.log_database),
    exports.ensureDesignDoc(config, path),
    exports.writeStartupDoc(config)
  ])
  .series();
};

/**
 * Removes auth info from a URL
 */

exports.removeAuth = function (location) {
  var parsed = url.parse(location);
  delete parsed.auth;
  delete parsed.href;
  return url.format(parsed);
};

exports.writeStartupDoc = function (config) {
  var doc = {
    type: 'started',
    name: config.name,
    database: exports.removeAuth(config.database),
    time: moment().toISOString()
  };
  return couchr.post(config.log_database, doc);
};

exports.ddocId = function (config) {
  return '_design/worker:' + config.name;
};

/**
 * Make sure there's an up-to-date design doc in place for monitoring
 * progress of the worker
 */

exports.ensureDesignDoc = function (config, path) {
  var worker = require(path)(config);
  var ddoc = {
    _id: exports.ddocId(config),
    language: 'javascript',
    views: {
      ignored: {
        map: 'function (doc) {\n' +
          'if ((' + worker.ignored.toString() + '(doc))) emit(doc._id, 1);' +
          '}',
        reduce: '_count'
      },
      not_migrated: {
        map: 'function (doc) {\n' +
          'if (!(' + worker.ignored.toString() + '(doc)) && \n' +
            '!(' + worker.migrated.toString() + '(doc))) emit(doc._id, 1);' +
          '}',
        reduce: '_count'
      },
      migrated: {
        map: 'function (doc) {\n' +
          'if (!(' + worker.ignored.toString() + '(doc)) && \n' +
            '(' + worker.migrated.toString() + '(doc))) emit(doc._id, 1);' +
          '}',
        reduce: '_count'
      }
    }
  };
  var ddoc_url = config.database + '/' + ddoc._id;
  return couchr.get(ddoc_url, {}).consume(function (err, x, push, next) {
    if (err) {
      if (err.error === 'not_found') {
        next(couchr.put(ddoc_url, ddoc));
      }
      else {
        push(err);
        next();
      }
    }
    else if (x === _.nil) {
      push(null, _.nil);
    }
    else {
      // check if ddoc is up to date
      var _rev = x.body._rev;
      delete x.body._rev;
      if (JSON.stringify(x.body) !== JSON.stringify(ddoc)) {
        var newddoc = exports.cloneJSON(ddoc);
        newddoc._rev = _rev;
        next(couchr.put(ddoc_url, newddoc));
      }
      else {
        // all done, end the stream
        push(null, _.nil);
      }
    }
  });
};

/**
 * Makes sure the log database at the given location exists
 */

exports.ensureLogDB = function (url) {
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


exports.timeout = function (f, timeout) {
  return function () {
    var args = Array.prototype.slice.call(arguments);
    var cb = args.pop();
    var timed_out = false;
    var t = setTimeout(function () {
      timed_out = true;
      return cb(
        new Error('Timed out (' + timeout + 'ms)')
      );
    }, timeout);
    return f.apply(this, args.concat([function () {
      if (timed_out) {
        // do nothing with the result
        return;
      }
      clearTimeout(t);
      return cb.apply(this, arguments);
    }]));
  };
};

/**
 * Returns a hex digest for the md5 hash of a string
 */

exports.hash = function (str) {
  return crypto.createHash('md5').update(str).digest('hex');
};

/**
 * Checks if an id falls into a bucket start/end range
 */

exports.inBucket = function (bucket, id) {
  var hash = exports.hash(id);
  return (bucket.start ? hash >= bucket.start : true) &&
         (bucket.end ? hash < bucket.end : true);
};

exports.addStatus = function (worker, migrations) {
  return migrations
    .flatMap(function (migration) {
      return _.wrapCallback(worker.ignored)(migration.original)
        .map(function (result) {
          migration.ignored = result;
          return migration;
        });
    })
    .flatMap(function (migration) {
      return _.wrapCallback(worker.migrated)(migration.original)
        .map(function (result) {
          migration.migrated = result;
          return migration;
        });
    });
};

/**
 * Process a changes stream returning a stream of update arrays
 */

exports.process = function (worker, config, changes) {
  var f = worker.migrate;
  if (config.timeout) {
    f = exports.timeout(f, config.timeout);
  }
  // return a stream from migrate function
  f = _.wrapCallback(f);
  if (config.retry_attempts) {
    f = exports.retry(f, config.retry_attempts, config.retry_interval);
  }

  // create a stream of migration events
  var migrations = changes.compact().map(function (change) {
    return {
      priority: change.priority || null,
      original: change.doc,
      seq: change.seq
    };
  });

  var bucket = config.bucket;

  // keep a list of in-progress migrations so we can avoid
  // migrating multiple revisions of the same document in parallel
  config.inprogress = [];

  // return a stream of updates (emits an empty array if nothing to do
  // for this doc) an array of doc updates otherwise
  return exports.addStatus(worker, migrations).map(function (migration) {
    if (bucket && !exports.inBucket(bucket, migration.original._id)) {
      // doc is not in the workers bucket, do nothing but update checkpoint
      migration.result = [];
      migration.success = false;
      return _([migration]);
    }
    else if (migration.ignored) {
      // doc ignored, do nothing but update checkpoint
      migration.result = [];
      migration.success = false;
      return _([migration]);
    }
    else if (migration.migrated) {
      // doc already migrated, do nothing but update checkpoint
      migration.result = [];
      migration.success = false;
      return _([migration]);
    }
    else if (exports.inProgress(config, migration)) {
      // doc already being migrated (due to earlier change event), skip seq id
      migration.result = [];
      migration.success = false;
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
              migration.success = false;
              return migration;
            })
          );
        }
        // end of data
        else if (x === _.nil) {
          push(null, _.nil);
        }
        else {
          x.success = true;
          push(null, x);
          next();
        }
      });
    }
  });
};

exports.delayStart = function (src, delay) {
  return _(function (push, next) {
    setTimeout(next.bind(null, src), delay);
  });
};

exports.retry = function (f, attempts, interval) {
  return function () {
    var args = Array.prototype.slice.call(arguments);
    return f.apply(null, args).consume(function (err, x, push, next) {
      if (err) {
        if (attempts > 1) {
          // try again
          return next(
            exports.delayStart(
              exports.retry(f, attempts - 1, interval).apply(null, args),
              interval
            )
          );
        }
        else {
          push(err);
          next();
        }
      }
      else if (x === _.nil) {
        push(null, _.nil);
      }
      else {
        push(null, x);
        next();
      }
    });
  };
};

/**
 * Writes an error message to the log database, including information
 * about the error, time, system, original document, source database etc.
 */

exports.writeErrorLog = function (config, err, migration) {
  var logdoc = {
    type: 'error',
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
    database: exports.removeAuth(config.database),
    seq: migration.seq,
    doc: migration.original
  };
  console.error(
    '[' + config.name + '] ' +
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
 * Starts listening to the CouchDB changes feed, returns a
 * stream of change events
 */

exports.listen = function (since, config) {
  var s = _(function (push, next) {
    if (s.stopped) {
      return push(null, _.nil);
    }
    exports.hasPriorityView(config).errors(push).apply(function (hasview) {
      if (s.stopped) {
        return push(null, _.nil);
      }
      var priority = exports.getPriority(config);
      var changes = exports.getChanges(since, config);
      s.stop = function (callback) {
        changes.stop(function () {
          priority.stop(callback);
        });
      };
      return next(_([priority, changes]).merge());
    });
  });
  s.stopped = false;
  s.stop = function () {
    s.stopped = true;
  };
  return s;
};

exports.hasPriorityView = function (config, callback) {
  var url = config.log_database + '/_design/couch-worker-dashboard';
  return couchr.get(url, {})
    .errors(function (err, rethrow) {
      if (err.error !== 'not_found') {
        rethrow(err);
      }
    })
    .map(function (res) {
      return res.body.views.priority;
    });
};

exports.getPriority = function (config, hasview) {
  var opts = {
    include_docs: true,
    since: 0
  };
  if (hasview) {
    // use priority view from couch-worker-dashboard for more efficient
    // changes feed filtering if available
    opt.filter = '_view';
    opt.view = 'couch-worker-dashboard/priority';
  }
  // TODO: use a ddoc with filter
  var changes = couchr.changes(config.log_database, opts);
  var s = changes.filter(function (change) {
      return (
        change.doc.type === 'priority' &&
        change.doc.worker === config.name &&
        !change.doc._deleted
      );
    })
    .flatMap(function (change) {
      return couchr.get(config.database + '/' + change.doc.id, {
        local_seq: true,
        conflicts: true
      })
      .map(function (res) {
        var doc = res.body;
        return {
          priority: change.doc,       // priority doc
          id: doc._id,                // id of target doc
          seq: doc._local_seq,        // seq of target doc in target db
          doc: doc,                   // target doc
          changes: [{rev: doc._rev}]
        };
      });
    });
  s.stop = changes.stop;
  return s;
};

exports.getChanges = function (since, config) {
  var opts = config.follow || {};
  var ddoc_name = exports.ddocId(config).replace(/^_design\//, '');
  opts.view = encodeURIComponent(ddoc_name) + '/not_migrated';
  opts.filter = '_view';
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
    var batch;
    var posted_checkpoint = false;
    var result = migration.result;
    migration.writes = Array.isArray(result) ? result : [result];
    var checkpoint = {
      _id: '_local/' + config.name,
      seq: migration.seq
    };
    if (config.checkpoint_rev) {
      checkpoint._rev = config.checkpoint_rev;
    }
    var docs = migration.writes;
    // don't checkpoint priority migrations
    // (as they may jump ahead in sequence ids)
    if (!migration.priority) {
      config.checkpoint_counter++;
      if (config.checkpoint_counter >= config.checkpoint_size) {
        config.checkpoint_counter = 0;
        docs = docs.concat([checkpoint]);
        posted_checkpoint = true;
      }
    }
    if (docs.length) {
      batch = couchr.post(config.database + '/_bulk_docs', {
          all_or_nothing: true, // write conflicts to db
          docs: docs
      });
    }
    else {
      // nothing to write
      return _([migration]);
    }
    // return original migration on success
    return batch.map(function (res) {
      migration.response = res.body;
      if (posted_checkpoint) {
        // checkpoint is last doc we sent
        var checkpoint = res.body[res.body.length - 1];
        // this may conflict if we have high concurrency, but we
        // don't really care so long as some checkpoints get through,
        // _local docs don't keep around conflicting revisions anyway
        if (checkpoint.ok) {
          config.checkpoint_rev = res.body[res.body.length - 1].rev;
          exports.logCheckpoint(config, migration.seq);
        }
      }
      return migration;
    });
});

exports.clearPriority = _.curry(function (config, migration) {
  if (!migration.priority) {
    // nothing to do
    return _([migration]);
  }
  var url = config.log_database + '/' + migration.priority._id;
  migration.priority._deleted = true;
  return couchr.put(url, migration.priority).map(function (res) {
    return migration;
  });
});

exports.logSuccess = _.curry(function (config, migration) {
  if (!migration.success) {
    // nothing to do
    return _([migration]);
  }
  var doc = {
    type: 'success',
    seq: migration.seq,
    worker: config.name,
    database: exports.removeAuth(config.database),
    docid: migration.original._id,
    time: moment().toISOString()
  };
  return couchr.post(config.log_database, doc).map(function (res) {
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
    // default to checkpointing every 100 docs
    config.checkpoint_size = config.checkpoint_size || 100;
    return config;
};

/**
 * Announce worker has started and is listening for changes
 */

exports.logStart = _.curry(function (config, seq) {
  console.log('[' + config.name + '] Started from change ' + seq);
});

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
 * Logs a successful write of seq id to _local doc for worker
 */

exports.logCheckpoint = _.curry(function (config, seq) {
  console.log('[' + config.name + '] Checkpoint ' + seq);
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
    if (writes.length) {
      console.log(
        '[' + config.name + '] Written: ' +
        (writes.length > 1 ? '\n  ': '') +
        writes.join('\n  ')
      );
    }
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
