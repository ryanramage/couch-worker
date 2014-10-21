# couch-worker

This module is used to provide shared functionality between CouchDB
workers. It manages the connection to CouchDB and listening for changes, as
well as posting those changes back to Couch and logging updates.


## Defining a worker

```javascript
var createWorker = require('couch-worker').createWorker;

module.exports = createWorker(function (config) {
  return {

    ignored: function (doc) {
      return doc._id[0] === '_';
    },

    migrated: function (doc) {
      return doc.new_property;
    },

    migrate: function (doc, callback) {
      doc.new_property = 'wheee';
      callback(null, doc);
    }

  };
});
```

### ignored(doc)

This should be a predicate which returns true if the document is ignored by
the worker, false otherwise. You might want to restrict the worker to
operating on a specific document type, and exclude design docs for example.

### migrated(doc)

This should be a predicate which returns true if the doc has already been
migrated, false otherwise. All documents returned from the `migrate()`
function **must** pass this predicate.

### migrate(doc, callback)

This is the migration function which can cause whatever effects may be
required to update the document then passes the updated document back to
the callback. You can return multiple documents in an array if you like,
but you **must** return the original document as one of them (modified so
that it passes the `migrated()`predicate).


## Starting a worker

```javascript
// require the worker definition (see above section)
var myworker = require('myworker');

var config = {
  name: 'My Worker',
  database: 'http://admin:password@localhost:5984/database',
  log_database: 'http://admin:password@localhost:5984/workers',
  concurrency: 4
};

// start the worker
var w = myworker.start(config);

// stop the worker
w.stop(function () {
  console.log('worker stopped');
});
```

### Common configuration options

Your worker can use additional configuration properties as required (for
API keys etc), but all workers using `couch-worker` have the following
options available.

* __name__ - *String* - The unique name for this worker instance
* __database__ - *String* - The database URL (with credentials) to migrate
  documents in
* __log\_database__ - *String* - The database URL (with credentials) to store
  worker-related data (error logs, priority queues etc)
* __concurrency__ - *Number* - Maximum number of documents to process in
  parallel
* __timeout__ - *Number* Time to wait in milliseconds for `migrate()` calls to
  return before causing a timeout error and discarding any future result
* __checkpoint\_size__ - *Number* - The number of documents to process before
  recording a checkpoint (the sequence id the worker will resume processing
  from on a restart)
* __retry\_attempts__ - *Number* - Number of times to retry a `migrate()` when
  an error is returned before recording the error in the log\_database and
  moving onto the next change
* __retry\_interval__ - *Number* - Number of milliseconds to wait before retrying
* __bucket__ - *Object* - An object with `start` and/or `end` properties. This
  causes the worker to hash all document IDs using md5 to put them into fair
  buckets. The worker will only process the document if the hex digest of the md5
  hash is greater than or equal to `start` and less than `end`. All other
  documents will be ignored. This allows you to run multiple instances of
  the same worker to split up processing of documents. Start and end
  properties should be Strings in the hex range ('0000...' to 'ffff..').
  Omitting the start property means "process everything up until 'end'",
  omitting the end property means "process everything from 'start'
  onwards".


## Viewing worker progress / logs

You can push the `couch-worker-dashboard` CouchApp to the `log_database` db
to get a web interface to workers using this module. This includes progress
information, error logs and the ability to process specific documents
on demand.
