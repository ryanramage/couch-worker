var worker = require('../../index');

module.exports = worker.createWorker(function (config) {
    var api = {};

    api.ignored = function (doc) {
        return !doc.hasOwnProperty('a') || !doc.hasOwnProperty('b');
    };

    api.migrated = function (doc) {
        return doc.hasOwnProperty('total');
    };

    api.migrate = function (doc, callback) {
        setTimeout(function () {
            doc.total = doc.a + doc.b;
            return callback(null, doc);
        }, 2000);
    };

    return api;
});
