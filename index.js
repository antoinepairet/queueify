// createAsyncQueue([
//     {
//         objArray: resources,
//         task: task,
//         taskName: 'updateDates'
//     }
// ]);

var _ = require('lodash');
var Q = require('q');
var async = require('async');
var moment = require('moment');
var fs = require('fs');


module.exports = {
    createAsyncQueue: createAsyncQueue
};


function createAsyncQueue(_taskDescriptions, _options) {
    var options = _.defaults(_options || {}, {
        parallel: false
    });

    var taskDescriptions = _.isArray(_taskDescriptions) ?
        _taskDescriptions :
        [_taskDescriptions];

    var promise;
    var errors = [];

    if (options.parallel) {
        promise = Q.all(_.map(taskDescriptions, taskDescription => {
            return _singleQueue(taskDescription);
        }));
    } else {
        promise = Q();
        _.forEach(taskDescriptions, taskDescription => {
            promise = promise.then(() => {
                return _singleQueue(taskDescription);
            })
        });
    }

    var processExitCode = 0;
    promise
        .then(() => {
            console.log('Congrats, everything created!!!');
        })
        .catch((err) => {
            console.error(err)
            processExitCode = -1;
        })
        .finally(() => {
            if (errors.length) {
                console.log('ERRORS: ', errors.length)
                fs.writeFileSync(moment().format('YYYY_MM_DD__HH_mm') + '_errors.json', JSON.stringify(errors, null, 2));
            }
            process.exit(processExitCode);
        });
}

function _singleQueue(task, errors) {
    var d = Q.defer();
    console.log(task.name, 'will start, total items', task.items.length);

    // Create an async queue
    var uploadQueue = async.queue(worker, task.concurency || 1); // Max concurrency

    var counter = 0;
    function worker(item, doneCb) {
        task.worker(item)
            .then(() => {
                console.log(task.name + ' ' + (counter += 1) + ' / ' + task.items.length);
                doneCb();
            })
            .catch(err => {
                console.error(err);
                errors.push({
                    task: task.name,
                    item: item,
                    error: err
                });

                doneCb(err);
            });
    }

    uploadQueue.push(task.items);

    uploadQueue.drain = function () {
        console.log('FINISHED for ' + task.name);
        d.resolve();
    };

    return d.promise;
}