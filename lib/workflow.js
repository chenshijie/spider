var async = require('async');

var WorkFlow = function(functions, getCallback, getNewTask, concurrency) {

  var worker = function(task, callback) {
    var start = function(callback) {
      callback(null, task);
    };
    async.waterfall([ start ].concat(functions), callback);
  };

  this.q = async.queue(worker, concurrency);

  this.q.drain = function() {
    console.log('all items have been processed');
  };

  this.q.empty = getNewTask;

  this.getCallback = getCallback;

};

WorkFlow.prototype.push = function(task) {
  this.q.push(task, this.getCallback(task));
};

WorkFlow.prototype.getQueueLength = function() {
  return this.q.length();
};
exports.WorkFlow = WorkFlow;
