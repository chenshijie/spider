var async = require('async');

var WorkFlow = function(func, getCallback, getTask, concurrency) {

  var task = function(info, callback) {
    var start = function(callback) {
      callback(null, info);
    };
    async.waterfall([ start ].concat(func), callback);
  };

  this.q = async.queue(task, concurrency);

  this.q.drain = function() {
    console.log('all items have been processed');
  };

  this.q.empty = getTask;

  this.getCallback = getCallback;

};

Flow.prototype.push = function(task) {
  this.q.push(task, this.getCallback(task));
};

exports.WorkFlow = WorkFlow;
