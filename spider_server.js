var fs = require('fs');
var MySqlClient = require('./lib/mysql').MySqlClient;
var logger = require('./lib/logger').logger;
var utils = require('./lib/utils');
var configs = require('./etc/settings.json');
var _logger = logger(__dirname + '/' + configs.log.file);
var queue = require('queuer');
var WorkFlow = require('./lib/workflow').WorkFlow;
var Worker = require('./lib/worker');
var request = require('request');
var redis = require("redis");
var redisClient = redis.createClient(configs.redis.port, configs.redis.host);
redisClient.select(configs.redis.db);
redisClient.on('ready', function() {
  redisClient.select(configs.redis.content_db);
});

var devent = require('devent').createDEvent('spider');
// 将pid写入文件，以便翻滚日志时读取
fs.writeFileSync(__dirname + '/run/server.lock', process.pid.toString(), 'ascii');
// 页面内容队列
var queue4PageContent = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_generate_queue);
// url队列
var queue4Url = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_monitor_queue);

var databases = {};
// 准备数据库队列
var i = 0;
for (i = 0; i < configs.mysql.length; i++) {
  var options = configs.mysql[i];
  var key = options.host + ':' + options.port + ':' + options.database;
  var mysql = new MySqlClient(options);
  databases[key] = mysql;
}

devent.on('queued', function(queue) {
  // 同时多个队列进入时会调用allSpidersRun()多次。
  if (queue == 'url') {
    // console.log('SERVER: ' + queue + " received task");
    // allSpidersRun();
  }
});

var sina_limit = 0;

var databases = {};
for (i = 0; i < configs.mysql.length; i++) {
  var option = configs.mysql[i];
  var key = option.host + ':' + option.port + ':' + option.database;
  var mysql = new MySqlClient(option);
  databases[key] = mysql;
}

var queue4PageContent = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_generate_queue);
var queue4Url = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_monitor_queue);

/**
 * 对task进行准备工作
 * 
 * @param task
 * @param callback
 */
var prepareTask = function(task, callback) {
  if (configs.debug) {
    console.log('----------> prepareTask <-----------');
  }
  if (task.original_task.retry >= 10) {
    var error = {
      error : 'TASK_RETRY_TIMES_LIMITED',
      msg : 'try to deal with the task more than 10 times'
    };
    callback(error, task);
  } else {
    var key = task.hostname + ':' + task.port + ':' + task.database;
    var db = databases[key];
    if (db != undefined) {
      task['mysql'] = db;
      task['redis'] = redisClient;
      task['logger'] = _logger;
      task['cache_time'] = configs.cache_time;
      task['debug'] = configs.debug;
      callback(null, task);
    } else {
      var error = {
        error : 'TASK_DB_NOT_FOUND',
        msg : 'cant not find the database configs included by task URI'
      };
      callback(error, task);
    }
  }
};
var getCallback = function(info) {
  return function(err, ret) {
    if (err == null) {
      // 所有步骤完成,任务完成
      console.log(utils.getLocaleISOString() + ' task-finished : ' + info.original_task.uri);
      devent.emit('task-finished', info.original_task);
      // 如果页面内容被保存到服务器,将新任务加入到队列
      if (info.save2DBOK && info.new_task_id > 0) {
        var new_task = utils.buildTaskURI({
          protocol : info.protocol,
          hostname : info.hostname,
          port : info.port,
          database : info.database,
          table : 'page_content',
          id : info.new_task_id
        });
        console.log(utils.getLocaleISOString() + ' NEW_TASK: ' + new_task);
        queue4PageContent.enqueue(new_task);
      }
      if (info.pageContentUnchanged) {
        console.log(utils.getLocaleISOString() + ' page content is not changed: ' + info.original_task.uri);
      }
    } else if (err.error == 'TASK_RETRY_TIMES_LIMITED') {
      console.log(utils.getLocaleISOString() + ' 任务尝试次数太多,通知队列任务完成,不在继续尝试' + info.original_task.uri);
      devent.emit('task-finished', info.original_task);
    } else if (err.error == 'TASK_DB_NOT_FOUND') {
      console.log(utils.getLocaleISOString() + ' TASK_DB_NOT_FOUND: ' + info.original_task.uri);
      devent.emit('task-finished', info.original_task);
    } else if (err.error == 'TASK_URL_NOT_FOUND') {
      console.log(utils.getLocaleISOString() + ' TASK_URL_NOT_FOUND: ' + info.original_task.uri);
      devent.emit('task-finished', info.original_task);
    } else if (err.error == 'PAGE_CONTENT_UNCHANGED') {
      console.log(utils.getLocaleISOString() + ' PAGE_CONTENT_UNCHANGED: ' + info.original_task.uri);
      devent.emit('task-finished', info.original_task);
    } else if (err.error == 'FETCH_URL_ERROR') {
      sina_limit = utils.getTimestamp();
      console.log(utils.getLocaleISOString() + ' FETCH_URL_ERROR do nothing:' + info.original_task.uri);
      // devent.emit('task-error', info.original_task);
    } else if (err.error == 'PAGE_CONTENT_SAVE_2_DB_ERROR') {
      devent.emit('task-error', info.original_task);
    } else {
      console.log(err);
    }
  };
};

/**
 * 从队列中获取新任务,取到新任务将其压入workFlow队列
 */
var getNewTask = function() {
  if (configs.debug) {
    console.log('----------> getNewTask <-----------');
  }
  var time_stamp = utils.getTimestamp();
  if (time_stamp - sina_limit > 60) {
    queue4Url.dequeue(function(error, task) {
      if (error != 'empty' && task != undefined) {
        var time = utils.getTimestamp();
        var task_obj = utils.parseTaskURI(task, time);
        workFlow.push(task_obj);
      } else {
        // console.log('task queue is empty');
      }
    });
  }
};

var worker = Worker.getWorker();
var workFlow = new WorkFlow([ prepareTask, worker.getTaskDetailFromDB, worker.getPageContentFromCache, worker.fetchPageContentViaHttp, worker.savePageContent2Cache, worker.checkPageContent, worker.save2Database, worker.updateUrlInfo ], getCallback, getNewTask, configs.spider_count);

setInterval(function() {
  var time_stamp = utils.getTimestamp();
  if (time_stamp - sina_limit > 60 && workFlow.getQueueLength() < 50) {
    for ( var i = 0; i < 50 - workFlow.getQueueLength(); i++) {
      getNewTask();
    }
  }
}, configs.check_interval);

console.log('Server Started ' + utils.getLocaleISOString());