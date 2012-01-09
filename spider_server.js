var http = require('http');
var fs = require('fs');
var MySqlClient = require('./lib/mysql').MySqlClient;
var logger = require('./lib/logger').logger;
var utils = require('./lib/utils');
var Spider = require('./lib/spiders').Spider;
var configs = require('./etc/settings.json');
var _logger = logger(__dirname + '/' + configs.log.file);
var queue = require('queuer');

var redis = require("redis");
var redisClient = redis.createClient(configs.redis.port, configs.redis.host);
redisClient.select(configs.redis.db);
redisClient.on('ready', function() {
  redisClient.select(configs.redis.db);
});

var databases = {};
var spiders = [];

var devent = require('devent').createDEvent('spider');

fs.writeFileSync(__dirname + '/run/server.lock', process.pid.toString(), 'ascii');

devent.on('queued', function(queue) {
  // 同时多个队列进入时会调用allSpidersRun()多次。
  if (queue == 'url') {
    // console.log('SERVER: ' + queue + " received task");
    // allSpidersRun();
  }
});

var queue4page_content = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_generate_queue);
// init spider
for ( var i = 1; i < configs.spider_count + 1; i++) {
  var spider = new Spider('spider_' + i, configs.cache_time || 300);
  spider.on('spider_finished', function(data) {
    var task = data.task;
    var run_time = utils.getTimestamp() - task.in_time;
    _logger.info([ 'TASK_FINISHED', this.name, 'RETRY:' + task.original_task.retry, task.original_task.uri, 'RUN_TIME:' + run_time ].join("\t"));
    devent.emit('task-finished', task.original_task);
  });

  spider.on('new_task', function(data) {
    var task = data.task;
    var new_task = utils.buildTaskURI({ protocol : task.protocol, hostname : task.hostname, port : task.port, database : task.database, table : 'page_content', id : data.new_task_id });
    _logger.info([ 'NEWTASK', this.name, 'RETRY:' + task.original_task.retry, task.original_task.uri, new_task ].join("\t"));
    queue4page_content.enqueue(new_task);
  });

  spider.on('spider_error', function(data) {
    var task = data.task;
    var run_time = utils.getTimestamp() - task.in_time;
    _logger.info([ 'TASK_ERROR', this.name, task.original_task.retry, task.original_task.uri, 'RUN_TIME:' + run_time ].join("\t"));
    devent.emit('task-error', task.original_task);
  });

  spiders.push(spider);
}

var queue4url = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_monitor_queue);
var all_spiders_last_start_time = new Date().getTime();

var allSpidersRun = function() {
  var time1 = new Date().getTime() - all_spiders_last_start_time;
  if (time1 < 1000) {
    console.log('################## the time between 2 allSpidersRun is too short (less than 1 second) ##################');
    return;
  }
  all_spiders_last_start_time = new Date().getTime();
  for ( var i = 0; i < configs.spider_count; i++) {
    var spider = spiders[i];
    if (spider.getStatus() == 'waiting') {
      startSimgleSpider(spider);
    }

  }
};

var startSimgleSpider = function(spider) {
  queue4url.dequeue(function(error, task) {
    if (error != 'empty') {
      _logger.info([ 'TASK_POPED', spider.getName(), task.retry, task.uri ].join("\t"));
      var time = utils.getTimestamp();
      var task_obj = utils.parseTaskURI(task, time);
      var key = task_obj.hostname + ':' + task_obj.port + ':' + task_obj.database;
      var uname_passwd = configs.mysql[key];
      if (databases[key] == undefined) {
        var mysql = new MySqlClient(task_obj.hostname, task_obj.port, uname_passwd.username, uname_passwd.password, task_obj.database);
        databases[key] = mysql;
      }
      var db = databases[key];
      spider.run(task_obj, db, redisClient, configs.redis.db, _logger);
    }
  });
};

setInterval(function() {
  allSpidersRun();
}, configs.check_interval);

console.log('Server Started ' + utils.getLocaleISOString());