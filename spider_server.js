var http = require('http');
var fs = require('fs');
var MySqlClient = require('./lib/mysql').MySqlClient;
var request = require('request');
var logger = require('./lib/logger').logger;
var utils = require('./lib/utils');

var Spider = require('./lib/spiders').Spider;

var config = __dirname + '/etc/settings.json';
var configs = JSON.parse(fs.readFileSync(config, 'utf8'));
var _logger = logger(__dirname + '/' + configs.log.file);
var queue = require('queuer');

var redis = require("redis");
var redisClient = redis.createClient(configs.redis.port, configs.redis.host);
redisClient.select(configs.redis.db);

var db_options = configs.mysql;
var databases = {};
var spiders = [];

var de = require('devent').createDEvent('spider');

fs.writeFileSync(__dirname + '/run/server.lock', process.pid.toString(), 'ascii');

de.on('queued', function(queue) {
  // 同时多个队列进入时会调用spiders_run()多次。
  if (queue == 'url') {
    // console.log('SERVER: ' + queue + " received task");
    // spiders_run();
  }
});

var queue4page = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_generate_queue);
// init spider
for ( var i = 1; i < configs.spider_count + 1; i++) {
  var spider = new Spider('spider_' + i, configs.cache_time || 300);
  spider.on('spider_ok', function(task) {
    // console.log('SERVER: ' + this.name + ' task-finished emmited');
    // var task_uri = utils.buildTaskURI(task.task);
    // var task_uri = task.task.original_task.uri;
    // console.log('TASK_FINISHED: ' + this.name + ' : ' + task_uri + ' will be
    // finished!');
    var run_time = utils.getTimestamp() - task.task.in_time;
    _logger.info([ 'TASK_FINISHED', this.name, 'RETRY:' + task.task.original_task.retry, task.task.original_task.uri, 'RUN_TIME:' + run_time ].join("\t"));
    de.emit('task-finished', task.task.original_task);
    if (task.new_task_id > 0) {
      var new_task = utils.buildTaskURI({ protocol : task.task.protocol, hostname : task.task.hostname, port : task.task.port, database : task.task.database, table : 'page_content', id : task.new_task_id });
      // console.log('NEWTASK: ' + this.name + ' : ' + new_task + ' will add to
      // queue');
      _logger.info([ 'NEWTASK', this.name, 'RETRY:' + task.task.original_task.retry, task.task.original_task.uri, new_task ].join("\t"));
      queue4page.enqueue(new_task);
    }
  });

  spider.on('spider_error', function(data) {
    // console.log('TASK_ERROR: ' + this.name + ' : task-error emmited : ' +
    // data.task.original_task.uri + ' ERROR_MSG: ' + data.msg);
    var run_time = utils.getTimestamp() - data.task.in_time;
    _logger.info([ 'TASK_ERROR', this.name, data.task.original_task.retry, data.task.original_task.uri, 'RUN_TIME:' + run_time ].join("\t"));
    de.emit('task-error', data.task.original_task);
  });

  spiders.push(spider);
}

var q4url = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_monitor_queue);
var all_spiders_last_start_time = new Date().getTime();
var spiders_run = function() {
  var time1 = new Date().getTime() - all_spiders_last_start_time;
  if (time1 < 1000) {
    console.log('################## the time between 2 spiders_run is too short (less than 1 second) ##################');
    return;
  }
  all_spiders_last_start_time = new Date().getTime();
  for ( var i = 0; i < configs.spider_count; i++) {
    var spider = spiders[i];
    // console.log('SERVER: ' + spider.getName() + ' is ' + spider.getStatus());
    if (spider.getStatus() == 'waiting') {
      spider2run(spider);

    }

  }
};

var spider2run = function(spider) {
  q4url.dequeue(function(error, task) {
    if (error != 'empty') {
      // console.log('SERVER: ' + task.uri + ' was poped, will be fetched by ' +
      // spider.getName());
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
  spiders_run();
}, configs.check_interval);

console.log('Server Started ' + utils.getLocaleISOString());