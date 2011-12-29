var MySqlClient = require('./lib/mysql').MySqlClient;
var fs = require('fs');

var config = __dirname + '/etc/settings.json';
var configs = JSON.parse(fs.readFileSync(config, 'utf8'));
var logger = require('./lib/logger').logger;
var _logger = logger(__dirname + '/log/refresh.log');
var utils = require('./lib/utils');
var mysql = new MySqlClient('172.16.33.237', 3306, 'stockradar', 'stockradar', 'stock_radar');
var queue = require('../queuer/lib/queue');
var q4url = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_monitor_queue);

var last_run_time = 0;
var refresh_queue = function() {
  var current_time = utils.getTimestamp();
  var hour = new Date().getHours();
  var time_step = 5 * 60;
  if(hour > 22 || hour < 8) {
    time_step = 30 * 60;
  }
  if(current_time - last_run_time < time_step) {
    return;
  }
  last_run_time = current_time;
  var fetch_time = current_time - time_step;
  mysql.get_base_url(fetch_time, function(result) {
    var length = result.length;
    for ( var i = 0; i < length; i++) {
      var task = 'mysql://172.16.33.237:3306/stock_radar?baseurl#' + result[i].id;
      _logger.info(task);
      q4url.enqueue(task);
    }
  });
};


refresh_queue();

/*
setInterval(function() {
  //refresh_queue();
}, 60*1000);
*/
