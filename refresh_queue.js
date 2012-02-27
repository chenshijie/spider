var MySqlClient = require('./lib/mysql').MySqlClient;
var fs = require('fs');

var configs = require('./etc/settings.json');
var logger = require('./lib/logger').logger;
var _logger = logger(__dirname + '/log/refresh.log');
var utils = require('./lib/utils');
var mysql = new MySqlClient(configs.baseurl);
var queue = require('queuer');
var q4url = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.spider_monitor_queue);

var base_url_count = 500;
var refresh_queue = function() {
  var current_time = utils.getTimestamp();
  var hour = new Date().getHours();
  var time_step = 15 * 60;
  if (hour > 22 || hour < 8) {
    time_step = 30 * 60;
  }
  console.log(utils.getLocaleISOString() + ' refresher run');
  var fetch_time = current_time - time_step;
  mysql.get_base_url(fetch_time, base_url_count, function(result) {
    var length = result.length;
    for ( var i = 0; i < length; i++) {
      var task = 'mysql://' + configs.baseurl.host + ':' + configs.baseurl.port + '/' + configs.baseurl.database + '?baseurl#' + result[i].id;
      console.log(task);
      _logger.info(task);
      q4url.enqueue(task);
    }
  });
};

// refresh_queue();

setInterval(function() {
  refresh_queue();
}, 60 * 1000);
