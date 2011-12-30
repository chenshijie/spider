var request = require('request');
var util = require("util");
var event = new require("events").EventEmitter;
var utils = require('./utils');

var Spider = function(spider_name) {
  var self = this;
  self.status = 'waiting';
  self.name = spider_name;
  self.db = '';

  var onFetchFinish = function(data) {
    // console.log(self.name + ' onFetchFinish ');
    var in_time = utils.getTimestamp();
    self.db.store_page_content(data.url_id, in_time, data.stock_code, self.task.table, JSON.stringify(data.meta), data.content, function(id) {
      // console.log(self.name + ' store_page_content !status: ' + self.status);
      if (id == 0) {
        // console.log(self.name + ' save page content to database error!');
        self.emit('spider_error', { task : self.task, msg : 'save page content to database error!' });
        self.status = 'waiting';
      } else {
        self.new_task_id = id;
        self.emit('save2db_ok', { task : self.task, new_task_id : id });
      }
    });
  };

  self.on('fetch_finish', onFetchFinish);

  var onSave2DBOK = function(task) {
    var fetch_time = utils.getTimestamp();
    self.db.update_url_fetch_time(task.task.table, task.task.id, fetch_time, function(result) {
      self.status = 'waiting';
      if (!result) {
        // console.log([ self.name, task, 'update url table error' ]);
      }
      self.emit('spider_ok', task);
    });
  };

  self.on('save2db_ok', onSave2DBOK);

  var onCheckSuccess = function(check_result) {
    self.db.get_url_info(self.task.table, self.task.id, function(result) {
      // console.log(self.name + ' ready to fetch ' + result[0].url);
      if (result.length == 0 || result[0].url == '' || result[0].site == '' || result[0].type == '') {
        // console.log(self.name + ' can not read url info by id ' + task.id);
        self.emit('spider_ok', { task : self.task, msg : ' can not read url info by id ' + self.task.id, new_task_id : 0 });
        self.status = 'waiting';
      } else {
        request({ 'url' : result[0].url, 'encoding' : 'binary', 'timeout' : 50000 }, function(error, response, body) {
          if (error) {
            console.log(error);
            self.emit('spider_error', { task : self.task, msg : 'request error' });
            self.status = 'waiting';
          } else {
            // console.log(self.name + ' emit fetch_finish');
            var meta = { url : result[0].url, site : result[0].site, type : result[0].type };
            self.emit('fetch_finish', { meta : meta, content : body, url_id : self.task.id, stock_code : result[0].stock_code });
          }
        });
      }
    });
  };

  self.on('check_success', onCheckSuccess);
};

util.inherits(Spider, event);

Spider.prototype.run = function(task, db) {
  var self = this;
  if (self.status == 'running') {
    console.log(self.name + 'is running ,task:' + task.original_task.uri + ' will be ignored!');
    return;
  }
  self.status = 'running';
  // console.log(self.name + ' run');
  self.task = task;
  self.db = db;
  if (task.table != 'baseurl') {
    self.db.get_page_content_count(self.task.id, self.task.table, function(count) {
      if (count == -1) {
        self.emit('spider_error', { task : self.task, msg : ' get_page_content_count error' });
        self.status = 'waiting';
      } else if (count >= 1) {
        self.emit('spider_ok', { task : self.task, new_task_id : 0 });
        self.status = 'waiting';
        console.log('#####################已经抓取过了');
      } else {
        self.emit('check_success', {});
      }
    });
  } else {
    self.emit('check_success', {});
  }
};

Spider.prototype.getStatus = function() {
  return this.status;
};

Spider.prototype.getName = function() {
  return this.name;
};

Spider.prototype.getTask = function() {
  return this.task;
};
exports.Spider = Spider;
