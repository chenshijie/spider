var request = require('request');
var util = require("util");
var event = new require("events").EventEmitter;
var utils = require('./utils');

var Spider = function(spider_name, cache_time) {
  var self = this;
  self.status = 'waiting';
  self.name = spider_name;
  self.db = '';
  self.cache_time = cache_time || 300;

  var onFetchFinish = function(data) {
    var in_time = utils.getTimestamp();
    self.db.store_page_content(data.url_id, in_time, data.stock_code, self.task.table, JSON.stringify(data.meta), data.content, function(id) {
      if (id == 0) {
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
      self.emit('spider_finished', task);
      self.emit('new_task', task);
    });
  };

  self.on('save2db_ok', onSave2DBOK);

  var onCheckSuccess = function(check_result) {
    self.db.get_url_info(self.task.table, self.task.id, function(result) {
      // console.log(self.name + ' ready to fetch ' + result[0].url);
      // can not read url information from database
      if (result.length == 0 || result[0].url == '' || result[0].site == '' || result[0].type == '') {
        // console.log(self.name + ' can not read url info by id ' + task.id);
        self.emit('spider_finished', { task : self.task, msg : ' can not read url info by id ' + self.task.id });
        self.status = 'waiting';
      } else {
        // get page content from cache server
        getPageContentFromCache(result[0].url, function(cache_result) {
          if (cache_result.cached) {
            var meta = { url : result[0].url, site : result[0].site, type : result[0].type };
            self.emit('fetch_finish', { meta : meta, content : cache_result.content, url_id : self.task.id, stock_code : result[0].stock_code });
          } else {
            // get page content from web site
            request({ 'url' : result[0].url, 'encoding' : 'binary', 'timeout' : 50000 }, function(error, response, body) {
              if (error) {
                console.log(error);
                self.emit('spider_error', { task : self.task, msg : 'request error' });
                self.status = 'waiting';
              } else {
                var meta = { url : result[0].url, site : result[0].site, type : result[0].type };
                self.emit('fetch_finish', { meta : meta, content : body, url_id : self.task.id, stock_code : result[0].stock_code });
                savePageContent2Cache(result[0].url, body);
              }
            });
          }
        });
      }
    });
  };

  self.on('spider_start', onCheckSuccess);

  var getPageContentFromCache = function(url, cb) {
    // console.log('get '+url+' from cache');
    var cache_key = 'url:' + utils.md5(url);
    self.redis.select(self.redis_db);
    self.redis.get(cache_key, function(error, reply) {
      if (reply) {
        cb({ cached : true, content : reply });
      } else {
        cb({ cached : false, content : '' });
      }
    });
  };

  var savePageContent2Cache = function(url, content) {
    var cache_key = 'url:' + utils.md5(url);
    self.redis.select(self.redis_db);
    self.redis.setex(cache_key, self.cache_time, content);
  };
};

util.inherits(Spider, event);

Spider.prototype.run = function(task, db, redis, redis_db, logger) {
  var self = this;
  if (self.status == 'running') {
    console.log(self.name + 'is running ,task:' + task.original_task.uri + ' will be ignored!');
    return;
  }
  self.redis = redis;
  self.redis_db = redis_db;
  self.logger = logger;

  self.status = 'running';
  self.task = task;
  self.db = db;
  self.emit('spider_start', {});
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
