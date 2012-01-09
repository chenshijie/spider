var request = require('request');
var util = require("util");
var event = new require("events").EventEmitter;
var utils = require('./utils');

var Spider = function(spider_name, cache_time) {
  var self = this;
  self.status = 'waiting';
  self.name = spider_name;
  self.cache_time = cache_time || 300;

  var onFetchFinish = function(options, data) {
    var in_time = utils.getTimestamp();
    options.db.store_page_content(options.task.id, in_time, data.stock_code, options.task.table, JSON.stringify(data.meta), data.content, function(id) {
      if (id == 0) {
        self.emit('spider_error', { task : options.task, msg : 'save page content to database error!' });
        self.status = 'waiting';
      } else {
        self.emit('save2db_ok', options, id);
      }
    });
  };

  self.on('fetch_finish', onFetchFinish);

  var onSave2DBOK = function(options, new_task_id) {
    var fetch_time = utils.getTimestamp();
    options.db.update_url_fetch_time(options.task.table, options.task.id, fetch_time, function(result) {
      self.status = 'waiting';
      if (!result) {
        // console.log([ self.name, task, 'update url table error' ]);
      }
      self.emit('spider_finished', options);
      self.emit('new_task', options, new_task_id);
    });
  };

  self.on('save2db_ok', onSave2DBOK);
  // var options = { task : task_obj, db : db, redis : redisClient, redis_db :
  // configs.redis.db, logger : configs.redis.db };
  var onCheckSuccess = function(options) {
    options.db.get_url_info(options.task.table, options.task.id, function(result) {
      if (result.length == 0 || result[0].url == '' || result[0].site == '' || result[0].type == '') {
        self.emit('spider_finished', { task : options.task, msg : ' can not read url info by id ' + options.task.id });
        self.status = 'waiting';
      } else {
        getPageContentFromCache(options.redis, result[0].url, function(cache_result) {
          if (cache_result.cached) {
            var meta = { url : result[0].url, site : result[0].site, type : result[0].type };
            self.emit('fetch_finish', options, { meta : meta, content : cache_result.content, stock_code : result[0].stock_code });
          } else {
            // get page content from web site
            request({ 'url' : result[0].url, 'encoding' : 'binary', 'timeout' : 50000 }, function(error, response, body) {
              if (error) {
                console.log(error);
                self.emit('spider_error', { task : options.task, msg : 'request error' });
                self.status = 'waiting';
              } else {
                var meta = { url : result[0].url, site : result[0].site, type : result[0].type };
                self.emit('fetch_finish', options, { meta : meta, content : body, stock_code : result[0].stock_code });
                savePageContent2Cache(options.redis, result[0].url, body);
              }
            });
          }
        });
      }
    });
  };

  self.on('spider_start', onCheckSuccess);

  var getPageContentFromCache = function(redis, url, cb) {
    var cache_key = 'url:' + utils.md5(url);
    redis.get(cache_key, function(error, reply) {
      if (reply) {
        cb({ cached : true, content : reply });
      } else {
        cb({ cached : false, content : '' });
      }
    });
  };

  var savePageContent2Cache = function(redis, url, content) {
    var cache_key = 'url:' + utils.md5(url);
    redis.setex(cache_key, self.cache_time, content);
  };
};

util.inherits(Spider, event);

Spider.prototype.run = function(options) {
  var self = this;
  if (self.status == 'running') {
    console.log(self.name + 'is running ,task:' + options.task.original_task.uri + ' will be ignored!');
    return;
  }
  self.status = 'running';
  self.emit('spider_start', options);
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
