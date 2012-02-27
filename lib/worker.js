var utils = require('./utils');
var request = require('request');
var Worker = function(options) {
};

/**
 * 根据任务URI从数据库中获取任务详情
 * 
 * @param task
 * @param callback
 */
Worker.prototype.getTaskDetailFromDB = function(task, callback) {
  if (task.debug) {
    console.log('----------> getTaskDetailFromDB <-----------');
  }
  task.mysql.get_url_info(task.table, task.id, function(result) {
    if (result.length == 0 || result[0].url == '' || result[0].site == '' || result[0].type == '') {
      var error = {
        error : 'TASK_URL_NOT_FOUND',
        msg : ' can not read url info by id ' + task.id
      };
      callback(error, task);
    } else {
      task['urlInfo'] = result[0];
      callback(null, task);
    }
  });
};

/**
 * 从缓存服务器中获取页面内容
 * 
 * @param task
 * @param callback
 */
Worker.prototype.getPageContentFromCache = function(task, callback) {
  if (task.debug) {
    console.log('----------> getPageContentFromCache ' + task.urlInfo.id + '<-----------');
  }
  var cache_key = 'CONTENT:' + task.urlInfo.url;
  task.redis.get(cache_key, function(error, reply) {
    if (reply) {
      task['pageContentCached'] = true;
      task['pageContent'] = reply;
      callback(null, task);
    } else {
      task['pageContentCached'] = false;
      callback(null, task);
    }
  });
};

/**
 * 抓取任务中的URL
 * 
 * @param task
 * @param callback
 * @returns
 */
Worker.prototype.fetchPageContent = function(task, callback) {
  if (task.debug) {
    console.log('----------> fetchPageContent ' + task.urlInfo.id + '<-----------');
  }
  if (task.pageContentCached) {
    callback(null, task);
  } else {
    request({
      'url' : task.urlInfo.url,
      'encoding' : 'binary',
      'timeout' : 50000
    }, function(error, response, body) {
      if (error) {
        console.log(error);
        var error = {
          error : 'FETCH_URL_ERROR',
          msg : 'request error'
        };
        callback(error, task);
      } else {
        task['pageContent'] = body;
        callback(null, task);
      }
    });
  }
};

/**
 * 将抓取到的页面内容存入缓存服务器
 * 
 * @param task
 * @param callback
 */
Worker.prototype.savePageContent2Cache = function(task, callback) {
  if (task.debug) {
    console.log('----------> savePageContent2Cache ' + task.urlInfo.id + '<-----------');
  }
  if (task.pageContentCached) {
    callback(null, task);
  } else {
    // 如果页面内容是通过抓取获得的，将页面内容缓存
    var cache_key = 'CONTENT:' + task.urlInfo.url;
    task.redis.setex(cache_key, task.cache_time, task.pageContent);
    callback(null, task);
  }
};

/**
 * 判断PageContent内容是否变化
 * 
 * @param task
 * @param callback
 */
Worker.prototype.checkPageContent = function(task, callback) {
  if (task.debug) {
    console.log('----------> checkPageContent  ' + task.urlInfo.id + '<-----------');
  }
  if (task.urlInfo.type == 'list') {
    var cache_key = 'HASH:' + task.urlInfo.url;
    task.redis.get(cache_key, function(error, reply) {
      var contentHash = utils.md5(task.pageContent);
      if (reply && reply == contentHash) {
        task['pageContentUnchanged'] = true;
        var error = {
          error : 'PAGE_CONTENT_UNCHANGED',
          msg : 'page content is not changed'
        };
        callback(null, task);
      } else {
        task['pageContentUnchanged'] = false;
        task.redis.set(cache_key, contentHash);
        callback(null, task);
      }
    });
  } else {
    task['pageContentUnchanged'] = false;
    callback(null, task);
  }
};

/**
 * 页面内容保存到数据库
 * 
 * @param task
 * @param callback
 */
Worker.prototype.save2Database = function(task, callback) {
  if (task.debug) {
    console.log('----------> save2Database ' + task.urlInfo.id + '<-----------');
  }
  if (task['pageContentUnchanged'] == false) {
    var in_time = utils.getTimestamp();
    var meta = {
      url : task.urlInfo.url,
      site : task.urlInfo.site,
      type : task.urlInfo.type
    };
    task.mysql.store_page_content(task.id, in_time, task.urlInfo.stock_code, task.table, JSON.stringify(meta), task.pageContent, function(id) {
      if (id == 0) {
        task['save2DBOK'] = false;
        var error = {
          error : 'PAGE_CONTENT_SAVE_2_DB_ERROR',
          msg : 'can not save page content to database'
        };
        callback(error, task);
      } else {
        task['save2DBOK'] = true;
        task['new_task_id'] = id;
        callback(null, task);
      }
    });
  } else {
    callback(null, task);
  }

};

/**
 * 更新URL的最后抓取时间
 * 
 * @param task
 * @param callback
 */
Worker.prototype.updateUrlInfo = function(task, callback) {
  if (task.debug) {
    console.log('----------> updateUrlInfo ' + task.urlInfo.id + '<-----------');
  }
  var fetch_time = utils.getTimestamp();
  task.mysql.update_url_fetch_time(task.table, task.id, fetch_time, function(result) {
    if (!result) {
      console.log('update url table error');
    }
    callback(null, task);
  });
};
exports.Worker = Worker;
exports.getWorker = function() {
  return new Worker();
};