var vows = require('vows');
var assert = require('assert');
var utils = require('../lib/utils');
var events = new require("events");
var MySqlClient = require('../lib/mysql').MySqlClient;
var logger = require('../lib/logger').logger;
var utils = require('../lib/utils');
var Spider = require('../lib/spiders').Spider;
var configs = require('../etc/settings.json');
var _logger = logger(__dirname + '/' + configs.log.file);
var queue = require('queuer');
var redis = require("redis");
var redisClient = redis.createClient(configs.redis.port, configs.redis.host);

redisClient.select(configs.redis.db);
redisClient.on('ready', function() {
  redisClient.select(configs.redis.db);
});

var mysql = new MySqlClient('127.0.0.1', 3306, 'root', '', 'spider');
var task1 = {
  queue : 'url',
  uri : 'mysql://127.0.0.1:3306/spider?url#1',
  retry : 0
};

var task2 = {
  queue : 'url',
  uri : 'mysql://127.0.0.1:3306/spider?url#9999',
  retry : 0
};

var task3 = {
    queue : 'url',
    uri : 'mysql://127.0.0.1:3306/spider?url#2',
    retry : 0
  };

vows.describe('测试 lib/spiders.js').addBatch({
  '测试创建Spider对象' : {
    topic : function() {
      return new Spider('spider_test');
    },
    '对象应为Object类型' : function(spider) {
      assert.isObject(spider);
    },
    '对象应具有run方法' : function(spider) {
      assert.isFunction(spider.run);
    },
    '对象应具有getStatus方法' : function(spider) {
      assert.isFunction(spider.getStatus);
    },
    '对象应具有getName方法' : function(spider) {
      assert.isFunction(spider.getName);
    },
    '对象应具有getTask方法' : function(spider) {
      assert.isFunction(spider.getTask);
    }
  }
}).addBatch({
  '测试Spider正常抓取过程触发spider_finished事件' : {
    topic : function() {
      var spider = new Spider('test');
      var promise = new (events.EventEmitter);
      spider.on('spider_finished', function(data) {
        promise.emit('success', data);
      });
      var task_obj = utils.parseTaskURI(task1, 0);
      var options = {
        task : task_obj,
        db : mysql,
        redis : redisClient,
        redis_db : configs.redis.db,
        logger : _logger
      };
      spider.run(options);
      return promise;
    },
    '正常情况测试一个出发task-finished事件' : function(error, data) {
      var task_obj = utils.parseTaskURI(task1, 0);
      assert.isNull(error);
      assert.deepEqual(data.task, task_obj);
    }
  }
}).addBatch({
  '测试Spider正常抓取过程抓取触发new_task事件' : {
    topic : function() {
      var spider = new Spider('test');
      var promise = new (events.EventEmitter);
      spider.on('new_task', function(data, new_id) {
        promise.emit('success', new_id);
      });
      var task_obj = utils.parseTaskURI(task1, 0);
      var options = {
        task : task_obj,
        db : mysql,
        redis : redisClient,
        redis_db : configs.redis.db,
        logger : _logger
      };
      spider.run(options);
      return promise;
    },
    '正常情况测试一个出发new_task事件' : function(error, new_id) {
      assert.isNumber(new_id);
    }
  }
}).addBatch({
  '测试Spider抓取不存在url信息触发spider_finished事件' : {
    topic : function() {
      var spider = new Spider('test');
      var promise = new (events.EventEmitter);
      spider.on('spider_finished', function(data) {
        promise.emit('success', data);
      });
      var task_obj = utils.parseTaskURI(task2, 0);
      var options = {
        task : task_obj,
        db : mysql,
        redis : redisClient,
        redis_db : configs.redis.db,
        logger : _logger
      };
      spider.run(options);
      return promise;
    },
    '数据中不存在url信息触发task-finished事件' : function(error, data) {
      var task_obj = utils.parseTaskURI(task2, 0);
      assert.isNull(error);
      assert.deepEqual(data.task, task_obj);
    }
  }
}).addBatch({
  '测试Spider抓取过程中无法获取页面内容触发spider_error事件' : {
    topic : function() {
      var spider = new Spider('test');
      var promise = new (events.EventEmitter);
      spider.on('spider_error', function(data) {
        promise.emit('success', data);
      });
      var task_obj = utils.parseTaskURI(task3, 0);
      var options = {
        task : task_obj,
        db : mysql,
        redis : redisClient,
        redis_db : configs.redis.db,
        logger : _logger
      };
      spider.run(options);
      return promise;
    },
    '无法获取页面内容触发spider_error事件' : function(error, data) {
      var task_obj = utils.parseTaskURI(task3, 0);
      assert.deepEqual(data.task, task_obj);
    },
    '无法获取页面内容触发spider_error事件,msg信息' : function(error, data) {
      var task_obj = utils.parseTaskURI(task3, 0);
      assert.equal(data.msg,'request error');
    }
  }
}).export(module);