var vows = require('vows');
var assert = require('assert');
var utils = require('../lib/utils');

var queue = 'test';
var uri = 'mysql://127.0.0.1:3306/spider?url#1';
var retry = 0;
var task = {
  queue : queue,
  uri : uri,
  retry : retry
};

vows.describe('测试 lib/utils.js').addBatch({
  'test md5()' : {
    topic : function() {
      return utils.md5('this is a test string');
    },
    'md5 result verify' : function(md5str) {
      assert.equal(md5str, '486eb65274adb86441072afa1e2289f3');
    }
  },
  '测试 utils.parseTaskURI' : {
    topic : function() {
      return utils.parseTaskURI(task, utils.getTimestamp());
    },
    'task解析后各项数据应该正确匹配' : function(ptask) {
      assert.equal(ptask.protocol, 'mysql');
      assert.equal(ptask.port, 3306);
      assert.equal(ptask.hostname, '127.0.0.1');
      assert.equal(ptask.database, 'spider');
      assert.equal(ptask.table, 'url');
      assert.equal(ptask.id, 1);
    },
    '解析后的task应该包含原始task' : function(ptask) {
      assert.deepEqual(ptask.original_task, task);
    }
  },
  '测试utils.buildTaskURI()' : {
    topic : function() {
      var task_obj = {
        protocol : 'mysql',
        hostname : '127.0.0.1',
        port : '33061',
        database : 'testdatabase',
        table : 'testtable',
        id : '110'
      };
      return utils.buildTaskURI(task_obj);
    },
    'buildTaskUri结果应该为字符串':function(task_uri) {
      assert.isString(task_uri);
    },
    '校验buildTaskUri结果是否正确:':function(task_uri) {
      assert.equal(task_uri,'mysql://127.0.0.1:33061/testdatabase?testtable#110');
    },
    'buildTaskUri结果应该能够被正常解析':function(task_uri) {
      var url = require('url');
      var result  = url.parse(task_uri);
      assert.equal(result.protocol,'mysql:');
      assert.equal(result.host,'127.0.0.1:33061');
      assert.equal(result.pathname,'/testdatabase');
      assert.equal(result.query,'testtable');
      assert.equal(result.hash,'#110');
    }
  }
}).export(module);