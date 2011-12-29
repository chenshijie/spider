var url = require('url');
var _ = require('underscore');
_.mixin(require('underscore.string'));

exports.getTimestamp = function() {
  return Math.floor(new Date().getTime() / 1000);
};

exports.getDateString = function() {
  var date = new Date();
  var pad = function(i) {
    if (i < 10) {
      return '0' + i;
    }
    return i;
  };
  return [ date.getFullYear(), pad(date.getMonth() + 1), pad(date.getDate()) ].join('');
};

exports.getLocaleISOString = function() {
  var date = new Date();
  var pad = function(i) {
    if (i < 10) {
      return '0' + i;
    }
    return i;
  };
  var pad3 = function(i) {
    if (i < 100) {
      return '0' + i;
    } else if (i < 10) {
      return '00' + i;
    }
    return i;
  };
  return [ date.getFullYear(), pad(date.getMonth() + 1), pad(date.getDate()) ].join('-') + ' ' + [ pad(date.getHours()), pad(date.getMinutes()), pad(date.getSeconds()) ].join(':') + '.' + pad3(date.getMilliseconds());
};

exports.parseTaskURI = function(task, time) {
  var task_info = url.parse(task.uri);
  var id = _.trim(task_info.hash, '#');
  var table_name = task_info.query;
  var db_name = _.trim(task_info.pathname, '/');
  var protocol = _.trim(task_info.protocol, ':');
  var task_obj = { in_time : time, protocol : protocol, hostname : task_info.hostname, port : task_info.port, database : db_name, table : table_name, id : id, original_task : task };
  return task_obj;
};

exports.buildTaskURI = function(task_obj) {
  // mysql://172.16.33.237:3306/stock_radar?url#60
  var uri = task_obj.protocol + '://' + task_obj.hostname + ':' + task_obj.port + '/' + task_obj.database + '?' + task_obj.table + '#' + task_obj.id;
  return uri;
};