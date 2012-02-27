var mysql = require('mysql');

var MySqlClient = function(options) {
  var self = this;
  self.client = mysql.createClient(options);
};

MySqlClient.prototype.store_page_content = function(url_id, in_time, stock_code, url_table, meta, content, cb) {
  this.client.query('INSERT INTO page_content SET url_id = ?, in_time = ?, stock_code = ?, url_table = ?, parse_time = 0, meta = ?, content = ?', [ url_id, in_time, stock_code, url_table, meta, content ], function(err, results) {
    if (err) {
      console.log(err);
      cb(0);
    } else {
      cb(results.insertId);
    }
  });
};

MySqlClient.prototype.get_page_content_count = function(url_id, url_table, cb) {
  this.client.query('SELECT count(*) AS CNT FROM page_content where url_id = ? and url_table = ?', [ url_id, url_table ], function(err, results) {
    if (err) {
      cb(-1);
    } else {
      cb(results[0]['CNT']);
    }
  });
};

MySqlClient.prototype.get_url_info = function(table_name, url_id, cb) {
  this.client.query('SELECT * FROM ' + table_name + ' WHERE id = ?', [ url_id ], function(err, results) {
    if (err) {
      console.log(err);
      cb(err);
    } else {
      cb(results);
    }
  });
};

MySqlClient.prototype.update_url_fetch_time = function(table_name, url_id, fetch_time, cb) {
  this.client.query('update ' + table_name + ' set fetch_time = ? where id = ?', [ fetch_time, url_id ], function(err, results) {
    if (err) {
      cb(false);
    } else {
      cb(true);
    }
  });
};

MySqlClient.prototype.get_base_url = function(fetch_time, count, cb) {
  this.client.query('SELECT * FROM baseurl WHERE fetch_time < ? limit ?', [ fetch_time, count ], function(err, results, fields) {
    if (err) {
      cb(err);
    } else {
      cb(results);
    }
  });
};

exports.MySqlClient = MySqlClient;