

#Spider

A server for fetching url from web site by url
## Installation

### Clone code from github

    $git clone https://github.com/netgen-inc/spider.git

### Install dependencies

    $cd spider
    $npm install -d
    
### Modify configuration file

    $cd etc
    $cp settings.original.json settings.json
    $vim settings.json
    
### settings.json 说明

```
    {
      "queue_server" : { //Queue Server
        "host" : "127.0.0.1", //Queue Server IP
        "port" : 3000, //Queue Server Port
        "queue_path" : "queue" //Queue Server Path
      },
      "mysql" : {
        "172.16.33.237:3306:stock_radar" : { //任务URI中的ip,端口和database
          "username" : "stockradar",//任务中数据库用户名
          "password" : "stockradar"//任务中数据库用密码
        },
        "172.16.39.117:3306:spider" : {
          "username" : "spider",
          "password" : "spider"
        },
        "redis" : {
          "host" : "172.16.39.117",  //redis服务器host
          "port" : 6379,//redis服务器端口
          "db" : 14 //redis数据库
        },
        "baseurl" : {
          "host" : "172.16.39.117", //BaseUrl表所在库的IP
          "port" : 3306, //BaseUrl表所在库的端口 
          "username" : "spider",//BaseUrl表所在库的用户名
          "password" : "spider",//BaseUrl表所在库的密码
          "database" : "spider" //BaseUrl表所在库的数据库名称
        }
      },
      "log" : {
        "file" : "log/spider.log" //Spider 日志文件
      },
      "spider_count" : 50, //同时最大请求数
      "spider_monitor_queue" : "url",
      "spider_generate_queue" : "page_content",
      "check_interval" : 2000
    }
```


## Start the spider server
    $node spider_server.js  
## Start refresh server
    $node refresh_queue.js
