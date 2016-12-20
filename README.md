# easyctp

### 订阅数据到 influxdb / 或者 mongodb


### influxdb

````shell
docker run -it --net host --rm easyctp:subcribe --user ctp用户名 --password ctp密码 --broker ctpbroker --front ctpfront_tcp://180.168.146.187:10011 --instruments rb1705 --db_uri influxdb://用户名:密码@127.0.0.1:8086/数据库名 --model influx
```

### mongodb

````shell
docker run -it --net host --rm easyctp:subcribe --user ctp用户名 --password ctp密码 --broker ctpbroker --front ctpfront_tcp://180.168.146.187:10011 --instruments rb1705 --db_uri mongodb://用户名:密码@127.0.0.1:27017/数据库名 --model mongo
```
