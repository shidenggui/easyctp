# easyctp

### 订阅数据到 influxdb

````shell
docker run -it --net host  --rm subcription_ctp_to_influxdb --user ctp用户名 --password ctp密码 --broker ctpbroker --front ctpfront_tcp://180.168.146.187:10011 --instruments rb1705 --influxdb influxdb://用户名:密码@127.0.0.1:8086/数据库名
```