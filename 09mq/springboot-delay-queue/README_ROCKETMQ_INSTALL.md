# rocketmq install

## windows-1
### standalone
Start Name Server
```cmd
D:\soft\rocketmq-all-4.9.8-bin-release>.\bin\mqnamesrv.cmd
```
Start Broker
```cmd
D:\soft\rocketmq-all-4.9.8-bin-release>.\bin\mqbroker.cmd -n localhost:9876 -c D:/soft/rocketmq-all-4.9.8-bin-release/conf/standalone/broker-a.properties autoCreateTopicEnable=true
```

## 快速启动
使用脚本
rocketmq4.9-start.bat
rocketmq4.9-stop.bat