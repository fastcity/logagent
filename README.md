# logagent
日志收集系统
# 说明
### 结构图
![结构图](https://raw.githubusercontent.com/AITown/logagent/master/structure/structure01.png)
1. 需要有配置文件，配置文件为logagent.ini
2. 配置文件路径为生成的exe的路径+\config\logagent.ini
3. 下来配置文件将利用web端进行配置,mysql进行存储(尚未完成)
# client
客户端
# config
日志配置文件
 # etcdset
 设置etcd的key的路径的，类似于："D:\GoWork\test0.txt"

# consumer
1. 消费kafka的消息
2. 发送到es
3. 用kibana 进行消息展示
