package main

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/astaxie/beego/logs"
	"github.com/olivere/elastic"
	"sync"
	"time"
)

var wd sync.WaitGroup

//CollectTypeInfo 日志收集类型的信息
type CollectTypeInfo struct {
	Partition int32
	Offset    int64
	Key       string
	Value     string
}

func main() {
	initAppLog("")
	//new kafka消费实例
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)

	if err != nil {
		logs.Error("failed to start consumer ", err)
	}
	//new es实例
	esclient, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL("http://127.0.0.1:9200"))
	if err != nil {
		logs.Debug("connect es err", err)
	}

	//获取topic的一个列表   topic 应该从etcd中读取此处先写死
	partlist, err1 := consumer.Partitions("path_topic")
	if err1 != nil {
		logs.Error("failed to get consumer.Partitions ", err1)
	}
	for part := range partlist {
		pc, err2 := consumer.ConsumePartition("path_topic", int32(part), sarama.OffsetNewest)
		if err2 != nil {
			logs.Error("failed to get consumer.Partitions ", err2)
			return
		}
		defer pc.AsyncClose()
		wd.Add(1)
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				logs.Debug("partition :%d,offset %d,key:%s value:%s \n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				collectTypeInfo := CollectTypeInfo{
					msg.Partition,
					msg.Offset,
					string(msg.Key),
					string(msg.Value),
				}
				uid := time.Now().Nanosecond()
				index := "log_" + string(uid)
				sendes(esclient, index, "collectTypeInfo", "", collectTypeInfo)
				//发送到es
			}
			wd.Done()
		}(pc)
	}
	wd.Wait()
	consumer.Close()
}

//发送到ES
func sendes(client *elastic.Client, index, estype, id string, body interface{}) (err error) {
	res, err := client.Index().
		Index(index).
		Type(estype).
		Id(id).
		BodyJson(body).
		Do(context.Background())
	if err != nil {
		logs.Debug("es insert  msg err", err)
	}

	logs.Debug(" es insert sucess, msg:%v", res)
	return
}

///初始化系统日志信息
func initAppLog(path string) (err error) {
	// config := make(map[string]interface{})
	// logpath := path + `\logagent\Logs`
	// //没有则创建
	// err = os.MkdirAll(logpath, os.ModeDir)
	// if err != nil {
	// 	config["filename"] = `longagent.log`
	// } else {
	// 	config["filename"] = path + `\logagent\Logs\longagent.log`
	// }
	// //设置不同级别的分开写
	// config["separate"] = []string{"error", "info", "debug"}

	// //输出调用的文件名和文件行号 默认是false
	// logs.EnableFuncCallDepth(true)
	// //异步输出 设置缓冲chan 为2
	// logs.Async(3)
	// //多文件 debug  error  等分开写

	// configJSON, err1 := json.Marshal(config)
	// if err1 != nil {
	// 	err = err1
	// 	err = logs.SetLogger(logs.AdapterMultiFile, `{"filename":"longagent.log"}`)
	// } else {
	// 	err = logs.SetLogger(logs.AdapterMultiFile, string(configJSON))
	// }

	//现在为了调试方便使用 输出到终端
	logs.SetLogger(logs.AdapterConsole)

	return
}
