package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	//"logCollection/logagent/tools"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/astaxie/beego/config"
	"github.com/astaxie/beego/logs"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/hpcloud/tail"
)

//CollectionInfo 需要收集日志的信息
type CollectionInfo struct {
	//tools.Slice
	Path   string `json:"logpath"` //路径
	Topic  string `json:"topic"`   //kakfa的topic名称
	update bool
	lock   sync.RWMutex
}

//MyConfig 配置的结构体
type MyConfig struct {
	kafkaAddr      string
	etcdAddr       string
	etcdkeycollect string
}

var (
	//Currentpath 当前的系统路径
	Currentpath string
	//CollentMap 存放etcd的里存储的key value
	CollentMap map[string]string
	//AppConfig 全局配置
	AppConfig MyConfig
	//CollectList 当前正在收集日志列表
	CollectList []CollectionInfo
)

func main() {
	currentpath, err := os.Getwd()
	Currentpath = currentpath + `\config\logagent.ini`
	//先初始化系统日志配置
	initAppLog(currentpath)
	//2.读系统配置文件
	AppConfig, err = loadConfig("ini", Currentpath)

	if err != nil {
		logs.Error("loadconfig err", err)
	} else {
		//根据配置文件读取etcd的配置
		endpointsetcd := []string{AppConfig.etcdAddr}

		CollentMap, err = initetcd(endpointsetcd, AppConfig.etcdkeycollect)

		//打印etcd获取的信息
		err = json.Unmarshal([]byte(CollentMap[AppConfig.etcdkeycollect]), &CollectList)
		if err != nil {
			logs.Error("json Unmarshal etcdkeycollect err:%v", err)
		}

		logs.Debug("获取的配置信息：%v", AppConfig)
		go watchetcdkey(endpointsetcd, AppConfig.etcdkeycollect)
		//根据etcd读取配置文件 开始跟踪日志
		endpoints := []string{AppConfig.kafkaAddr}
		lines := make(chan *tail.Line)
		for _, p := range CollectList {
			go readLog(lines, p)
			// 读取出来，放到kafka上即可
			go sendMsg(lines, p, endpoints)
		}

	}

	for {
		time.Sleep(10 * time.Second)
		logs.Debug("i am main===========")
	}
}

//LoadConfig 加载配置文件
func loadConfig(configType, path string) (myConfig MyConfig, err error) {
	defer func(myConfig *MyConfig) {
		logs.Debug("read kafka   addr=: ", myConfig.kafkaAddr)
		logs.Debug("read etcdaddr=: ", myConfig.etcdAddr)
		logs.Debug("read etcdkeycollect=: ", myConfig.etcdkeycollect)
	}(&myConfig)

	conf, err := config.NewConfig(configType, path)
	if err != nil {
		logs.Error("new config failed, err:", err)
	}

	logs.Debug("读取配置得路径是：", path)
	myConfig.kafkaAddr = conf.String("kafka::addr")
	if len(myConfig.kafkaAddr) == 0 {
		myConfig.kafkaAddr = "127.0.0.1:9092"
		err = errors.New("Not find server ip ,use default addr:127.0.0.1:9092")
	}

	myConfig.etcdAddr = conf.String("etcd::addr")
	if len(myConfig.etcdAddr) == 0 {
		err = errors.New("Not find etcd path,use defauly ip port:127.0.0.1:2379 ")
		myConfig.etcdAddr = "127.0.0.1:2379"
	}

	myConfig.etcdkeycollect = conf.String("etcd::keycollect")
	if len(myConfig.etcdkeycollect) == 0 {
		err = errors.New("Not find etcd keycollect")
		return
	}
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

//初始化etcd
func initetcd(endpoint []string, key string) (result map[string]string, err error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoint,
		DialTimeout: 5 * time.Second,
	})
	result = make(map[string]string, len(key))
	if err != nil {
		logs.Error("etcd clientv3.New err", err)
		return
	}
	logs.Debug("etcd clientv3.New success")
	defer cli.Close()

	//获取key所对应的值
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	resp, err1 := cli.Get(ctx, key)
	logs.Debug("etcd get key=%s sucess\n", key)
	cancel()
	if err1 != nil {
		logs.Error("cli.Get err", err1)
		err = err1
	}
	for _, ev := range resp.Kvs {
		logs.Debug("etcd get key=%s ,value=%s\n", ev.Key, ev.Value)
		result[string(ev.Key)] = string(ev.Value)
	}

	return
}

//获取kafka 跟日志路径后 并检测其变化
func watchetcdkey(endpoint []string, key string) {
	fmt.Println("watchetcdkey keys", key)
	result := make(map[string]string, len(key))
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoint,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("clientv3.New err", err)
	}
	defer cli.Close()
	//是否需要更新etcd的值
	b := false
	//监听该值的变化
	fmt.Println("watching keys", key)
	rch := cli.Watch(context.Background(), key, clientv3.WithPrefix())
	var (
		k, v string
	)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			k = string(ev.Kv.Key)
			v = string(ev.Kv.Value)
			if k == AppConfig.etcdkeycollect {
				switch ev.Type {
				case mvccpb.DELETE:
					logs.Error(fmt.Sprintf("key is DELETE,key=:%s", k))
					result[string(ev.Kv.Key)] = "DELETE"
					b = true
				case mvccpb.PUT:
					logs.Debug(fmt.Sprintf("key is update,key=:%s", k))
					if err != nil {
						logs.Error(fmt.Sprintf("cli.Watch getkey,key:%s, err:%s", k, err))
					} else {
						b = true
						result[k] = v
						// fmt.Println("updateKeys ", result)
						// updateKeys(&result)
						// logs.Debug(fmt.Sprintf("updateKeys:%v ", result))
					}

				default:
					logs.Debug(fmt.Sprintf("%s %q :%q \n", ev.Type, ev.Kv.Key, ev.Kv.Value))
				}
			}
		}
		if b {
			updateKeys(&result)
		}
	}
	// if b {
	//  fmt.Println("updateKeys ", result)
	//  updateKeys(&result)
	//  logs.Debug(fmt.Sprintf("updateKeys:%v ", result))
	// }

}
func updateKeys(result *map[string]string) {
	logs.Debug("updateKeys:%v ", result)
	endpoints := []string{AppConfig.kafkaAddr}
	for _, v := range *result {
		if v != "DELETE" {
			var collectTemplist []CollectionInfo
			err := json.Unmarshal([]byte(v), &collectTemplist)
			if err != nil {
				logs.Error("json Unmarshal etcdkeycollect err", err)
			}
			logs.Debug("update keys after json.Unmarshal collectTemplist:", collectTemplist)
			//todo停止以前的
			for _, coll := range collectTemplist {
				for _, call := range CollectList {
					if coll.Path != call.Path {
						//停止该项
						call.lock.Lock()
						//这样更新是错误的，更新不了
						call.Path = coll.Path
						call.Topic = coll.Topic
						call.update = true
						call.lock.Unlock()

						lines := make(chan *tail.Line)
						logs.Debug("start update new address ", coll.Path)
						go readLog(lines, coll)
						// 读取出来，放到kafka上即可
						go sendMsg(lines, coll, endpoints)
					} else {
						logs.Debug("path is same ,noneed update", coll.Path)
					}
				}
			}
			logs.Debug("update keys read send log CollectList:", CollectList)
		} else {
			//停止被删除路径的读取
		}

	}

}

//读取相应路径下的日志
func readLog(msgchan chan *tail.Line, collectionInfo CollectionInfo) {
	logs.Debug("tail.TailFile init CollectionInfo:%v", collectionInfo)
	tails, err := tail.TailFile(collectionInfo.Path, tail.Config{
		ReOpen: true,
		Follow: true,
		//Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	})

	if err != nil {
		logs.Error("tail.TailFile err:", err)
		return
	}
	logs.Debug("tail.TailFile init success")
	var (
		msg *tail.Line
		ok  bool
	)

	for {
		collectionInfo.lock.RLock()
		if collectionInfo.update {
			collectionInfo.lock.RUnlock()
			logs.Debug("check path:%s is update so return current  ", collectionInfo.Path)
			close(msgchan)
			return
		}
		logs.Info("============i am ready for read log of %s=========", collectionInfo.Path)

		collectionInfo.lock.RUnlock()
		if msg, ok = <-tails.Lines; !ok {
			logs.Error("tail file close, reopen filename :%s after 100ms\n", tails.Filename)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		msgchan <- msg
		logs.Debug("读取到msg的详细信息 时间:%v,内容：%v", msg.Time, msg.Text)
	}
}

//给kafka发送消息
func sendMsg(lines chan *tail.Line, collectionInfo CollectionInfo, endpoint []string) {

	config := sarama.NewConfig()
	//是否需要回复
	config.Producer.RequiredAcks = sarama.WaitForAll
	//消息分区 设置为随机的
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	//新建一个同步的发送者  地址是参数
	client, err := sarama.NewSyncProducer(endpoint, config)

	defer client.Close()
	if err != nil {
		logs.Error("sarama.NewAsyncProducer err:", err)
		return
	}
	logs.Info("start sendmsg to kafka:")
	var (
		pid     int32
		offset  int64
		msgsend *tail.Line
		ok      bool
	)
	for {
		//ok=false 代表chan关闭 应该结束
		logs.Info("start forever revice path:%s and sendmsg to kafka:", collectionInfo.Path)
		if msgsend, ok = <-lines; ok {
			logs.Info("sendmsg to kafak msg is %s:", msgsend.Text)

			msg := &sarama.ProducerMessage{
				Topic: collectionInfo.Topic,
				Value: sarama.StringEncoder(msgsend.Text),
			}
			pid, offset, err = client.SendMessage(msg)
			if err != nil {
				logs.Error("client.SendMesage err:", err)
				return
			}
			logs.Info("sendmsg to kafak success ,pid:%v, offset:%v", pid, offset)
		} else {
			logs.Error("check path:%s read chan is closed", collectionInfo.Path)
			return
		}
	}
}

//给kafka发送消息
func sendMsgAsync(lines chan *tail.Line, topic string, endpoint []string) {

	config := sarama.NewConfig()
	//是否需要回复
	config.Producer.RequiredAcks = sarama.WaitForAll
	//消息分区 设置为随机的
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	//新建一个同步的发送者  地址是参数
	client, err := sarama.NewSyncProducer(endpoint, config)

	defer client.Close()
	if err != nil {
		logs.Error("sarama.NewAsyncProducer err:", err)
		return
	}
	logs.Info("start sendmsg to kafka:")
	var (
		pid     int32
		offset  int64
		msgsend *tail.Line
		ok      bool
	)
	for {
		//ok=false 代表chan关闭 应该结束
		if msgsend, ok = <-lines; ok {
			logs.Info("sendmsg to kafak msg is %s:", msgsend.Text)

			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(msgsend.Text),
			}
			pid, offset, err = client.SendMessage(msg)
			if err != nil {
				logs.Error("client.SendMesage err:", err)
				return
			}
			logs.Info("sendmsg to kafak success ,pid:%v, offset:%v", pid, offset)
		} else {
			logs.Error("client.SendMesage chan close:", ok)
			return
		}
	}
}
