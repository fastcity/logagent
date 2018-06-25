package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type collectionInfo struct {
	Path  string `json:"logpath"` //路径
	Topic string `json:"topic"`   //kakfa的topic名称
}

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("clientv3.New err", err)
	}
	defer cli.Close()

	setkey := "/logagent/collect"
	rand.Seed(time.Now().Unix())
	name := rand.Intn(10)
	path := `D:\GoWork\test` + strconv.Itoa(name) + `.txt`
	fmt.Println("set path:", path)
	//path = `D:\GoWork\testone.txt`
	collect := collectionInfo{path, "path_topic"}
	value := []collectionInfo{collect}

	confjson, _ := json.Marshal(value)
	put(cli, setkey, string(confjson))
	get(cli, setkey)

	// fmt.Println("start update key after 10 second ....")

	// count := 10
	// for {
	// 	fmt.Println(count)
	// 	count--
	// 	time.Sleep(1 * time.Second)
	// 	if count == 0 {
	// 		break
	// 	}
	// }

	// collect = collectionInfo{`D:\GoWork\testtwo.txt`, "path_topic"}
	// value = []collectionInfo{collect}
	// confjson, _ = json.Marshal(value)
	// put(cli, setkey, string(confjson))

}

func put(cli *clientv3.Client, key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	fmt.Printf("cli Put key:%s,value:%s \n", key, value)
	_, err := cli.Put(ctx, key, value)
	cancel()
	if err != nil {
		fmt.Println("cli.Put err", err)
	}
}

func get(cli *clientv3.Client, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("cli.Get %s err:%v", key, err)
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("cli Get key:%s,value:%s\n", ev.Key, ev.Value)
	}
}
