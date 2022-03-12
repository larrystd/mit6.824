package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MapF func(string, string) []KeyValue
type ReduceF func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(mapf MapF, reducef ReduceF) {
	log.SetOutput(ioutil.Discard)
	for {
		time.Sleep(1 * time.Second)
		task := Task{}
		// 利用rpc请求Master给予Task, 参数包括Placeholder{}和task, 调用后task被赋值
		call("Master.RequestTask", &Placeholder{}, &task)

		if task.Operation == ToWait {
			continue
		}

		if task.IsMap {
			log.Printf("received map job %s", task.Map.Filename)
			err := handleMap(task, mapf)
			if err != nil {
				log.Fatalf(err.Error())
				return
			}
		} else {
			log.Printf("received reduce job %d %v", task.Reduce.Id, task.Reduce.IntermediateFilenames)
			err := handleReduce(task, reducef)
			if err != nil {
				log.Fatalf(err.Error())
				return
			}
		}
	}
}

// 从master获取的task, 执行mapper操作
func handleMap(task Task, mapf MapF) error {
	filename := task.Map.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()

	kva := mapf(filename, string(content)) // kva是word列表
	var encoders []*json.Encoder
	// 每个mapper task会有n个reduce
	for i := 0; i < task.NReduce; i++ {
		f, err := os.Create(fmt.Sprintf("mr-%d-%d", task.Map.Id, i))
		if err != nil {
			log.Fatalf("cannot create intermediate result file")
		}
		encoders = append(encoders, json.NewEncoder(f))
	}

	// mapper负责将一个task对应的数据分成N个reduce的小块, 并保存到mr-%d-%d文件中
	// 即将kv根据kv.Key写到对应的文件块中(并不是依次写, 而是基于哈希函数分配到哪个文件中)
	for _, kv := range kva {
		_ = encoders[ihash(kv.Key)%task.NReduce].Encode(&kv)
	}
	// 执行完毕
	call("Master.Finish", &FinishArgs{IsMap: true, Id: task.Map.Id}, &Placeholder{})
	return nil
}

// reduce任务, 如果所有的MapTask都已经完成，创建ReduceTask，转入Reduce阶段
// 每个mapper对应n个reduce, reduce任务只处理每个reduce中的数据, 这些数据可能来自多个文件
func handleReduce(task Task, reducef ReduceF) error {
	var kva []KeyValue
	// 每个reduce处理的内容来自多个文件
	for _, filename := range task.Reduce.IntermediateFilenames {
		iFile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// 将文件读取出
		dec := json.NewDecoder(iFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err = iFile.Close()
		if err != nil {
			log.Fatalf("cannot close %v", filename)
		}
	}

	// kva排序
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", task.Reduce.Id)
	temp, err := os.CreateTemp(".", oname)
	if err != nil {
		log.Fatalf("cannot create reduce result tempfile %s", oname)
		return err
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		// 每个key(word)出现values次, 需要写到对应的output文件
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, _ = fmt.Fprintf(temp, "%v %v\n", kva[i].Key, output)

		i = j
	}
	err = os.Rename(temp.Name(), oname)
	if err != nil {
		return err
	}

	call("Master.Finish", &FinishArgs{IsMap: false, Id: task.Reduce.Id}, &Placeholder{})
	return nil
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) // 利用rpc和master通信
	if err == nil {
		return true
	}

	fmt.Println(err)
	os.Exit(0)
	return false
}
