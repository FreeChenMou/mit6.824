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
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	flag := true

	for flag {
		task := getWorker()
		switch task.TaskType {
		case MapTask:
			DoMapTask(mapf, &task)
			callDone(task)
		case Wait:
			time.Sleep(time.Second)
		case ReduceTask:
			DoReduceTask(reducef, &task)
			callDone(task)
		case Exit:
			flag = false
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func getWorker() Task {
	// send the RPC request, wait for the reply.
	task := Task{}
	args := TaskArgs{}
	ok := call("Coordinator.PollWork", &args, &task)
	if ok {
	} else {
		fmt.Println("Coordinator.PollWork Error")
	}

	return task
}

func callDone(task Task) {
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.MarkFinished", task, &task)
	if ok {
	} else {
		fmt.Println("Coordinator.MarkFinished Error")
	}

}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := shuffle(task.FileName)
	dir, _ := os.Getwd()
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(name []string) []KeyValue {
	var kva []KeyValue

	for _, file := range name {
		open, err := os.Open(file)
		if err != nil {
			fmt.Println("cannot open", file)
		}
		dec := json.NewDecoder(open)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		open.Close()
	}

	sort.Sort(ByKey(kva))
	return kva
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	var intermediate []KeyValue
	filename := task.FileName[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 通过io工具包获取conten,作为mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// map返回一组KV结构体数组
	intermediate = mapf(filename, string(content))

	//initialize and loop over []KeyValue
	rn := task.ReduceNum
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		dir, _ := os.Getwd()
		//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
		temp, _ := ioutil.TempFile(dir, "mr-tmp-*")

		enc := json.NewEncoder(temp)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		temp.Close()
		os.Rename(temp.Name(), oname)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the task.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()

	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
