package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
// NOTE:这里嫌io麻烦直接ai了
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		task := CallGetTask()
		if task == nil {
			return
		}

		switch task.TaskType {
		case 0: // map
			content, err := os.ReadFile(task.FileURL)
			if err != nil {
				log.Printf("read map input %s failed: %v", task.FileURL, err)
				time.Sleep(time.Second)
				continue
			}

			kva := mapf(task.FileURL, string(content))

			buckets := make([][]KeyValue, task.NReduce)
			for _, kv := range kva {
				rid := ihash(kv.Key) % task.NReduce
				buckets[rid] = append(buckets[rid], kv)
			}

			success := true
			for rid := 0; rid < task.NReduce; rid++ {
				tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-*", task.TaskId, rid))
				if err != nil {
					log.Printf("create temp map file failed: %v", err)
					success = false
					break
				}

				enc := json.NewEncoder(tmpFile)
				for _, kv := range buckets[rid] {
					if err := enc.Encode(&kv); err != nil {
						log.Printf("encode kv failed: %v", err)
						success = false
						break
					}
				}

				tmpName := tmpFile.Name()
				tmpFile.Close()

				if !success {
					_ = os.Remove(tmpName)
					break
				}

				finalName := fmt.Sprintf("mr-%d-%d", task.TaskId, rid)
				if err := os.Rename(tmpName, finalName); err != nil {
					log.Printf("rename %s -> %s failed: %v", tmpName, finalName, err)
					_ = os.Remove(tmpName)
					success = false
					break
				}
			}

			if success {
				_ = CallReportTask(task)
			}

		case 1: // reduce
			intermediate := []KeyValue{}
			success := true

			for mid := 0; mid < task.NMap; mid++ {
				name := fmt.Sprintf("mr-%d-%d", mid, task.TaskId)

				f, err := os.Open(name)
				if err != nil {
					log.Printf("open intermediate file %s failed: %v", name, err)
					success = false
					break
				}

				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						if err == io.EOF {
							break
						}
						log.Printf("decode intermediate file %s failed: %v", name, err)
						success = false
						break
					}
					intermediate = append(intermediate, kv)
				}

				f.Close()
				if !success {
					break
				}
			}

			if !success {
				time.Sleep(time.Second)
				continue
			}

			sort.Sort(ByKey(intermediate))

			tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-*", task.TaskId))
			if err != nil {
				log.Printf("create temp reduce output failed: %v", err)
				time.Sleep(time.Second)
				continue
			}

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}

				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, values)
				if _, err := fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output); err != nil {
					log.Printf("write reduce output failed: %v", err)
					success = false
					break
				}

				i = j
			}

			tmpName := tmpFile.Name()
			tmpFile.Close()

			if !success {
				_ = os.Remove(tmpName)
				time.Sleep(time.Second)
				continue
			}

			finalName := fmt.Sprintf("mr-out-%d", task.TaskId)
			if err := os.Rename(tmpName, finalName); err != nil {
				log.Printf("rename reduce output failed: %v", err)
				_ = os.Remove(tmpName)
				time.Sleep(time.Second)
				continue
			}

			_ = CallReportTask(task)
		case 2: // wait
			time.Sleep(time.Second)

		case 3: // exit
			return
		}
	}
}

func CallReportTask(task *Task) bool {
	args := ReportTaskArgs{
		TaskId:   task.TaskId,
		TaskType: task.TaskType,
		Version:  task.Version,
	}
	reply := false
	ok := call("Coordinator.ReportTask", &args, &reply)
	return ok && reply
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetTask() *Task {
	args := GetTaskArgs{}
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
