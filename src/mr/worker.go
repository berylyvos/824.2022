package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	args := TaskArg{}
	args.WorkerPid = os.Getpid()
	reply := TaskReply{}

	ok := call("Coordinator.HandleTaskRequest", &args, &reply)
	if ok {
		if reply.InputFileName != "" {
			filename := reply.InputFileName
			f, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(f)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			f.Close()

			kva := mapf(filename, string(content))
			intermediateFileName := fmt.Sprintf("mr-%d-", reply.MapTaskIndex)
			for _, kv := range kva {
				reduceIdx := ihash(kv.Key) % reply.NReduce
				fn := intermediateFileName + strconv.Itoa(reduceIdx)
				intermediateFile, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
				if err != nil {
					log.Fatalf("cannot open %v, %v", fn, err)
				}

				enc := json.NewEncoder(intermediateFile)
				enc.Encode(&kv)
				intermediateFile.Close()
			}
		}
	} else {
		fmt.Printf("worker %v call failed!\n", args.WorkerPid)
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

// send an RPC request to the coordinator, wait for the response.
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
