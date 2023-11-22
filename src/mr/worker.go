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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func intermediateFileName(x, y int) string {
	return fmt.Sprintf("mr-%d-%d", x, y)
}

func outputFileName(x int) string {
	return fmt.Sprintf("mr-out-%d", x)
}

func doMap(mapf func(string, string) []KeyValue, reply *TaskReply) {
	log.Printf("worker [%v] doMap #%v", os.Getpid(), reply.TaskIndex)

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
	mkva := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceIdx := ihash(kv.Key) % NReduce
		mkva[reduceIdx] = append(mkva[reduceIdx], kv)
	}

	for i := 0; i < NReduce; i++ {
		f, _ := os.Create(intermediateFileName(reply.TaskIndex, i))
		enc := json.NewEncoder(f)
		for _, kv := range mkva[i] {
			enc.Encode(&kv)
		}
		f.Close()
	}
}

func doReduce(reducef func(string, []string) string, reply *TaskReply) {
	log.Printf("worker [%v] doReduce #%v", os.Getpid(), reply.TaskIndex)

	kva := []KeyValue{}

	for i := 0; i < NMap; i++ {
		fn := intermediateFileName(i, reply.TaskIndex)
		f, err := os.Open(fn)
		if err != nil {
			log.Fatalf("cannot open %v", fn)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	ofile, _ := os.Create(outputFileName(reply.TaskIndex))

	sort.Sort(ByKey(kva))
	i, sz := 0, len(kva)
	for i < sz {
		j := i + 1
		for j < sz && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := TaskArg{}
	args.TaskDone = false
	args.WorkerPid = os.Getpid()

	for {
		reply := TaskReply{}

		ok := call("Coordinator.HandleTaskRequest", &args, &reply)
		if ok {
			switch reply.Type {
			case kTaskTypeMap:
				doMap(mapf, &reply)
				args.TaskDone = true
				args.TaskIndex = reply.TaskIndex
				args.TaskDoneType = kTaskTypeMap
			case kTaskTypeReduce:
				doReduce(reducef, &reply)
				args.TaskDone = true
				args.TaskIndex = reply.TaskIndex
				args.TaskDoneType = kTaskTypeReduce
			case kTaskTypeNone:
				time.Sleep(time.Second * 3)
				args.TaskDone = false
			case kTaskTypeExit:
				log.Printf("worker [%v] exit...", args.WorkerPid)
				return
			}
		} else {
			break
		}

		time.Sleep(time.Millisecond * 500)
	}

	log.Printf("worker [%v] exit...", args.WorkerPid)
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
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
