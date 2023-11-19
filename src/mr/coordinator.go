package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type FileState struct {
	started bool
	idx     int
}

type Coordinator struct {
	fileMap map[string]*FileState
	mu      sync.Mutex

	mapDone bool
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleTaskRequest(args *TaskArg, reply *TaskReply) error {
	log.Printf("hanldle task request from worker %v", args.WorkerPid)

	if !c.mapDone {
		c.mu.Lock()
		for fn, st := range c.fileMap {
			if !st.started {
				reply.InputFileName = fn
				reply.MapTaskIndex = c.fileMap[fn].idx
				c.fileMap[fn].started = true
				break
			}
		}
		c.mu.Unlock()
		reply.NReduce = c.nReduce
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		fileMap: make(map[string]*FileState),
		mapDone: false,
		nReduce: nReduce,
	}
	for i, fn := range files {
		c.fileMap[fn] = &FileState{started: false, idx: i}
	}

	c.server()
	return &c
}
