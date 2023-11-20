package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

type Coordinator struct {
	files         []string
	fileIdx       int
	mu            *sync.RWMutex
	cmu           *sync.Mutex
	cond          *sync.Cond
	mapTaskCnt    int
	reduceTaskCnt atomic.Int32
	wg            *sync.WaitGroup
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleTaskRequest(args *TaskArg, reply *TaskReply) error {
	if args.TaskDone {
		c.wg.Done()
		c.cmu.Lock()
		c.mapTaskCnt--
		if c.mapTaskCnt == 0 {
			c.cond.Broadcast()
		}
		c.cmu.Unlock()
	}

	c.mu.RLock()
	idx := c.fileIdx
	c.mu.RUnlock()

	if idx >= NMap {
		c.cmu.Lock()
		for c.mapTaskCnt > 0 {
			c.cond.Wait()
		}
		c.cmu.Unlock()

		reduceIdx := c.reduceTaskCnt.Add(-1)
		if reduceIdx < 0 {
			reply.Type = kTaskTypeNone
			log.Println("No task to distribute!")
		} else {
			log.Printf("Reduce task #%v -> worker [%v]", reduceIdx, args.WorkerPid)
			reply.Type = kTaskTypeReduce
			reply.TaskIndex = int(reduceIdx)
		}
	} else {
		c.mu.Lock()
		reply.TaskIndex = c.fileIdx
		reply.InputFileName = c.files[c.fileIdx]
		c.fileIdx++
		c.mu.Unlock()

		reply.Type = kTaskTypeMap
		log.Printf("Map task #%v -> worker [%v]", reply.TaskIndex, args.WorkerPid)
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

	c.wg.Wait()

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		fileIdx:    0,
		mapTaskCnt: NMap,
		mu:         new(sync.RWMutex),
		cmu:        new(sync.Mutex),
		wg:         new(sync.WaitGroup),
	}
	c.reduceTaskCnt.Store(int32(nReduce))
	c.cond = sync.NewCond(c.cmu)
	c.wg.Add(NMap + nReduce)

	c.server()
	return &c
}
