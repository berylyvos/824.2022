package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskState struct {
	WorkerPid int
	fileName  string
	started   bool
	finished  bool
}

type Coordinator struct {
	timeTable     map[int]time.Time
	tmu           *sync.Mutex
	mapTasks      []*TaskState
	reduceTasks   []*TaskState
	mmu           *sync.Mutex
	rmu           *sync.Mutex
	cmu           *sync.Mutex
	mapTaskCnt    int
	mapDone       atomic.Bool
	reduceTaskCnt atomic.Int32
	allDone       atomic.Bool
	wg            *sync.WaitGroup
}

func (c *Coordinator) allocMapTask(workerPid int, reply *TaskReply) bool {
	c.mmu.Lock()
	defer c.mmu.Unlock()
	for i := 0; i < NMap; i++ {
		if !c.mapTasks[i].started {
			reply.TaskIndex = i
			reply.InputFileName = c.mapTasks[i].fileName
			c.mapTasks[i].started = true
			c.mapTasks[i].WorkerPid = workerPid
			return true
		}
	}
	return false
}

func (c *Coordinator) allocReduceTask(workerPid int, reply *TaskReply) bool {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for i := 0; i < NReduce; i++ {
		if !c.reduceTasks[i].started {
			reply.TaskIndex = i
			c.reduceTasks[i].started = true
			c.reduceTasks[i].WorkerPid = workerPid
			return true
		}
	}
	return false
}

func (c *Coordinator) recycleUnDoneTask(workerPid int) {
	if !c.mapDone.Load() {
		c.mmu.Lock()
		for i := 0; i < NMap; i++ {
			if c.mapTasks[i].WorkerPid == workerPid &&
				c.mapTasks[i].started && !c.mapTasks[i].finished {
				c.mapTasks[i].started = false
				// log.Printf("Recycle Map task #%v", i)
			}
		}
		c.mmu.Unlock()
	}

	c.rmu.Lock()
	for i := 0; i < NReduce; i++ {
		if c.reduceTasks[i].WorkerPid == workerPid &&
			c.reduceTasks[i].started && !c.reduceTasks[i].finished {
			c.reduceTasks[i].started = false
			// log.Printf("Recycle Reduce task #%v", i)
		}
	}
	c.rmu.Unlock()
}

func (c *Coordinator) checkCrashWorker() {
	for {
		tm := time.NewTimer(time.Second * 5)
		now := <-tm.C
		c.tmu.Lock()
		for wid, t := range c.timeTable {
			if t.Add(time.Second * 10).Before(now) {
				c.recycleUnDoneTask(wid)
			}
		}
		c.tmu.Unlock()
		tm.Reset(time.Second * 5)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleTaskRequest(args *TaskArg, reply *TaskReply) error {
	if c.allDone.Load() {
		reply.Type = kTaskTypeExit
		return nil
	}

	c.tmu.Lock()
	c.timeTable[args.WorkerPid] = time.Now()
	c.tmu.Unlock()

	if args.TaskDone {
		c.wg.Done()

		if args.TaskDoneType == kTaskTypeMap {
			c.mmu.Lock()
			c.mapTasks[args.TaskIndex].finished = true
			c.mmu.Unlock()

			c.cmu.Lock()
			c.mapTaskCnt--
			if c.mapTaskCnt == 0 {
				c.mapDone.Store(true)
			}
			c.cmu.Unlock()
		} else if args.TaskDoneType == kTaskTypeReduce {
			c.rmu.Lock()
			c.reduceTasks[args.TaskIndex].finished = true
			c.rmu.Unlock()
		}
	}

	if !c.mapDone.Load() {
		if c.allocMapTask(args.WorkerPid, reply) {
			reply.Type = kTaskTypeMap
			// log.Printf("Map task #%v -> worker [%v]", reply.TaskIndex, args.WorkerPid)
			return nil
		}
	} else if c.allocReduceTask(args.WorkerPid, reply) {
		reply.Type = kTaskTypeReduce
		// log.Printf("Reduce task #%v -> worker [%v]", reply.TaskIndex, args.WorkerPid)
		return nil
	}

	reply.Type = kTaskTypeNone
	// log.Printf("No task to distribute to worker [%v]", args.WorkerPid)
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

	c.allDone.Store(true)
	log.Println("Coordinator Exit...")

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		timeTable:   make(map[int]time.Time),
		mapTasks:    make([]*TaskState, 0, NMap),
		reduceTasks: make([]*TaskState, 0, nReduce),
		mapTaskCnt:  NMap,
		tmu:         new(sync.Mutex),
		mmu:         new(sync.Mutex),
		rmu:         new(sync.Mutex),
		cmu:         new(sync.Mutex),
		wg:          new(sync.WaitGroup),
	}

	for i := 0; i < NMap; i++ {
		c.mapTasks = append(c.mapTasks, &TaskState{
			fileName: files[i],
			started:  false,
			finished: false,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, &TaskState{
			started:  false,
			finished: false,
		})
	}

	c.reduceTaskCnt.Store(int32(nReduce))
	c.mapDone.Store(false)
	c.allDone.Store(false)
	c.wg.Add(NMap + nReduce)

	c.server()
	go c.checkCrashWorker()

	return &c
}
