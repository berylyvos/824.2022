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

type MRTask struct {
	idx       int
	workerPid int
	started   bool
	finished  bool
	fileName  string
}

type Coordinator struct {
	timeTable map[int]time.Time
	tmu       *sync.Mutex

	mapTasks    []MRTask
	reduceTasks []MRTask
	mmu         *sync.Mutex
	rmu         *sync.Mutex
	mapChan     chan MRTask
	reduceChan  chan MRTask

	mapTaskCnt atomic.Int32
	mapDone    atomic.Bool
	allDone    atomic.Bool
	wg         *sync.WaitGroup
}

func (c *Coordinator) allocMapTask(workerPid int, reply *TaskReply) bool {
	if len(c.mapChan) == 0 {
		return false
	}

	task := <-c.mapChan
	reply.Type = kTaskTypeMap
	reply.TaskIndex = task.idx
	reply.InputFileName = task.fileName
	c.mmu.Lock()
	c.mapTasks[task.idx].started = true
	c.mapTasks[task.idx].workerPid = workerPid
	c.mmu.Unlock()
	return true
}

func (c *Coordinator) allocReduceTask(workerPid int, reply *TaskReply) bool {
	if len(c.reduceChan) == 0 {
		return false
	}

	task := <-c.reduceChan
	reply.Type = kTaskTypeReduce
	reply.TaskIndex = task.idx
	c.rmu.Lock()
	c.reduceTasks[task.idx].started = true
	c.reduceTasks[task.idx].workerPid = workerPid
	c.rmu.Unlock()
	return true
}

func (c *Coordinator) recycleUnDoneTask(workerPid int) {
	if !c.mapDone.Load() {
		c.mmu.Lock()
		for i := 0; i < NMap; i++ {
			if c.mapTasks[i].workerPid == workerPid &&
				c.mapTasks[i].started && !c.mapTasks[i].finished {
				c.mapTasks[i].started = false
				c.mapChan <- c.mapTasks[i]
				log.Printf("Recycle Map task #%v", i)
				break
			}
		}
		c.mmu.Unlock()
	} else {
		c.rmu.Lock()
		for i := 0; i < NReduce; i++ {
			if c.reduceTasks[i].workerPid == workerPid &&
				c.reduceTasks[i].started && !c.reduceTasks[i].finished {
				c.reduceTasks[i].started = false
				c.reduceChan <- c.reduceTasks[i]
				log.Printf("Recycle Reduce task #%v", i)
				break
			}
		}
		c.rmu.Unlock()
	}
}

func (c *Coordinator) checkCrashWorker() {
	go func() {
		tm := time.NewTimer(time.Second * 5)
		for {
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
	}()
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

			if c.mapTaskCnt.Add(-1) == 0 {
				c.mapDone.Store(true)
			}
		} else if args.TaskDoneType == kTaskTypeReduce {
			c.rmu.Lock()
			c.reduceTasks[args.TaskIndex].finished = true
			c.rmu.Unlock()
		}
	}

	if !c.mapDone.Load() {
		if c.allocMapTask(args.WorkerPid, reply) {
			return nil
		}
	} else if c.allocReduceTask(args.WorkerPid, reply) {
		return nil
	}

	reply.Type = kTaskTypeNone
	log.Printf("No task to distribute to worker [%v]", args.WorkerPid)
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
		mapTasks:    make([]MRTask, 0, NMap),
		reduceTasks: make([]MRTask, 0, nReduce),
		mapChan:     make(chan MRTask, NMap),
		reduceChan:  make(chan MRTask, nReduce),
		tmu:         new(sync.Mutex),
		mmu:         new(sync.Mutex),
		rmu:         new(sync.Mutex),
		wg:          new(sync.WaitGroup),
	}

	for i := 0; i < NMap; i++ {
		task := MRTask{
			idx:      i,
			started:  false,
			finished: false,
			fileName: files[i],
		}
		c.mapTasks = append(c.mapTasks, task)
		c.mapChan <- task
	}

	for i := 0; i < nReduce; i++ {
		task := MRTask{
			idx:      i,
			started:  false,
			finished: false,
		}
		c.reduceTasks = append(c.reduceTasks, task)
		c.reduceChan <- task
	}

	c.mapTaskCnt.Store(int32(NMap))
	c.mapDone.Store(false)
	c.allDone.Store(false)
	c.wg.Add(NMap + nReduce)

	c.server()
	c.checkCrashWorker()

	return &c
}
