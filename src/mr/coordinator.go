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

const (
	PhaseMap = iota
	PhaseReduce
	PhaseDone
)

const (
	TaskInit TaskState = iota
	TaskStart
	TaskFinish
)

type (
	TaskState uint8

	MRTask struct {
		idx       int
		workerPid int
		state     TaskState
		fileName  string
	}
)

type Coordinator struct {
	mapTasks    []MRTask
	reduceTasks []MRTask
	mmu         *sync.Mutex
	rmu         *sync.Mutex
	mapChan     chan MRTask
	reduceChan  chan MRTask
	mapTaskCnt  atomic.Int32
	phase       atomic.Uint32
	wg          *sync.WaitGroup
	timeTable   *sync.Map
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
	c.mapTasks[task.idx].state = TaskStart
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
	c.reduceTasks[task.idx].state = TaskStart
	c.reduceTasks[task.idx].workerPid = workerPid
	c.rmu.Unlock()
	return true
}

func (c *Coordinator) recycleUnDoneTask(workerPid int) {
	if c.phase.Load() == PhaseMap {
		c.mmu.Lock()
		for i := 0; i < NMap; i++ {
			if c.mapTasks[i].workerPid == workerPid &&
				c.mapTasks[i].state == TaskStart {
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
				c.reduceTasks[i].state == TaskStart {
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
			c.timeTable.Range(func(k, v interface{}) bool {
				workerPid := k.(int)
				lastRequestTime := v.(time.Time)
				if lastRequestTime.Add(time.Second * 10).Before(now) {
					c.recycleUnDoneTask(workerPid)
				}
				return true
			})
			tm.Reset(time.Second * 5)
		}
	}()
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleTaskRequest(args *TaskArg, reply *TaskReply) error {
	if c.phase.Load() == PhaseDone {
		reply.Type = kTaskTypeExit
		return nil
	}

	c.timeTable.Store(args.WorkerPid, time.Now())

	if args.TaskDone {
		c.wg.Done()

		if args.TaskDoneType == kTaskTypeMap {
			c.mmu.Lock()
			c.mapTasks[args.TaskIndex].state = TaskFinish
			c.mmu.Unlock()

			if c.mapTaskCnt.Add(-1) == 0 {
				c.phase.Store(PhaseReduce)
			}
		} else if args.TaskDoneType == kTaskTypeReduce {
			c.rmu.Lock()
			c.reduceTasks[args.TaskIndex].state = TaskFinish
			c.rmu.Unlock()
		}
	}

	if c.phase.Load() == PhaseMap {
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

	c.phase.Store(PhaseDone)
	log.Println("Coordinator Exit...")

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]MRTask, 0, NMap),
		reduceTasks: make([]MRTask, 0, nReduce),
		mapChan:     make(chan MRTask, NMap),
		reduceChan:  make(chan MRTask, nReduce),
		mmu:         new(sync.Mutex),
		rmu:         new(sync.Mutex),
		wg:          new(sync.WaitGroup),
		timeTable:   new(sync.Map),
	}

	for i := 0; i < NMap; i++ {
		task := MRTask{
			idx:      i,
			state:    TaskInit,
			fileName: files[i],
		}
		c.mapTasks = append(c.mapTasks, task)
		c.mapChan <- task
	}

	for i := 0; i < nReduce; i++ {
		task := MRTask{
			idx:   i,
			state: TaskInit,
		}
		c.reduceTasks = append(c.reduceTasks, task)
		c.reduceChan <- task
	}

	c.mapTaskCnt.Store(int32(NMap))
	c.phase.Store(PhaseMap)
	c.wg.Add(NMap + nReduce)

	c.server()
	c.checkCrashWorker()

	return &c
}
