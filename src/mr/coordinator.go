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
		mu        *sync.Mutex
	}
)

type Coordinator struct {
	mapTasks    []MRTask
	reduceTasks []MRTask
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

	c.mapTasks[task.idx].mu.Lock()
	defer c.mapTasks[task.idx].mu.Unlock()
	c.mapTasks[task.idx].state = TaskStart
	c.mapTasks[task.idx].workerPid = workerPid
	return true
}

func (c *Coordinator) allocReduceTask(workerPid int, reply *TaskReply) bool {
	if len(c.reduceChan) == 0 {
		return false
	}

	task := <-c.reduceChan
	reply.Type = kTaskTypeReduce
	reply.TaskIndex = task.idx

	c.reduceTasks[task.idx].mu.Lock()
	defer c.reduceTasks[task.idx].mu.Unlock()
	c.reduceTasks[task.idx].state = TaskStart
	c.reduceTasks[task.idx].workerPid = workerPid
	return true
}

func (c *Coordinator) recycleUnDoneTask(workerPid int) {
	if c.phase.Load() == PhaseMap {
		for i := 0; i < NMap; i++ {
			c.mapTasks[i].mu.Lock()
			if c.mapTasks[i].workerPid == workerPid &&
				c.mapTasks[i].state == TaskStart {
				c.mapChan <- c.mapTasks[i]
				c.mapTasks[i].mu.Unlock()
				log.Printf("Recycle Map task #%v", i)
				break
			}
			c.mapTasks[i].mu.Unlock()
		}
	} else {
		for i := 0; i < NReduce; i++ {
			c.reduceTasks[i].mu.Lock()
			if c.reduceTasks[i].workerPid == workerPid &&
				c.reduceTasks[i].state == TaskStart {
				c.reduceChan <- c.reduceTasks[i]
				c.reduceTasks[i].mu.Unlock()
				log.Printf("Recycle Reduce task #%v", i)
				break
			}
			c.reduceTasks[i].mu.Unlock()
		}
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
			c.mapTasks[args.TaskIndex].mu.Lock()
			c.mapTasks[args.TaskIndex].state = TaskFinish
			c.mapTasks[args.TaskIndex].mu.Unlock()

			if c.mapTaskCnt.Add(-1) == 0 {
				c.phase.Store(PhaseReduce)
			}
		} else if args.TaskDoneType == kTaskTypeReduce {
			c.reduceTasks[args.TaskIndex].mu.Lock()
			c.reduceTasks[args.TaskIndex].state = TaskFinish
			c.reduceTasks[args.TaskIndex].mu.Unlock()
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
		wg:          new(sync.WaitGroup),
		timeTable:   new(sync.Map),
	}

	for i := 0; i < NMap; i++ {
		task := MRTask{
			idx:      i,
			state:    TaskInit,
			fileName: files[i],
			mu:       new(sync.Mutex),
		}
		c.mapTasks = append(c.mapTasks, task)
		c.mapChan <- task
	}

	for i := 0; i < nReduce; i++ {
		task := MRTask{
			idx:   i,
			state: TaskInit,
			mu:    new(sync.Mutex),
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
