package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	NMap    int = 8
	NReduce int = 10
)

type TaskType uint8

const (
	kTaskTypeMap TaskType = iota
	kTaskTypeReduce
	kTaskTypeWait
	kTaskTypeNone
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskArg struct {
	WorkerPid int
	TaskDone  bool
}

type TaskReply struct {
	Type          TaskType
	InputFileName string
	TaskIndex     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
