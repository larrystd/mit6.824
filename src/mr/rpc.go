package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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

// Add your RPC definitions here.

type Placeholder struct{}

// ====== FinishTask =======

type FinishArgs struct {
	IsMap bool
	Id    int
}

// ====== Task =======

type TaskState int

const (
	Pending TaskState = iota
	Executing
	Finished
)

type MapTask struct {	// 每个filename对应一个MapTask执行之
	TaskMeta
	Filename string
}

type ReduceTask struct {
	TaskMeta
	IntermediateFilenames []string
}

type TaskMeta struct {		// 任务元数据
	State     TaskState
	StartTime time.Time
	Id        int
}

type TaskOperation int

const (
	ToWait TaskOperation = iota
	ToRun
)

type Task struct {	// 任务结构信息
	Operation TaskOperation
	IsMap     bool
	NReduce   int
	Map       MapTask
	Reduce    ReduceTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
