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
type Task struct {
	TaskId    int
	TaskType  int
	ReduceNum int
	FileName  []string
}

type TaskArgs struct{}

// task type for worker current excute task
const (
	MapTask int = iota
	ReduceTask
	Wait
	Exit
)

// coordinator phase state
const (
	MapPhase int = iota
	ReducePhase
	Finish
)

// 任务状态类型
const (
	Working  int = iota // 此阶段在工作
	Waitting            // 此阶段在等待执行
	Done                // 此阶段已经做完
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
