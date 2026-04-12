package mr

import "time"

//
// RPC definitions.
//
// remember to capitalize all names.
//

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
	TaskType  int //0:map 1:reduce 2:wait 3:exit
	NReduce   int
	NMap      int
	Version   int
	FileURL   string
	StartTime time.Time
}

type GetTaskArgs struct {
}

type ReportTaskArgs struct {
	TaskId   int
	TaskType int
	Version  int
}
