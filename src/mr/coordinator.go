package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	//TODO:这里把status放到task里怎么样
	MapStatus     map[int]int //0 new, 1 in pending, 2 done
	ReduceStatus  map[int]int //0 new, 1 in pending, 2 done
	MapTasks      map[int]*Task
	ReduceTasks   map[int]*Task
	ExpireSeconds int64
}

var (
	lock    sync.Mutex
	started bool
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// init部分是AI写的
func (c *Coordinator) initTasks(files []string, nReduce int) {
	c.MapStatus = make(map[int]int)
	c.ReduceStatus = make(map[int]int)
	c.MapTasks = make(map[int]*Task)
	c.ReduceTasks = make(map[int]*Task)

	c.loadMapTasks(files, nReduce)
	c.initReduceTasks(len(files), nReduce)

	started = true
}

func (c *Coordinator) loadMapTasks(files []string, nReduce int) {
	for i, file := range files {
		c.MapTasks[i] = &Task{
			TaskId:    i,
			TaskType:  0, // map
			NReduce:   nReduce,
			NMap:      len(files),
			Version:   0,
			FileURL:   file,
			StartTime: time.Time{}, // 还没开始分配
		}
		c.MapStatus[i] = 0
	}
}

func (c *Coordinator) initReduceTasks(nMap, nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = &Task{
			TaskId:    i,
			TaskType:  1,       // reduce
			NReduce:   nReduce, // 虽然当前 reduce worker 没用到，带着也没坏处
			NMap:      nMap,
			Version:   0,
			FileURL:   "",
			StartTime: time.Time{},
		}
		c.ReduceStatus[i] = 0
	}
}

// TODO:添加协程扫描过期任务
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *Task) error {
	//其实下面的代码明显可以优化,但是为了可读性就不弄了
	//直接一把大锁梭哈
	lock.Lock()
	defer lock.Unlock()
	if len(c.MapTasks) > 0 {
		for _, task := range c.MapTasks {
			if c.MapStatus[task.TaskId] == 1 {
				//获取当前时间
				currentTime := time.Now()
				//如果当前时间-过期时间>10s,说明这个任务已经过期了,需要重新分配
				if currentTime.Sub(task.StartTime) > time.Duration(c.ExpireSeconds)*time.Second {

					//再次次检查任务状态,如果还是1,说明这个任务确实过期了,需要重新分配
					if c.MapStatus[task.TaskId] == 1 {
						task.StartTime = time.Now()
						task.Version++
						*reply = *task
						return nil

					}
				}
			} else {
				//再次检查
				if c.MapStatus[task.TaskId] == 0 {
					c.MapStatus[task.TaskId] = 1
					task.StartTime = time.Now()
					task.Version++
					*reply = *task
					return nil
				}
			}
		}
	} else if len(c.ReduceTasks) > 0 {
		for _, task := range c.ReduceTasks {
			if c.ReduceStatus[task.TaskId] == 1 {
				//获取当前时间
				currentTime := time.Now()
				//如果当前时间-过期时间>10s,说明这个任务已经过期了,需要重新分配
				if currentTime.Sub(task.StartTime) > time.Duration(c.ExpireSeconds)*time.Second {
					//再次次检查任务状态,如果还是1,说明这个任务确实过期了,需要重新分配
					if c.ReduceStatus[task.TaskId] == 1 {
						task.StartTime = time.Now()
						task.Version++
						*reply = *task
						return nil
					}
				}
			} else {
				//再次检查
				if c.ReduceStatus[task.TaskId] == 0 {
					c.ReduceStatus[task.TaskId] = 1
					task.StartTime = time.Now()
					task.Version++
					*reply = *task
					return nil
				}
			}
		}
	}
	task := Task{
		TaskId:    -1,
		TaskType:  2,
		Version:   0,
		FileURL:   "",
		StartTime: time.Now(),
	}
	*reply = task
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *bool) error {
	lock.Lock()
	defer lock.Unlock()
	valid := c.checkTask(args.TaskId, args.TaskType, args.Version)
	if !valid {
		*reply = false
		return nil
	}
	switch args.TaskType {
	case 0:
		delete(c.MapTasks, args.TaskId)
		c.MapStatus[args.TaskId] = 2
	case 1:
		delete(c.ReduceTasks, args.TaskId)
		c.ReduceStatus[args.TaskId] = 2
	}
	*reply = true
	return nil
}

func (c *Coordinator) checkTask(taskId int, taskType int, version int) bool {
	switch taskType {
	case 0:
		if c.MapStatus[taskId] == 2 || c.MapStatus[taskId] == 0 || version != c.MapTasks[taskId].Version {
			return false
		}
	case 1:
		if c.ReduceStatus[taskId] == 2 || c.ReduceStatus[taskId] == 0 || version != c.ReduceTasks[taskId].Version {
			return false
		}
	}
	return true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//我这里先不上锁试试,盒盒盒
	//测试后老实了,还是得加锁
	lock.Lock()
	defer lock.Unlock()
	if !started {
		return false
	}
	ret := len(c.MapTasks) == 0 && len(c.ReduceTasks) == 0
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.ExpireSeconds = 10
	// Your code here.
	c.initTasks(files, nReduce)

	c.server(sockname)
	return &c
}
