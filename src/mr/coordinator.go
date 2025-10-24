package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Status         int
	ReduceNumber   int
	MapTask        chan *Task     //map task channel
	ReduceTask     chan *Task     //reduce task channel
	taskMetaHolder TaskMetaHolder // 存着task
	TaskId         int
	files          []string //file content
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	status    int   // 任务的状态
	TaskAdr   *Task // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
	startTIme time.Time
}

var mu sync.Mutex

func (c *Coordinator) CrashDetect() {
	for true {
		time.Sleep(1 * time.Second)
		mu.Lock()
		if c.Status == Finish {
			mu.Unlock()
			break
		}
		for _, info := range c.taskMetaHolder.MetaMap {
			if info.status == Working && time.Since(info.startTIme) >= 10*time.Second {
				info.status = Waitting
				if info.TaskAdr.TaskType == MapTask {
					c.MapTask <- info.TaskAdr
				} else if info.TaskAdr.TaskType == ReduceTask {
					c.ReduceTask <- info.TaskAdr
				}
			}
		}

		mu.Unlock()
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PollWork(args *TaskArgs, task *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.Status {
	case MapPhase:
		if len(c.MapTask) > 0 {
			*task = *<-c.MapTask
			if !c.taskMetaHolder.judgeState(task.TaskId) {
				fmt.Printf("current taskId %d is running\n", task.TaskId)
			}
		} else {
			task.TaskType = Wait
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(c.ReduceTask) > 0 {
			*task = *<-c.ReduceTask
			if !c.taskMetaHolder.judgeState(task.TaskId) {
				fmt.Printf("current taskId %d is running\n", task.TaskId)
			}
		} else {
			task.TaskType = Wait
			if c.taskMetaHolder.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case Finish:
		task.TaskType = Exit
	default:
		panic("The phase undefined ! ! !")
	}

	return nil
}

func (c *Coordinator) MarkFinished(args Task, task *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		info, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && info.status == Working {
			info.status = Done
		} else {
			fmt.Printf("taskId %d is  finished,please stop! \n", args.TaskId)
		}
		break
	case ReduceTask:
		info, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && info.status == Working {
			info.status = Done
		} else {
			fmt.Printf("taskId %d is  finished,please stop! \n", args.TaskId)
		}
		break
	default:
		panic("error")
	}

	return nil
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.status != Waitting {
		return false
	}
	taskInfo.status = Working
	taskInfo.startTIme = time.Now()
	return true
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (c *Coordinator) startReduceTask() {
	for i := 0; i < c.ReduceNumber; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileName:  selectReduceName(i),
			ReduceNum: c.ReduceNumber,
		}
		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			status:  Waitting, // 任务等待被执行
			TaskAdr: &task,    // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		c.ReduceTask <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) startMapTask() {
	for _, file := range c.files {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  MapTask,
			FileName:  []string{file},
			ReduceNum: c.ReduceNumber,
		}
		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			status:  Waitting, // 任务等待被执行
			TaskAdr: &task,    // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		c.MapTask <- &task
	}
}

// 将接受taskMetaInfo储存进MetaHolder里
func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

func (c *Coordinator) toNextPhase() {
	if c.Status == MapPhase {
		c.startReduceTask()
		c.Status = ReducePhase
	} else if c.Status == ReducePhase {
		c.Status = Finish
	}
}

func (t *TaskMetaHolder) checkTaskDone() bool {
	var mapTaskDone, mapTaskUnDone, reduceDone, reduceUnDone int
	for _, info := range t.MetaMap {
		if info.TaskAdr.TaskType == MapTask {
			if info.status == Done {
				mapTaskDone++
			} else {
				mapTaskUnDone++
			}
		} else if info.TaskAdr.TaskType == ReduceTask {
			if info.status == Done {
				reduceDone++
			} else {
				reduceUnDone++
			}
		}
	}
	if (mapTaskDone > 0 && mapTaskUnDone == 0) && (reduceDone == 0 && reduceUnDone == 0) {
		return true
	} else {
		if reduceDone > 0 && reduceUnDone == 0 {
			return true
		}
	}

	return false
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
	mu.Lock()
	defer mu.Unlock()
	if c.Status == Finish {
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceTask:   make(chan *Task, nReduce),
		MapTask:      make(chan *Task, len(files)),
		ReduceNumber: nReduce,
		files:        files,
		Status:       MapPhase,
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	c.startMapTask()
	c.server()
	go c.CrashDetect()
	return &c
}
