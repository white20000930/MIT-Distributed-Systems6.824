package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
)

var lock sync.Mutex

type Coordinator struct {
	ReduceNum       int
	CurrentTaskid   int
	Prog            Progress
	Map_TaskChan    chan *Task
	Reduce_TaskChan chan *Task
	TaskSlice       []*Task
}

type Progress int

const (
	Maping Progress = iota
	Reducing
	AllDone
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) PollTask(args *struct{}, reply *Task) error {
	fmt.Println("+++Test+++ in PollTask")
	lock.Lock()
	defer lock.Unlock()

	switch c.Prog {
	case Maping:
		{
			if len(c.Map_TaskChan) > 0 { //have task to do
				fmt.Println("+++Test+++ in PollTask, len> 0")
				taskPtr := <-c.Map_TaskChan
				*reply = *taskPtr
				taskPtr.State = Working
			} else {
				fmt.Println("+++Test+++ in PollTask, WaitingTask")
				reply.TaskType = WaitingTask //when no task to do
				c.CheckProgress()
				fmt.Println("+++Test+++ in PollTask, before return")
				return nil

			}
		}
	case Reducing:
		{
			{
				if len(c.Reduce_TaskChan) > 0 { //have task to do
					taskPtr := <-c.Reduce_TaskChan
					*reply = *taskPtr
					taskPtr.State = Working
				} else {

					reply.TaskType = WaitingTask //when no task to do
					c.CheckProgress()
					return nil

				}
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *Task, reply *struct{}) error {
	lock.Lock()
	defer lock.Unlock()
	if c.TaskSlice[args.TaskId].State == Working {
		c.TaskSlice[args.TaskId].State = Done
	}
	fmt.Println("+++Test+++ in TaskDone", c.TaskSlice[args.TaskId].State)
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
	ret := false

	// Your code here.
	if c.Prog == AllDone {
		ret = true
	}
	return ret
}

// 1.create a Coordinator.
// 2.make all MapTasks
// 3.start a server
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("+++Test+++ in MakeCoordinator")
	c := Coordinator{
		ReduceNum:       nReduce,
		Map_TaskChan:    make(chan *Task, len(files)),
		Reduce_TaskChan: make(chan *Task, nReduce),
		Prog:            Maping,
	}

	c.makeMapTasks(files)

	c.server()
	return &c
}

// 1. make map tasks
// 2. append to TaskSlice
// 3. send to Map_TaskChan
func (c *Coordinator) makeMapTasks(files []string) {
	fmt.Println("+++Test+++ in makeMapTasks")
	for _, v := range files {
		id := c.CurrentTaskid
		c.CurrentTaskid++
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.ReduceNum,
			FileSlice: []string{v},
			State:     Waitingtodo,
		}

		c.TaskSlice = append(c.TaskSlice, &task)

		fmt.Println("make a map task :", &task)
		c.Map_TaskChan <- &task
	}

}

func (c *Coordinator) makeReduceTasks() {
	fmt.Println("+++Test+++ in makeReduceTasks")
	for i := 0; i < c.ReduceNum; i++ {
		id := c.CurrentTaskid
		c.CurrentTaskid++
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			ReduceNum: c.ReduceNum,
			FileSlice: PickReduceFiles(i),
			State:     Waitingtodo,
		}
		c.TaskSlice = append(c.TaskSlice, &task)

		fmt.Println("make a reduce task :", &task)
		c.Reduce_TaskChan <- &task
	}

}

func PickReduceFiles(reduceNum int) []string {
	fmt.Println("+++Test+++ in PickReduceFiles")
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	r := regexp.MustCompile(`mr-tmp-\d+-` + strconv.Itoa(reduceNum))
	for _, fi := range files {
		if r.MatchString(fi.Name()) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) CheckProgress() bool {
	fmt.Println("+++Test+++ in CheckProgress")

	for _, v := range c.TaskSlice {
		if v.State != Done {
			return false
		}
	}
	if c.Prog == Maping {
		c.makeReduceTasks()
		c.Prog = Reducing

	} else if c.Prog == Reducing {
		c.Prog = AllDone
	}

	return true
}
