package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type Task struct {
	TaskType  TaskType
	TaskId    int
	ReduceNum int
	FileSlice []string
	State     TaskState
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

type TaskState int

const (
	Waitingtodo TaskState = iota
	Working
	Done
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Println("+++Test+++ in Worker")
	keepworking := true
	for keepworking {
		task := CallTask()
		fmt.Println("before switch", task)
		switch task.TaskType {
		case MapTask:
			{
				task.State = Working
				DoMapTask(mapf, &task)
				callDone(task)
				task.State = Done
			}
		case ReduceTask:
			{
				task.State = Working
				DoReduceTask(reducef, &task)
				callDone(task)
				task.State = Done
			}
		case WaitingTask:
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				keepworking = false
			}
		}

	}
	//CallExample()

}
func CallTask() Task {
	fmt.Println("+++Test+++ in CallTask")
	args := struct{}{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		fmt.Println("CallTask reply", reply)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply
}

func callDone(task Task) {
	fmt.Println("+++Test+++ in callDone")
	reply := struct{}{}
	ok := call("Coordinator.TaskDone", &task, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)

	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	fmt.Println("+++Test+++ in DoMapTask")
	filename := task.FileSlice[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	intermediate := mapf(filename, string(content))

	rn := task.ReduceNum

	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}

	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

// for sorting by key.
type SortByKey []KeyValue

// for sorting by key.
func (a SortByKey) Len() int           { return len(a) }
func (a SortByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	fmt.Println("+++Test+++ in DoReduceTask")
	//sort
	var intermediate []KeyValue
	for _, file := range task.FileSlice {
		fi, _ := os.Open(file)
		dec := json.NewDecoder(fi)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		fi.Close()
	}
	sort.Sort(SortByKey(intermediate))

	//reduce
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), fn)

}
