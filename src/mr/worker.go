package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//TODO One way to get started is to modify mr/worker.go's Worker() to send an RPC to the coordinator asking for a task
	//     	Then modify the coordinator to respond with the file name of an as-yet-unstarted map task.
	//     	Then modify the worker to read that file and call the application Map function, as in mrsequential.go.
	//		timeout after 10 sec
	// 		Implement atomic writing
	for {
		// Your worker implementation here.
		taskType, task, ncount, taskID := taskCall()
		switch taskType {
		case MAP:
			mapTask(mapf, task, ncount, taskID)
		case REDUCE:
			reduceTask(reducef, ncount, taskID)
		case WAIT:
			time.Sleep(300 * time.Millisecond)
		case EXIT:
			return
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//incase of multiple files

}
func reduceTask(reducef func(string, []string) string, NMap int, reduceID int) {
	var intermediate []KeyValue
	for i := 0; i < NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceID)
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceID)
	ofile, _ := ioutil.TempFile("", fmt.Sprintf("tm-out-%d", reduceID))
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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)
	reduceDoneCall(reduceID)
}

func mapTask(mapf func(string, string) []KeyValue, task string, nReduce int, taskID int) {
	file, _ := os.Open(task)
	content, _ := ioutil.ReadAll(file)
	defer file.Close()
	kva := mapf(task, string(content))
	intermediateFileCreator(kva, nReduce, taskID)
	mapDoneCall(taskID, nReduce)
}

func intermediateFileCreator(kva []KeyValue, nReduce int, taskID int) {
	enc := make([]*json.Encoder, nReduce)
	tempFiles := make([]*os.File, nReduce)
	outputFileNames := make([]string, nReduce)

	for i := 0; i < nReduce; i++ {
		tmp, _ := ioutil.TempFile("", fmt.Sprintf("tm-%d-%d", taskID, i))
		tempFiles[i] = tmp
		enc[i] = json.NewEncoder(tmp)
		outputFileNames[i] = fmt.Sprintf("mr-%d-%d", taskID, i)

	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		enc[bucket].Encode(kv)
	}

	for i, f := range tempFiles {
		f.Close()
		os.Rename(f.Name(), outputFileNames[i])

	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func taskCall() (string, string, int, int) {
	args := TaskArgs{}

	reply := TaskReply{}

	ok := call("Coordinator.TaskDistributor", &args, &reply)
	if !ok {
		//TODO
	}
	return reply.TaskType, reply.Task, reply.NCount, reply.TaskID
}
func mapDoneCall(mapID int, nReduce int) {
	args := MapDoneArgs{}
	args.MapID = mapID
	reply := DoneReply{}
	ok := call("Coordinator.MapTaskDone", &args, &reply)
	if !ok {
		//TODO
	}

}
func reduceDoneCall(reduceID int) {
	args := ReduceDoneArgs{}
	args.ReduceID = reduceID
	reply := DoneReply{}
	ok := call("Coordinator.ReduceTaskDone", &args, &reply)
	if !ok {
		//TODO
	}
}

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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
