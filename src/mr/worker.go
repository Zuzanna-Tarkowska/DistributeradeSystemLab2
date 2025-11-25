package mr

import (
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		fileName := taskCall()
		if fileName == "" {
			return
		}
		file, _ := os.Open(fileName)
		content, _ := ioutil.ReadAll(file)
		file.Close()
		mapf(fileName, string(content))
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func taskCall() string {
	args := TaskArgs{}

	reply := TaskReply{}

	ok := call("Coordinator.server", &args, &reply)
	if ok {
		return reply.Task
	} else {
		//TODO terminate worker
		return ""
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
		//TODO use call = false to stop worker
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
