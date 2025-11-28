package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var taskCount int = 2

type Coordinator struct {
	// Your definitions here.
	NReduce int
}

// TODO Possibly make a psudo please exit task for workers
// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskDistributor(args *TaskArgs, reply TaskReply) error {
	reply.Task = taskHandler()
	reply.NReduce = c.NReduce
	return nil
}

// TODO end program
func taskHandler() string {
	filename := os.Args[taskCount]
	if taskCount < len(os.Args[2:]) {
		taskCount++
	}
	return filename
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.NReduce = nReduce

	c.server()
	return &c
}
