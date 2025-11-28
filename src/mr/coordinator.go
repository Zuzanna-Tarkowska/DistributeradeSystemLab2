package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	IDLE        = 0
	IN_PROGRESS = 1
	COMPLETED   = 2

	MAP     = "map"
	REDUCE  = "reduce"
	WAIT    = "wait"
	EXIT    = "exit"
	TIMEOUT = 10 * time.Second
)

type MapTaskInfo struct {
	state     int
	taskID    int
	file      string
	workerID  int
	startTime time.Time
}
type reduceTaskInfo struct {
	state     int
	taskID    int
	workerID  int
	startTime time.Time
}
type Coordinator struct {
	// Your definitions here.
	files       []string
	nmap        int
	NReduce     int
	mapTasks    []MapTaskInfo
	reduceTasks []reduceTaskInfo

	completedMapTasks    int
	completedReduceTasks int

	Mu sync.Mutex
}

// TODO Possibly make a psudo please exit task for workers
// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) taskDistributor(args *TaskArgs, reply *TaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	now := time.Now()

	if c.completedMapTasks < len(c.mapTasks) {
		for i := range c.mapTasks {
			task := &c.mapTasks[i]
			if task.state == IDLE || (TIMEOUT <= now.Sub(task.startTime)) && task.state == IN_PROGRESS {
				task.state = IN_PROGRESS
				task.startTime = time.Now()
				//TODO
				//task.workerID = args.WorkerID

				reply.TaskType = MAP
				reply.TaskID = task.taskID
				reply.Task = task.file
				reply.NCount = c.NReduce
				return nil
			}
		}

	} else if c.completedReduceTasks < c.NReduce {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].state == IDLE {
				c.reduceTasks[i].state = IN_PROGRESS
				c.reduceTasks[i].startTime = time.Now()
				//TODO
				//task.workerID = args.WorkerID

				reply.TaskType = REDUCE
				reply.TaskID = c.reduceTasks[i].taskID
				reply.NCount = len(c.mapTasks)
				return nil
			}
		}

	} else {
		reply.TaskType = EXIT
		return nil
	}

	reply.TaskType = WAIT
	return nil

}

//TODO end program

func (c *Coordinator) mapTaskDone(args *MapDoneArgs, reply *DoneReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.mapTasks[args.MapID].state = COMPLETED
	c.completedMapTasks += 1
	return nil
}

func (c *Coordinator) reduceTaskDone(args *ReduceDoneArgs, reply *DoneReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.reduceTasks[args.ReduceID].state = COMPLETED
	c.completedReduceTasks += 1
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	ret := false
	// Your code here.
	if c.completedMapTasks == len(c.mapTasks) && c.completedReduceTasks == c.NReduce {
		return true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.NReduce = nReduce
	c.completedMapTasks = 0
	c.completedReduceTasks = 0
	for i, f := range files {
		c.mapTasks = append(c.mapTasks, MapTaskInfo{
			taskID: i,
			file:   f,
			state:  IDLE,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, reduceTaskInfo{
			taskID: i,
			state:  IDLE,
		})
	}
	c.server()
	return &c
}
