package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type JobState int

const (
	Mapping JobState = iota
	Reducing
	Done
)

type Coordinator struct {
	// Your definitions here.
	State       JobState
	NReduce     int
	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask

	MappedTaskId map[int]struct{}
	MaxTaskId    int
	Mutex        sync.Mutex

	WorkerCount  int
	ExcitedCount int
}

const TIMEOUT = 10 * time.Second

// Worker request task from Coordinator
func (c *Coordinator) RequestTask(_ *Placeholder, reply *Task) error {
	reply.Operation = ToWait
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.State == Mapping { // master的状态
		for _, task := range c.MapTasks { // for map task to assign every task
			now := time.Now()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = now // set task metadata
				task.State = Executing
				c.MaxTaskId++
				task.Id = c.MaxTaskId // set taskId, one inputfile one mapper taskId
				log.Printf("assigned map task %d %s", task.Id, task.Filename)

				reply.Operation = ToRun
				reply.IsMap = true
				reply.NReduce = c.NReduce
				reply.Map = *task // task
				return nil
			}
		}
	} else if c.State == Reducing {
		for _, task := range c.ReduceTasks {
			now := time.Now()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = now
				task.State = Executing
				task.IntermediateFilenames = nil // provide intermedidateFilename for reduce task
				for id := range c.MappedTaskId {
					task.IntermediateFilenames = append(task.IntermediateFilenames, fmt.Sprintf("mr-%d-%d", id, task.Id))
				} // reduce files named mr-maptaskId-reducetaskId, reduce task process following reduceId instead of mapper id
				log.Printf("assigned reduce task %d", task.Id)

				reply.Operation = ToRun
				reply.IsMap = false
				reply.NReduce = c.NReduce
				reply.Reduce = *task
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, _ *Placeholder) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.IsMap {
		for _, task := range c.MapTasks {
			if task.Id == args.Id { // Set the task finished
				task.State = Finished
				log.Printf("finished task %d, total %d", task.Id, len(c.MapTasks))
				c.MappedTaskId[task.Id] = struct{}{}
				break
			}
		}
		for _, t := range c.MapTasks {
			if t.State != Finished {
				return nil
			}
		}
		// At there every mapper task are finished, set Coordinator* m
		c.State = Reducing
	} else {
		for _, task := range c.ReduceTasks {
			if task.Id == args.Id {
				task.State = Finished
				break
			}
		}
		for _, t := range c.ReduceTasks {
			if t.State != Finished {
				return nil
			}
		}
		c.State = Done
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c) // Could call object throught RPC
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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.State == Done
}

// create a Coordinator, and Init Coordinator
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:      nReduce,
		MaxTaskId:    0,
		MappedTaskId: make(map[int]struct{}),
	}
	// Add mapper tasks, number is len(files)
	for _, f := range files {
		c.MapTasks = append(c.MapTasks, &MapTask{TaskMeta: TaskMeta{State: Pending}, Filename: f})
	}
	// Add reduce tasks, number is nReduce
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &ReduceTask{TaskMeta: TaskMeta{State: Pending, Id: i}})
	}
	c.State = Mapping

	c.server() // start a new goroutine to serve
	return &c
}
