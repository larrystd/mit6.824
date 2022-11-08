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

func (m *Coordinator) RequestTask(_ *Placeholder, reply *Task) error {
	reply.Operation = ToWait
	if m.State == Mapping { // master的状态
		for _, task := range m.MapTasks { // 从m.MapTasks中分配task到worker
			now := time.Now()
			m.Mutex.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = now // 设置task的一些信息
				task.State = Executing
				m.MaxTaskId++
				task.Id = m.MaxTaskId
				m.Mutex.Unlock()
				log.Printf("assigned map task %d %s", task.Id, task.Filename)

				reply.Operation = ToRun
				reply.IsMap = true
				reply.NReduce = m.NReduce
				reply.Map = *task
				return nil
			}
			m.Mutex.Unlock()
		}
	} else if m.State == Reducing {
		for _, task := range m.ReduceTasks {
			now := time.Now()
			m.Mutex.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = now
				task.State = Executing
				task.IntermediateFilenames = nil
				for id := range m.MappedTaskId {
					task.IntermediateFilenames = append(task.IntermediateFilenames, fmt.Sprintf("mr-%d-%d", id, task.Id))
				}
				m.Mutex.Unlock()
				log.Printf("assigned reduce task %d", task.Id)

				reply.Operation = ToRun
				reply.IsMap = false
				reply.NReduce = m.NReduce
				reply.Reduce = *task
				return nil
			}
			m.Mutex.Unlock()
		}
	}
	return nil
}

func (m *Coordinator) Finish(args *FinishArgs, _ *Placeholder) error {
	if args.IsMap {
		for _, task := range m.MapTasks {
			if task.Id == args.Id {
				task.State = Finished
				log.Printf("finished task %d, total %d", task.Id, len(m.MapTasks))
				m.MappedTaskId[task.Id] = struct{}{}
				break
			}
		}
		//
		for _, t := range m.MapTasks {
			if t.State != Finished {
				return nil
			}
		}
		m.State = Reducing
	} else {
		for _, task := range m.ReduceTasks {
			if task.Id == args.Id {
				task.State = Finished
				break
			}
		}
		for _, t := range m.ReduceTasks {
			if t.State != Finished {
				return nil
			}
		}
		m.State = Done
	}
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
	return c.State == Done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:      nReduce,
		MaxTaskId:    0,
		MappedTaskId: make(map[int]struct{}),
	}

	for _, f := range files { // for file
		c.MapTasks = append(c.MapTasks, &MapTask{TaskMeta: TaskMeta{State: Pending}, Filename: f})
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &ReduceTask{TaskMeta: TaskMeta{State: Pending, Id: i}})
	}
	c.State = Mapping

	c.server()
	return &c
}
