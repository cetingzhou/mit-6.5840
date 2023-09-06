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

type Coordinator struct {
	// Your definitions here.
	mapState    MapState
	reduceState ReduceState
	splits      []string
}

// MapState tracks the state of all map tasks.
// state: idle, inProress, completed
// taskId is the file name of each split
type MapState struct {
	states map[int]string
	mu     *sync.Mutex
}

// ReduceState tracks the state of all reduce tasks.
// state: idle, inProgress, completed
// taskId is nReduce
type ReduceState struct {
	states map[int]string
	mu     *sync.Mutex
}

func (c *Coordinator) HeartbeatHandler(args *Heartbeat, reply *HeartbeatReply) error {
	fmt.Println("Got heartbeat request!")
	switch workerState := args.State; workerState {
	case "idle":
		// check if all map tasks have been completed
		// if yes, start to assign reduce tasks
		taskId, err := c.getTaskId("map", "idle")
		if err != nil {
			log.Fatal(err)
		}
		if taskId != -1 {
			reply.FileLocation = c.splits[taskId]
			reply.NReduce = len(c.reduceState.states)
			reply.TaskId = taskId
			reply.TaskType = "map"
		} else {
			taskId, err = c.getTaskId("map", "inProgress")
			if err != nil {
				log.Fatal(err)
			}
			if taskId == -1 {
				time.Sleep(1)
			}
		}
	case "inProgress":
		// if a task has been inProgress 10 seconds we should assign the task to other worker
	case "completed":
	}
	return nil
}

func (c *Coordinator) getTaskId(taskType, taskState string) (int, error) {
	taskId := -1
	if taskType == "map" {
		c.mapState.mu.Lock()
		for k, v := range c.mapState.states {
			if v == taskState {
				taskId = k
				break
			}
		}
		c.mapState.mu.Unlock()
	} else if taskType == "reduce" {
		c.reduceState.mu.Lock()
		for k, v := range c.reduceState.states {
			if v == taskState {
				taskId = k
				break
			}
		}
		c.reduceState.mu.Unlock()
	} else {
		return taskId, fmt.Errorf("Invalid task type %s", taskType)
	}
	return taskId, nil
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
	var mmu sync.Mutex
	ms := MapState{mu: &mmu}
	ms.states = map[int]string{}
	for i := range files {
		ms.states[i] = "idle"
	}
	log.Println(ms)

	var rmu sync.Mutex
	rs := ReduceState{mu: &rmu}
	rs.states = map[int]string{}
	for i := 0; i < nReduce; i++ {
		rs.states[i] = "idle"
	}
	log.Println(rs)

	c := Coordinator{mapState: ms, reduceState: rs, splits: files}

	c.server()
	return &c
}
