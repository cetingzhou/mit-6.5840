package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type TaskState struct {
	startTime time.Time
	state     string
}

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
	states map[int]*TaskState
	mu     *sync.Mutex
}

// ReduceState tracks the state of all reduce tasks.
// state: idle, inProgress, completed
// taskId is nReduce
type ReduceState struct {
	states map[int]*TaskState
	mu     *sync.Mutex
}

func (c *Coordinator) HeartbeatHandler(args *Heartbeat, reply *HeartbeatReply) error {
	log.Println("Got heartbeat request!")
	if args.TaskState == "completed" {
		switch taskType := args.TaskType; taskType {
		case "map":
			c.mapState.mu.Lock()
			c.mapState.states[args.TaskId].state = "completed"
			c.mapState.mu.Unlock()
		case "reduce":
			c.reduceState.mu.Lock()
			c.reduceState.states[args.TaskId].state = "completed"
			c.reduceState.mu.Unlock()
		}
	}

	if c.isReduceComplete() {
		reply.Done = true
		log.Println("finished all tasks!")
	} else if c.isMapComplete() {
		// assign reduce task
		taskId, err := c.getTaskId("reduce", "idle")
		if err != nil {
			log.Fatal(err)
		}
		if taskId != -1 {
			mappedFileLocation := []string{}
			for i := 0; i < len(c.mapState.states); i++ {
				mappedFileLocation = append(mappedFileLocation, fmt.Sprintf("mr-%d-%d", i, taskId))
			}
			c.assignTask(reply, taskId, len(c.mapState.states), "reduce", strings.Join(mappedFileLocation, ","))
			c.reduceState.mu.Lock()
			c.reduceState.states[taskId].startTime = time.Now()
			c.reduceState.states[taskId].state = "inProgress"
			c.reduceState.mu.Unlock()
			log.Printf("assigned reduce task %d\n", taskId)
			log.Println(mappedFileLocation)
		} else {
			reply.TaskId = -1
			log.Println("all reduce tasks are in progress...")
		}
	} else {
		// assign map task
		taskId, err := c.getTaskId("map", "idle")
		if err != nil {
			log.Fatal(err)
		}
		if taskId != -1 {
			c.assignTask(reply, taskId, len(c.reduceState.states), "map", c.splits[taskId])
			c.mapState.mu.Lock()
			c.mapState.states[taskId].startTime = time.Now()
			c.mapState.states[taskId].state = "inProgress"
			c.mapState.mu.Unlock()
			log.Printf("assigned map task %d\n", taskId)
			log.Println(c.splits[taskId])
		} else {
			reply.TaskId = -1
			log.Println("all map tasks are in progress...")
		}
	}
	return nil
}

func (c *Coordinator) isMapComplete() bool {
	c.mapState.mu.Lock()
	complete := true
	for _, s := range c.mapState.states {
		if s.state != "completed" {
			complete = false
			break
		}
	}
	c.mapState.mu.Unlock()
	return complete
}

func (c *Coordinator) isReduceComplete() bool {
	c.reduceState.mu.Lock()
	complete := true
	for _, s := range c.reduceState.states {
		if s.state != "completed" {
			complete = false
			break
		}
	}
	c.reduceState.mu.Unlock()
	return complete
}

func (c *Coordinator) assignTask(reply *HeartbeatReply, taskId, ntask int, taskType, fileLocation string) {
	reply.FileLocation = fileLocation
	reply.TaskId = taskId
	reply.NTask = ntask
	reply.TaskType = taskType
}

func (c *Coordinator) getTaskId(taskType, taskState string) (int, error) {
	taskId := -1
	if taskType == "map" {
		c.mapState.mu.Lock()
		for k, v := range c.mapState.states {
			if v.state == taskState {
				taskId = k
				break
			}
		}
		c.mapState.mu.Unlock()
	} else if taskType == "reduce" {
		c.reduceState.mu.Lock()
		for k, v := range c.reduceState.states {
			if v.state == taskState {
				taskId = k
				break
			}
		}
		c.reduceState.mu.Unlock()
	} else {
		return taskId, fmt.Errorf("invalid task type %s", taskType)
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
	return c.isReduceComplete()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var mmu sync.Mutex
	ms := MapState{mu: &mmu}
	ms.states = map[int]*TaskState{}
	for i := range files {
		ms.states[i] = &TaskState{state: "idle"}
	}

	var rmu sync.Mutex
	rs := ReduceState{mu: &rmu}
	rs.states = map[int]*TaskState{}
	for i := 0; i < nReduce; i++ {
		rs.states[i] = &TaskState{state: "idle"}
	}

	c := Coordinator{mapState: ms, reduceState: rs, splits: files}

	go c.resetLongTailTask(2 * time.Second)
	c.server()
	log.Println("Start coordinator successfully!")
	return &c
}

func (c *Coordinator) resetLongTailTask(interval time.Duration) {
	ticker := time.NewTicker(interval)
	log.Println("Start long tail ticket")
	for t := range ticker.C {
		for id, s := range c.mapState.states {
			c.mapState.mu.Lock()
			if t.Sub(s.startTime) >= 10*time.Second && s.state == "inProgress" {
				c.mapState.states[id].state = "idle"
			}
			c.mapState.mu.Unlock()
		}
		for id, s := range c.reduceState.states {
			c.reduceState.mu.Lock()
			if t.Sub(s.startTime) >= 10*time.Second && s.state == "inProgress" {
				c.reduceState.states[id].state = "idle"
			}
			c.reduceState.mu.Unlock()
		}
	}
}
