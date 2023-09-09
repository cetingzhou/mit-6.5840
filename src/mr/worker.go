package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerState struct {
	state     string
	heartbeat *Heartbeat
	mu        *sync.Mutex
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// worker should report heartbeat to coordinator every second to report its state
// if worker is idle, it should be assigned a task.

// when worker complete a task, it also should send a request to coordinator tell it
// the assigned task has been completed.

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.Println("start worker")
	var wmu sync.Mutex
	workerState := &WorkerState{
		state:     "idle",
		heartbeat: &Heartbeat{},
		mu:        &wmu,
	}
	heartbeatChan := make(chan *HeartbeatReply)
	done := make(chan bool)
	go func() {
		log.Println("start worker heartbeat ticker")
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				workerState.mu.Lock()
				if workerState.state == "idle" {
					log.Println("worker is idle and start to request task")
					request := workerState.heartbeat
					reply := &HeartbeatReply{}
					CallHeartbeat(request, reply)
					log.Printf("assigned task: %v\n", *reply)
					heartbeatChan <- reply
				}
				workerState.mu.Unlock()
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
	for {
		reply := <-heartbeatChan
		if reply.Done {
			done <- true
			return
		} else if reply.TaskId != -1 {
			log.Println("start to handle a task")
			HandleHeartbeatReply(workerState, mapf, reducef, reply)
			workerState.heartbeat.TaskId = reply.TaskId
			workerState.heartbeat.TaskState = "completed"
			workerState.heartbeat.TaskType = reply.TaskType
			log.Println("finish a task")
		}
	}
}

func HandleHeartbeatReply(workerState *WorkerState, mapf func(string, string) []KeyValue, reducef func(string, []string) string, reply *HeartbeatReply) {
	workerState.mu.Lock()
	workerState.state = "inProgress"
	workerState.mu.Unlock()
	switch taskType := reply.TaskType; taskType {
	case "map":
		// call map function and save the output into nReduce intermediate files with file
		// naming convention 'mr-M-R'. We need to use ihash to partition the intermidiate key-value
		// pair.
		filePath := reply.FileLocation
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v: %v", filePath, err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v: %v", filePath, err)
		}
		file.Close()
		kvs := mapf(filePath, string(content))
		partitions := make([][]KeyValue, reply.NTask)
		for _, kv := range kvs {
			i := ihash(kv.Key) % reply.NTask
			partitions[i] = append(partitions[i], kv)
		}
		var done sync.WaitGroup
		for i, p := range partitions {
			done.Add(1)
			go func(i int, p []KeyValue) {
				defer done.Done()
				tempFile, err := ioutil.TempFile("", "maptemp.*")
				if err != nil {
					log.Fatalf("Failed to create map temp file: %v", err)
				}
				enc := json.NewEncoder(tempFile)
				for _, kv := range p {
					err := enc.Encode(&kv)
					if err != nil {
						log.Printf("failed to encode %v\n", kv)
					}
				}
				// rename the temp file to avoid reduce task reading partial file
				err = os.Rename(tempFile.Name(), fmt.Sprintf("mr-%d-%d", reply.TaskId, i))
				if err != nil {
					log.Fatalf("Failed to rename map temp file: %v", err)
				}
				defer tempFile.Close()
			}(i, p)
		}
		done.Wait()
	case "reduce":
		// call reduce function to reduce all mapped files and write the output to a file
		// with naming convention mr-out-R
		fileLocations := strings.Split(reply.FileLocation, ",")
		kvs := []KeyValue{}
		var done sync.WaitGroup
		var mu sync.Mutex
		for _, f := range fileLocations {
			done.Add(1)
			go func(f string) {
				defer done.Done()
				file, err := os.Open(f)
				if err != nil {
					log.Fatalf("failed to open file: %v", err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					mu.Lock()
					kvs = append(kvs, kv)
					mu.Unlock()
				}
				defer file.Close()
			}(f)
		}
		done.Wait()
		sort.Sort(ByKey(kvs))

		// write out final output
		oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
		ofile, _ := os.Create(oname)
		i := 0
		for i < len(kvs) {
			j := i + 1
			for j < len(kvs) && kvs[j].Key == kvs[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kvs[k].Value)
			}
			output := reducef(kvs[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

			i = j
		}
		defer ofile.Close()
	default:
		log.Fatalf("Invalid task type %s, which must be map or reduce.", taskType)
	}
	workerState.mu.Lock()
	workerState.state = "idle"
	workerState.mu.Unlock()
}

func CallHeartbeat(heartbeat *Heartbeat, heartbeatReply *HeartbeatReply) {
	err := call("Coordinator.HeartbeatHandler", heartbeat, heartbeatReply)
	if err != nil {
		log.Fatalf("Failed to send RPC request: %v", err)
	}
}

// call sends request to RPC server
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Call(rpcname, args, reply)
}
