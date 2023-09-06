package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	request := Heartbeat{State: "idle"}
	reply := HeartbeatReply{}
	CallHeartbeat(&request, &reply)

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
		partitions := make([][]KeyValue, reply.NReduce)
		for _, kv := range kvs {
			i := ihash(kv.Key) % reply.NReduce
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
				tempFile.Close()
			}(i, p)
		}
		done.Wait()
		// worker needs to signify coordinator that it has finished the task
		signal := Heartbeat{"completed", "map", request.TaskId}
		signalReply := HeartbeatReply{}
		CallHeartbeat(&signal, &signalReply)
	case "reduce":
	default:
		log.Fatalf("Invalid task type %s, which must be map or reduce.", taskType)
	}
}

func CallHeartbeat(heartbeat *Heartbeat, heartbeatReply *HeartbeatReply) {
	err := call("Coordinator.HeartbeatHandler", &heartbeat, &heartbeatReply)
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
