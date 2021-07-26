package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

type Task struct {
	File    string // file name
	NReduce int
	NMaps   int
	Seq     int
	Phase   TaskPhase
	Alive   bool
}

// MapFunc takes document name and document content, and
// produces a set of intermediate KV pairs. It should be
// provided by the user.
type MapFunc func(name string, content string) []KeyValue

// ReduceFunc accepts an intermediate key and a set of values
// for that key. It merges together these values to form a result.
// It should also be provided by the user.
type ReduceFunc func(key string, values []string) string

// worker can do either mapping or reducing
type worker struct {
	id       int
	mapFn    MapFunc
	reduceFn ReduceFunc
}

// Registers this worker to the coordinator through RPC.
func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Coordinator.RegiterWorker", args, reply); !ok {
		log.Fatal("[worker.regiter] register fail")
	}
	w.id = reply.WorkerId
}

// Requests a Task from the coordinator through RPC,
// then returns a Task assigned by the coordinator.
func (w *worker) request() Task {
	args := &TaskArgs{WorkerId: w.id}
	reply := &TaskReply{}

	if ok := call("Coorindator.GetTask", args, reply); !ok {
		log.Fatal("[worker.request] get task failed")
		os.Exit(1)
	}
	DebugPrintf("worker-%d get task:%+v", w.id, reply.Task)
	return *reply.Task
}

// do locally, so use *Task instead of Task
func (w *worker) doTask(t *Task) {
	DebugPrintf("worker-%d do task", w.id)

	switch t.Phase {
	case MapPhase:
		w.doMap(t)
	case ReducePhase:
		w.doReduce(t)
	default:
		panic(fmt.Sprintf("task err in phase: %v", t.Phase))
	}
}

func (w *worker) doMap(t *Task) {
	byts, err := ioutil.ReadFile(t.File)
	if err != nil {
		w.report(*t, false, err)
		return
	}

	// 1. Map to intermediate kv pairs
	kvs := w.mapFn(t.File, string(byts))

	// 2. Partition intermediates into NReduce regions by hashing.
	tasks := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		i := ihash(kv.Key) % t.NReduce
		tasks[i] = append(tasks[i], kv)
	}

	// 3. Marshal into JSON, and write to local disk
	for i, kvs := range tasks {
		filename := reduceName(t.Seq, i)
		f, err := os.Create(filename)
		if err != nil {
			w.report(*t, false, err)
			return
		}
		codec := json.NewEncoder(f)
		for _, kv := range kvs {
			if err := codec.Encode(&kv); err != nil {
				w.report(*t, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.report(*t, false, err)
		}
	}
	w.report(*t, true, nil)
}

func (w *worker) doReduce(t *Task) {
	// 1. Call RPC to read buffered data from map worker's local disk.

	// 2. Sort by keys for grouping.

	// 3. Iterate over the data, pass the key and its corresponding
	// set of values to the user's reduce function. Append the output
	// to a final output file for this reduce partition.
}

// Reports task to the coorindinator through RPC.
func (w *worker) report(t Task, done bool, err error) {

}
