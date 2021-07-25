package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
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
// produces a set of intermediate KV pairs. It's provided
// by the user.
type MapFunc func(name string, content string) []KeyValue

// ReduceFunc accepts an intermediate key and a set of values
// for that key. It merges together these values to form a result.
// It's also provided by the user.
type ReduceFunc func(key string, values []string) string

// worker can do either mapping or reducing
type worker struct {
	id       int
	mapFn    MapFunc
	reduceFn ReduceFunc
}

// Registers this worker to the coordinator through RPC.
func (w *worker) register() {

}

// Requests a Task from the coordinator through RPC,
// then returns a Task assigned by the coordinator.
func (w *worker) request() Task {

}

// Reports task to the coorindinator through RPC.
func (w *worker) report(t Task, done bool, err error) {

}

func (w *worker) doMap(t Task) {

}

func (w *worker) doReduce(t Task) {

}
