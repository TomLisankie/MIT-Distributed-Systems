package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"

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
	/* TODO: Instead of storing a slice of intermediate values, store intermediate
	values in *nReduce* buckets in the current directory. Declaration probably doesn't go here */
	// uncomment to send the Example RPC to the master.
	reply := AskForTask()

	// read in contents of file

	data, _ := ioutil.ReadFile(reply.InputFileName.FileName)
	if reply.TaskType == mapTask {
		keyVals := mapf(reply.InputFileName.FileName, string(data))
		fmt.Println(keyVals)
		// Now, write keyVals into an intermediate bucket
		// Then tell the master that we're done
	} else if reply.TaskType == reduceTask {

	} else {

	}

}

func runMap(reply TaskDescription) {

}

// Makes an RPC call to the master asking for a task.

func AskForTask() TaskDescription {
	args := TaskRequest{}

	args.Message = ""

	reply := TaskDescription{}

	call("Master.AssignTask", &args, &reply)

	// fmt.Printf("Input File Name: %v, Task Number: %v\n", reply.InputFileName, reply.MapTaskNumber)

	return reply

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {

	// TODO: When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from call(): if the worker fails to contact the master, it can assume that the master has exited because the job is done, and so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the master can give to workers.

	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
