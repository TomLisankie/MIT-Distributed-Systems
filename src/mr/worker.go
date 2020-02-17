package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "encoding/json"

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
	for i := 0; i < 3; i++ { // reply.TaskType != terminateTask
		fmt.Println(reply.FileName)
		data, _ := ioutil.ReadFile(reply.FileName)
		if reply.TaskType == mapTask {
			intermediateNameTemplate := fmt.Sprintf("mr-%v-", reply.TaskNumber)
			keyVals := mapf(reply.FileName, string(data))
			fmt.Println(fmt.Sprintf("keyVals: %v", keyVals))
			// Now, write keyVals into an intermediate bucket
			for _, kv := range keyVals {
				bucketNum := ihash(kv.Key) % reply.NReduce
				intermediateFileName := intermediateNameTemplate + string(bucketNum)

				file, err := os.OpenFile("./"+intermediateFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fmt.Println("There was a problem opening the file!")
				}
				defer file.Close()
				// file.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
				encoder := json.NewEncoder(file)
				err = encoder.Encode(&kv)
				if err != nil {
					fmt.Println("There was an error.")
				}

			}
			// Then tell the master that we're done
			// DoneWithTask(reply)
		} else if reply.TaskType == reduceTask {

		} else {

		}
		reply = AskForTask()
	}

}

func DoneWithTask(doneTask *Task) {
	// payload := CompletionPayload{doneTask}
	// call("Master.MarkTaskComplete", &doneTask)
}

// Makes an RPC call to the master asking for a task.

func AskForTask() Task {
	args := TaskRequest{}

	args.Message = ""

	reply := Task{}

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
