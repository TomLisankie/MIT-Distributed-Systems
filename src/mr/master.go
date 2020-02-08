package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "math/rand"
import "fmt"

type Master struct {
	// Your definitions here.
	// TODO: Declare Master data structures here
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignTask(args *TaskRequest, reply *MapTaskDescription) error {
	reply.InputFileName = fmt.Sprintf("task%v.txt", rand.Intn(100))
	reply.MapTaskNumber = rand.Intn(100)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := true

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	// TODO: Probably put code for splitting input here

	m.server()
	return &m
}
