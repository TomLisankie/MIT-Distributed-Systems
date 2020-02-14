package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "math/rand"

type Master struct {
	// Your definitions here.
	// TODO: Declare Master data structures here

	// The master keeps several data structures. For each map
	// task and reduce task, it stores the state (idle, in-progress,
	// or completed), and the identity of the worker machine
	// (for non-idle tasks).
	// The master is the conduit through which the location
	// of intermediate file regions is propagated from map tasks
	// to reduce tasks. Therefore, for each completed map task,
	// the master stores the locations and sizes of the R intermediate file regions produced by the map task.
	// Updates to this location and size information are received as map
	// tasks are completed. The information is pushed incrementally to workers that have in-progress reduce tasks.
	MapTasks    map[Task]State
	ReduceTasks map[Task]State
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) getAvailableMapTask() Task {
	// TODO: Take into account when no Map tasks are available
	for task, state := range m.MapTasks {
		if state == idle {
			m.MapTasks[task] = inProgress
			return task
		}
	}

	return Task{}
}

func (m *Master) AssignTask(args *TaskRequest, reply *MapTaskDescription) error {
	reply.InputFileName = m.getAvailableMapTask()
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

	// Should probably use WaitGroup or something here

	// Your code here.

	return ret
}

type Task struct {
	// TODO: implement Task struct
	FileName string
}

type State int

const (
	idle       = iota
	inProgress = iota
	completed  = iota
)

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{map[Task]State{}, map[Task]State{}}

	// Your code here.

	// splitting has already been taken care of, each pg file corresponds to one input file for a map task
	// so there's gonna be a max of 8 Map tasks
	// also, no wonder there only an nReduce parameter since # of map tasks is predetermined.

	// Probably need to take care of loading tasks into the Master's task map
	// We're already being handed the files in a string slice

	for _, file := range files {
		m.MapTasks[Task{file}] = idle
	}
	m.server()
	return &m
}
