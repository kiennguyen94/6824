package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type WorkType int64

const (
	Map WorkType = iota
	Reduce
	wait
	Finished
)

// GetWorkReq: get work request object
// Uniquely identify this worker by its process id
type GetWorkReq struct {
	Pid int
}

type GetWorkRep struct {
	Item     string
	Id       int
	WorkType WorkType
	NReduce  int
}

type WorkDone struct {
	Id       int
	WorkType WorkType
}

// notify coordinator that this worker is alive
type Health struct {
	Id  int
	Pid int
}

type None struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
