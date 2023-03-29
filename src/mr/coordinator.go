package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const CHECK_FREQ = 9 * time.Second

type State int32

const (
	StNotSent State = iota
	StSent
	StFinished
)

type Job struct {
	// current state
	State State
	// time stamp of last health check
	LastCheck time.Time
	// work item
	Work string
}

func NewJob(work string) *Job {
	rv := Job{
		State: StNotSent,
	}
	return &rv
}

type Coordinator struct {
	// Your definitions here.
	file_list      []string
	map_is_done    bool
	reduce_is_done bool
	// number of reduce jobs intended
	nreduce int
	// number of distrbuted reduce jobs so far
	nreduce_jobs int
	lock         sync.Mutex
	// set of input files, as we distribute works to workers, remove from this set
	file_set map[string]bool
	// map input file to worker id
	worker_m map[string]int
	// map worker id to input file
	worker_m_inv map[int]string
	// TODO clean up above
	map_jobs    []Job
	reduce_jobs []Job
}

// Your code here -- RPC handlers for the worker to call.

// return map task
func (c *Coordinator) give_map(args *GetWorkReq, reply *GetWorkRep) error {
	var work string
	var map_id int
	has_job := false

	func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		for id, k := range c.map_jobs {
			// if job is not sent
			// or job is sent but last update has been more than 4 seconds ago
			if k.State == StNotSent || (k.State == StSent && k.LastCheck.Before(time.Now().Add(-4*time.Second))) {
				has_job = true
				work = k.Work
				map_id = id
				c.map_jobs[id].State = StSent
				c.map_jobs[id].LastCheck = time.Now()
				break
			}
		}
	}()
	if has_job {
		reply.Item = work
		reply.WorkType = Map
		reply.Id = map_id
		reply.NReduce = c.nreduce
	} else {
		reply.WorkType = wait
	}
	return nil
}

// return reduce task
func (c *Coordinator) give_reduce(args *GetWorkReq, reply *GetWorkRep) error {
	// TODO
	log.Printf("Give Reduce start")
	var work string
	var reduce_id int
	has_job := false
	func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		for id, k := range c.reduce_jobs {
			// job not sent or sent and last updated > 4 secs ago
			if k.State == StNotSent || (k.State == StSent && k.LastCheck.Before(time.Now().Add(-4*time.Second))) {
				has_job = true
				work = k.Work
				reduce_id = id
				c.reduce_jobs[id].State = StSent
				c.reduce_jobs[id].LastCheck = time.Now()
				break
			}
		}
	}()
	if has_job {
		reply.Item = work
		reply.WorkType = Reduce
		reply.Id = reduce_id
	} else {
		reply.WorkType = wait
	}
	log.Printf("Give Reduce stop")
	return nil
}

func (c *Coordinator) HealthCheck(args *Health, reply *None) error {
	// TODO check job type
	log.Printf("Health check id [%v]", args.Id)
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.map_is_done {
		c.map_jobs[args.Id].LastCheck = time.Now()
		log.Printf("Health check for MAP")
	} else if !c.reduce_is_done {
		c.reduce_jobs[args.Id].LastCheck = time.Now()
		log.Printf("Health check for REDUCE")
	}
	return nil
}

// func (c *Coordinator) check_health() {
// 	var job_arr *[]Job
// 	if !c.map_is_done {
// 		job_arr = &c.map_jobs
// 	} else if !c.reduce_is_done {
// 		job_arr = &c.reduce_jobs
// 	}
// 	for idx, mj := range *job_arr {
// 		// if last check timestamp is more than 4 secs ago, consider dead
// 		if mj.State != StFinished && mj.LastCheck.Before(time.Now().Add(-4*time.Second)) {
// 			(*job_arr)[idx].State = StNotSent
// 		}
// 	}
// 	time.Sleep(5 * time.Second)
// }

// GetWork: get work item, including work type, keys, nreduce, and unique id
func (c *Coordinator) GetWork(args *GetWorkReq, reply *GetWorkRep) error {
	// if map stage is not done, try to give map task
	if !c.map_is_done {
		return c.give_map(args, reply)
	} else if !c.reduce_is_done {
		return c.give_reduce(args, reply)
	} else {
		reply.WorkType = Finished
	}
	return nil
}

// for workers to announce that they've completed work.
func (c *Coordinator) WorkerDone(args *WorkDone, reply *None) error {
	// may not be necessary to lock; but to be sure:
	// TODO set map/reduce_is_done
	all_done := true
	var job_arr *[]Job
	var work_is_done *bool
	if args.WorkType == Map {
		job_arr = &c.map_jobs
		work_is_done = &c.map_is_done
	} else {
		job_arr = &c.reduce_jobs
		work_is_done = &c.reduce_is_done
	}
	if (*job_arr)[args.Id].State == StSent {
		(*job_arr)[args.Id].State = StFinished
	}
	// log.Printf("WORKERDONE id [%v]", args.Id)
	var logger string
	for id, j := range *job_arr {
		logger += fmt.Sprintf("%v - %v\n", id, j.State)
		if j.State != StFinished {
			all_done = false
			break
		}
	}
	// log.Printf("WORKERDONE work type [%v] id [%v] state [%v]", args.WorkType, args.Id, logger)
	func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		*work_is_done = all_done
	}()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	// TODO reenable
	// go c.check_health()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	var ret bool
	c.lock.Lock()
	defer c.lock.Unlock()
	ret = c.map_is_done && c.reduce_is_done
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// convert list files to map
	files_m := make(map[string]bool)
	for i := 0; i < len(files); i++ {
		if _, ok := files_m[files[i]]; !ok {
			files_m[files[i]] = true
		}
	}
	// pre-fill map and reduce job array
	map_jobs := make([]Job, len(files))
	for i, f := range files {
		map_jobs[i].Work = f
		map_jobs[i].State = StNotSent
	}
	reduce_jobs := make([]Job, nReduce)
	for i := 0; i < nReduce; i++ {
		reduce_jobs[i].Work = strconv.Itoa(i)
		reduce_jobs[i].State = StNotSent
	}

	c := Coordinator{
		file_list:      files,
		map_is_done:    false,
		reduce_is_done: false,
		nreduce:        nReduce,
		nreduce_jobs:   0,
		file_set:       files_m,
		worker_m:       make(map[string]int),
		worker_m_inv:   make(map[int]string),

		// TODO clean up above
		map_jobs:    map_jobs,
		reduce_jobs: reduce_jobs,
	}

	// Your code here.

	c.server()
	return &c
}

/*
scenario 1: more work than workers:
inputs: A B C D E
workers: 1 2 3
1 takes A
2 takes B
3 takes C
(assuming 1 finishes, then 2, then 3)
1 takes D
2 takes E
--------------------------------------

scenario 2: fewer work than workers:
inputs: A B C
workers: 1 2 3 4
1 takes A
2 takes B
3 takes C
4 pings coordinator and gets nothing, 4 continues to wait
*/
