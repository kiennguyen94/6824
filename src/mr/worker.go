package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const GET_WORK = "Coordinator.GetWork"
const WORK_DONE = "Coordinator.WorkerDone"
const HEALTH = "Coordinator.HealthCheck"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func do_map(mapf func(string, string) []KeyValue, work_items string, id int, nreduce int) []KeyValue {
	log.Printf("MAP: work item [%v] id [%v] pid [%v]", work_items, id, os.Getpid())
	quit := make(chan bool)
	go CallHealth(id, quit)
	defer func() {
		quit <- true
	}()
	file, err := os.Open(work_items)
	var kva []KeyValue
	if err != nil {
		log.Fatalf("cannot open %v", work_items)
		return kva
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", work_items)
		return kva
	}
	kva = mapf(work_items, string(content))

	// Partition kv to buckets
	buckets := make([][]KeyValue, nreduce)
	for _, kv := range kva {
		hash := ihash(kv.Key)
		bucket_idx := hash % nreduce
		buckets[bucket_idx] = append(buckets[bucket_idx], kv)
	}

	// write to file
	file_prefix := "mr-" + strconv.Itoa(id) + "-"
	for b_id, bucket := range buckets {
		file_path := file_prefix + strconv.Itoa(b_id)
		func() {
			file, err := os.Create(file_path)
			if err != nil {
				log.Fatalf("cannot open %v", file_path)
				return
			}
			defer file.Close()
			enc := json.NewEncoder(file)
			for _, kv := range bucket {
				enc.Encode(kv)
			}
		}()
	}

	// announce to coordinator that i'm done
	args := WorkDone{
		Id:       id,
		WorkType: Map,
	}
	reply := None{}
	call(WORK_DONE, &args, &reply)
	log.Printf("work item [%v] done pid [%v]", work_items, os.Getpid())
	return kva
}

func do_reduce(reducef func(string, []string) string, work_items string, id int) {
	log.Printf("REDUCE: work item [%v] id [%v] pid [%v]", work_items, id, os.Getpid())
	quit := make(chan bool)
	go CallHealth(id, quit)
	defer func() {
		quit <- true
	}()
	// shuffle stage
	// ----------------
	// find files ending with work_items, eg work_items="1"
	// return "mr-0-1 mr-1-1 mr-2-1 mr-3-1 mr-4-1 mr-5-1 mr-6-1 mr-7-1"
	matches, err := filepath.Glob(fmt.Sprintf("mr-*-%s", work_items))
	if err != nil {
		log.Fatalf("cannot glob for work item [%v]", work_items)
	}
	// read from list of files to []KeyValue
	var kva []KeyValue
	for _, match := range matches {
		func() {
			file, err := os.Open(match)
			if err != nil {
				log.Fatalf("cannot open %v", match)
				return
			}
			defer file.Close()
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", work_items)
	ofile, _ := os.Create(oname)
	// reduce on each distinct key
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// announce done
	args := WorkDone{
		Id:       id,
		WorkType: Reduce,
	}
	reply := None{}
	call(WORK_DONE, &args, &reply)
	log.Printf("work item [%v] done pid [%v]", work_items, os.Getpid())
}

// every 4 seconds, send health check signal to coordinator
func CallHealth(id int, quit chan bool) {
	// TODO
	args := Health{Id: id, Pid: os.Getpid()}
	reply := None{}
	for {
		select {
		case <-quit:
			return
		default:
			//
		}
		call(HEALTH, &args, &reply)
		time.Sleep(4 * time.Second)
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Printf("WORKER pid [%v] starting", os.Getpid())
	// Your worker implementation here.
OuterLoop:
	for {
		start := time.Now()
		work_req := CallGetWork()
		work_type := work_req.WorkType
		switch work_type {
		case WorkType(Map):
			do_map(mapf, work_req.Item, work_req.Id, work_req.NReduce)
		case WorkType(Reduce):
			do_reduce(reducef, work_req.Item, work_req.Id)
		case WorkType(wait):
			log.Printf("waiting 1 secs\n")
			time.Sleep(time.Second)
		case WorkType(Finished):
			log.Printf("received DONE pid [%v]", os.Getpid())
			break OuterLoop
		default:
			log.Fatalf("unknown work type %v", work_type)
		}
		log.Printf("WORKER 1 cycle duration [%v] pid [%v]", time.Since(start), os.Getpid())
	}

	log.Printf("Worker [%v] DONE", os.Getpid())
}

func CallGetWork() GetWorkRep {
	start := time.Now()
	args := GetWorkReq{
		Pid: os.Getpid(),
	}
	reply := GetWorkRep{}
	ok := call(GET_WORK, &args, &reply)
	log.Printf("Call Get Work Done, duration [%v], work type [%v], pid [%v], id [%v]", (time.Since(start)), reply.WorkType, os.Getpid(), reply.Id)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed\n")
		return GetWorkRep{}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
