package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	db      map[string]string
	history map[int64]*Result // Identifier -> biggest Seq Result
}

type Result struct {
	LastSeq uint64
	Value   string // old value when append
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.history[args.Identifier]
	if ok {
		if value.LastSeq >= args.Sequence {
			return
		}
	}

	kv.db[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.history[args.Identifier]
	if ok {
		if value.LastSeq >= args.Sequence {
			// do not need to append, return the old value
			reply.Value = kv.history[args.Identifier].Value
			return
		}
	} else {
		kv.history[args.Identifier] = new(Result)
	}

	reply.Value = kv.db[args.Key]
	kv.history[args.Identifier].LastSeq = args.Sequence
	kv.history[args.Identifier].Value = kv.db[args.Key]
	kv.db[args.Key] += args.Value
}

func StartKVServer() *KVServer {

	kv := new(KVServer)

	kv.db = make(map[string]string)
	kv.history = make(map[int64]*Result)

	return kv

}
