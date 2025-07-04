package store

import (
	"reredis/pkg/resp"
	"sync"
)

type Store struct {
	Pairs map[string]string //maybe implement my own?
	Mutex sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		Pairs: map[string]string{},
		Mutex: sync.RWMutex{},
	}
}

func (store *Store) Ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		str := "PONG"
		return resp.Value{Type: "string", String: &str}
	}

	return resp.Value{Type: "string", String: args[0].Bulk}
}

func (store *Store) Set(args []resp.Value) resp.Value {
	if len(args) != 2 {
		errStr := "key and/or value not given or incorrect number of arguments passed"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	//check for NX

	//check for expiry and set that
	store.Mutex.Lock()
	store.Pairs[*args[0].Bulk] = *args[1].Bulk
	store.Mutex.Unlock()

	ok := "OK"
	return resp.Value{
		Type:   "string",
		String: &ok,
	}
}

func (store *Store) Get(args []resp.Value) resp.Value {
	if len(args) != 1 {
		errStr := "key not given or incorrect number of arguments passed"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	store.Mutex.RLock()
	value, ok := store.Pairs[*args[0].Bulk]
	store.Mutex.RUnlock()

	if !ok {
		errStr := "key does not exist or has expired"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	return resp.Value{
		Type: "bulk",
		Bulk: &value,
	}
}

func (store *Store) SetNx(args []resp.Value) resp.Value {
	if len(args) != 2 {
		errStr := "key and/or value not given or incorrect number of arguments passed"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	store.Mutex.RLock()
	_, ok := store.Pairs[*args[0].Bulk]
	if ok {
		errStr := "key already exists."
		store.Mutex.RUnlock()
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}
	store.Pairs[*args[0].Bulk] = *args[1].Bulk
	store.Mutex.RUnlock()

	okStr := "OK"
	return resp.Value{
		Type:   "string",
		String: &okStr,
	}
}
