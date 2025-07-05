package store

import (
	"reredis/pkg/resp"
	"strconv"
	"sync"
	"time"
)

type Store struct {
	Pairs  map[string]any //maybe implement my own hashMap?
	Hsets  map[string]HSet
	Mutex  sync.RWMutex
	HMutex sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		Pairs:  map[string]any{},
		Hsets:  map[string]HSet{},
		Mutex:  sync.RWMutex{},
		HMutex: sync.RWMutex{},
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
	if len(args) < 2 {
		errStr := "key and/or value not given or incorrect number of arguments passed"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	//check for NX
	//nxFlag := false
	var expiresAt *time.Time

	for i := 2; i < len(args); i++ {
		switch *args[i].Bulk {
		case "NX":
			{
				_, ok := store.Pairs[*args[0].Bulk]
				if ok {
					errStr := "key already exists."
					store.Mutex.RUnlock()
					return resp.Value{
						Type:   "error",
						String: &errStr,
					}
				}
			}
		case "EX":
			{
				if expiresAt != nil {
					errStr := "syntax error"
					return resp.Value{
						Type:   "error",
						String: &errStr,
					}
				}
				i++
				val := args[i].Bulk
				if val == nil {
					errStr := "syntax error"
					return resp.Value{
						Type:   "error",
						String: &errStr,
					}
				}
				valNum, err := strconv.Atoi(*val)
				if err != nil {
					errStr := "syntax error: invalid expiration time"
					return resp.Value{
						Type:   "error",
						String: &errStr,
					}
				}
				timeObj := time.Now().Add(time.Second * time.Duration(valNum))
				expiresAt = &timeObj
			}
		case "EXAT":
			{
				if expiresAt != nil {
					errStr := "syntax error"
					return resp.Value{
						Type:   "error",
						String: &errStr,
					}
				}
				i++
				val := args[i].Number
				if val == nil {
					errStr := "syntax error"
					return resp.Value{
						Type:   "error",
						String: &errStr,
					}
				}
				timeObj := time.Unix(*val, 0)
				expiresAt = &timeObj
			}
		}
	}

	//check for expiry and set that
	store.Mutex.Lock()
	if expiresAt == nil {
		store.Pairs[*args[0].Bulk] = ValueStringObj{
			Value:     *args[1].Bulk,
			ExpiresAt: time.Now().Add(time.Hour * 1),
		}
	} else {
		store.Pairs[*args[0].Bulk] = ValueStringObj{
			Value:     *args[1].Bulk,
			ExpiresAt: *expiresAt,
		}
	}
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
	value, ok := store.Pairs[*args[0].Bulk].(ValueStringObj)
	store.Mutex.RUnlock()

	if !ok {
		errStr := "key does not exist or has expired"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	if time.Now().After(value.ExpiresAt) { //if its expired, get rid of it
		errStr := "key does not exist or has expired"
		delete(store.Pairs, *args[0].Bulk)
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	return resp.Value{
		Type: "bulk",
		Bulk: &value.Value,
	}
}

func (store *Store) Del(args []resp.Value) resp.Value {
	if len(args) < 1 {
		errStr := "key not given or incorrect number of arguments passed"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	deleted := 0

	store.Mutex.Lock()
	for _, key := range args {
		_, ok := store.Pairs[*key.Bulk]
		if ok {
			delete(store.Pairs, *key.Bulk)
			deleted++
		}
	}
	store.Mutex.Unlock()

	bulk := strconv.Itoa(deleted)
	return resp.Value{
		Type: "bulk",
		Bulk: &bulk,
	}
}

func (store *Store) HSet(args []resp.Value) resp.Value {
	if len(args) != 3 {
		errStr := "ERR wrong number of arguments for 'HSET'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	hkey := *args[0].Bulk
	key := *args[1].Bulk
	value := *args[2].Bulk

	store.HMutex.Lock()

	if _, ok := store.Hsets[hkey]; !ok {
		store.Hsets[hkey] = HSet{
			Hset:      map[string]any{},
			ExpiresAt: time.Now().Add(1 * time.Hour),
		}
	}

	store.Hsets[hkey].Hset[key] = ValueStringObj{
		Value:     value,
		ExpiresAt: time.Now(),
	}

	store.HMutex.Unlock()

	str := "OK"
	return resp.Value{
		Type:   "string",
		String: &str,
	}
}

func (store *Store) HGet(args []resp.Value) resp.Value {
	if len(args) != 2 {
		errStr := "wrong number of arguments for 'hget'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	hkey := *args[0].Bulk
	key := *args[1].Bulk

	store.HMutex.RLock()
	value, ok := store.Hsets[hkey].Hset[key].(ValueStringObj)
	store.HMutex.RUnlock()

	if !ok {
		return resp.Value{
			Type: "null",
		}
	}

	if time.Now().After(store.Hsets[hkey].ExpiresAt) {
		errStr := "hset does not exist or has expired"

		delete(store.Hsets, hkey)

		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	return resp.Value{
		Type: "bulk",
		Bulk: &value.Value,
	}
}

func (store *Store) HGetAll(args []resp.Value) resp.Value {
	if len(args) != 1 {
		errStr := "wrong number of arguments for 'hgetall'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	hkey := *args[0].Bulk

	store.HMutex.RLock()
	set, ok := store.Hsets[hkey]
	store.HMutex.RUnlock()

	if !ok {
		return resp.Value{
			Type: "null",
		}
	}

	res := []resp.Value{}

	for key, value := range set.Hset {
		valObj := value.(ValueStringObj)
		res = append(res, resp.Value{
			Type: "bulk",
			Bulk: &key,
		})
		res = append(res, resp.Value{
			Type: "bulk",
			Bulk: &valObj.Value,
		})
	}

	return resp.Value{
		Type:  "array",
		Array: res,
	}
}
