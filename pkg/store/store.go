package store

import (
	"reredis/pkg/resp"
	"reredis/pkg/utils"
	"strconv"
	"sync"
	"time"
)

type Store struct {
	Pairs  *utils.HashMap //maybe implement my own hashMap?
	Hsets  *utils.HashMap
	Lists  *utils.HashMap
	Mutex  sync.RWMutex
	HMutex sync.RWMutex
	LMutex sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		Pairs:  utils.NewHashMap(4),
		Hsets:  utils.NewHashMap(4),
		Mutex:  sync.RWMutex{},
		HMutex: sync.RWMutex{},
		Lists:  utils.NewHashMap(4),
		LMutex: sync.RWMutex{},
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
				_, ok := store.Pairs.Get(*args[0].Bulk)
				//_, ok := store.Pairs[*args[0].Bulk]
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
		store.Pairs.Set(*args[0].Bulk, ValueStringObj{
			Value:     *args[1].Bulk,
			ExpiresAt: time.Now().Add(time.Hour * 1),
		})
	} else {
		store.Pairs.Set(*args[0].Bulk, ValueStringObj{
			Value:     *args[1].Bulk,
			ExpiresAt: *expiresAt,
		})
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
	value, ok := store.Pairs.Get(*args[0].Bulk)
	store.Mutex.RUnlock()

	if !ok {
		errStr := "key does not exist or has expired"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	valueObj, ok := value.(ValueStringObj)
	if !ok {
		errStr := "INTERNAL ERROR"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	if time.Now().After(valueObj.ExpiresAt) { //if its expired, get rid of it
		errStr := "key does not exist or has expired"

		store.Mutex.Lock()
		store.Pairs.Delete(*args[0].Bulk)
		//delete(store.Pairs, *args[0].Bulk)
		store.Mutex.Unlock()

		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	return resp.Value{
		Type: "bulk",
		Bulk: &valueObj.Value,
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
		_, ok := store.Pairs.Get(*key.Bulk)
		if ok {
			store.Pairs.Delete(*key.Bulk)
			//delete(store.Pairs, *key.Bulk)
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
		errStr := "wrong number of arguments for 'HSET'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	hkey := *args[0].Bulk
	key := *args[1].Bulk
	value := *args[2].Bulk

	store.HMutex.Lock()

	var hsetObj *HSet
	var hset any
	var ok bool

	hset, ok = store.Hsets.Get(hkey)
	if !ok {
		hset = &HSet{
			Hset:      utils.NewHashMap(4),
			ExpiresAt: time.Now().Add(1 * time.Hour),
		}
		store.Hsets.Set(hkey, hset)
	}

	hsetObj = hset.(*HSet)

	hsetObj.Hset.Set(key, ValueStringObj{
		Value:     value,
		ExpiresAt: time.Now(),
	})
	// store.Hsets[hkey].Hset[key] = ValueStringObj{
	// 	Value:     value,
	// 	ExpiresAt: time.Now(),
	// }

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
	hset, ok := store.Hsets.Get(hkey)
	if !ok {
		store.HMutex.RUnlock()
		return resp.Value{
			Type: "null",
		}
	}

	hsetObj, ok := hset.(*HSet)
	if !ok {
		store.HMutex.RUnlock()
		errStr := "INTERNAL ERROR"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	value, ok := hsetObj.Hset.Get(key)
	if !ok {
		store.HMutex.RUnlock()
		return resp.Value{
			Type: "null",
		}
	}

	valueObj, ok := value.(ValueStringObj)
	if !ok {
		store.HMutex.RUnlock()
		errStr := "INTERNAL ERROR"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}
	//value, ok := store.Hsets[hkey].Hset[key].(ValueStringObj)
	store.HMutex.RUnlock()

	// if !ok {
	// 	return resp.Value{
	// 		Type: "null",
	// 	}
	// }

	if time.Now().After(hsetObj.ExpiresAt) {
		errStr := "hset does not exist or has expired"

		store.HMutex.Lock()
		store.Hsets.Delete(hkey)
		//delete(store.Hsets, hkey)
		store.HMutex.Unlock()

		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	return resp.Value{
		Type: "bulk",
		Bulk: &valueObj.Value,
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
	hset, ok := store.Hsets.Get(hkey)
	//set, ok := store.Hsets[hkey]
	store.HMutex.RUnlock()

	if !ok {
		return resp.Value{
			Type: "null",
		}
	}

	hsetObj, ok := hset.(*HSet)
	if !ok {
		store.HMutex.RUnlock()
		errStr := "INTERNAL ERROR"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	res := []resp.Value{}

	for _, value := range hsetObj.Hset.Buckets {
		valObj, ok := value.Value.(ValueStringObj)
		if !ok {
			continue //empty idx
		}
		res = append(res, resp.Value{
			Type: "bulk",
			Bulk: &value.Key,
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

func (store *Store) LPush(args []resp.Value) resp.Value {
	if len(args) < 2 {
		errStr := "wrong number of arguments for 'LPUSH'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	key := *args[0].Bulk

	store.LMutex.RLock()
	dq, ok := store.Lists.Get(key)
	//dq, ok := store.Lists[key]
	store.LMutex.RUnlock()

	var dqObj *Deque
	//if list doesnt exist
	if !ok {
		dqObj = NewDeque(4)
	} else {
		dqObj, ok = dq.(*Deque)
		if !ok {
			errStr := "INTERNAL ERROR"
			return resp.Value{
				Type:   "error",
				String: &errStr,
			}
		}
	}

	store.LMutex.Lock()
	for i := 1; i < len(args); i++ {
		val := *args[i].Bulk
		if dqObj.Size == len(dqObj.Buffer) {
			dqObj.Grow()
		}
		dqObj.Head = dqObj.Wrap(dqObj.Head - 1)
		dqObj.Buffer[dqObj.Head] = val
		dqObj.Size++
	}

	store.Lists.Set(key, dqObj)
	//store.Lists[key] = dq
	store.LMutex.Unlock()

	resNum := strconv.Itoa(dqObj.Size)

	return resp.Value{
		Type: "bulk",
		Bulk: &resNum,
	}
}

func (store *Store) RPush(args []resp.Value) resp.Value {
	if len(args) < 2 {
		errStr := "wrong number of arguments for 'RPUSH'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	key := *args[0].Bulk

	store.LMutex.RLock()
	dq, ok := store.Lists.Get(key)
	//dq, ok := store.Lists[key]
	store.LMutex.RUnlock()

	var dqObj *Deque
	//if list doesnt exist
	if !ok {
		dqObj = NewDeque(4)
	} else {
		dqObj, ok = dq.(*Deque)
		if !ok {
			errStr := "INTERNAL ERROR"
			return resp.Value{
				Type:   "error",
				String: &errStr,
			}
		}
	}

	store.LMutex.Lock()

	for i := 1; i < len(args); i++ {
		val := *args[i].Bulk
		if dqObj.Size == len(dqObj.Buffer) {
			dqObj.Grow()
		}
		dqObj.Buffer[dqObj.Tail] = val
		dqObj.Tail = dqObj.Wrap(dqObj.Tail + 1)
		dqObj.Size++
	}

	store.Lists.Set(key, dqObj)
	//store.Lists[key] = dq
	store.LMutex.Unlock()

	resNum := strconv.Itoa(dqObj.Size)

	return resp.Value{
		Type: "bulk",
		Bulk: &resNum,
	}
}

func (store *Store) LPop(args []resp.Value) resp.Value {
	if len(args) != 1 {
		errStr := "wrong number of arguments for 'LPop'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	key := *args[0].Bulk

	store.LMutex.RLock()
	dq, ok := store.Lists.Get(key)
	//dq, ok := store.Lists[key]
	store.LMutex.RUnlock()

	if !ok {
		return resp.Value{
			Type: "null",
		}
	}

	dqObj, ok := dq.(*Deque)
	if !ok {
		errStr := "INTERNAL ERROR"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	store.LMutex.Lock()
	val := dqObj.Buffer[dqObj.Head]
	dqObj.Head = dqObj.Wrap(dqObj.Head + 1)
	dqObj.Size--
	store.LMutex.Unlock()

	return resp.Value{
		Type: "bulk",
		Bulk: &val,
	}
}

func (store *Store) RPop(args []resp.Value) resp.Value {
	if len(args) != 1 {
		errStr := "wrong number of arguments for 'RPop'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	key := *args[0].Bulk

	store.LMutex.RLock()
	dq, ok := store.Lists.Get(key)
	//dq, ok := store.Lists[key]
	store.LMutex.RUnlock()

	if !ok {
		return resp.Value{
			Type: "null",
		}
	}

	dqObj, ok := dq.(*Deque)
	if !ok {
		errStr := "INTERNAL ERROR"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	store.LMutex.Lock()
	dqObj.Tail = dqObj.Wrap(dqObj.Tail - 1)
	val := dqObj.Buffer[dqObj.Tail]
	dqObj.Size--
	store.LMutex.Unlock()

	return resp.Value{
		Type: "bulk",
		Bulk: &val,
	}
}

func (store *Store) LLen(args []resp.Value) resp.Value {
	if len(args) != 1 {
		errStr := "wrong number of arguments for 'LLen'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	key := *args[0].Bulk
	store.LMutex.RLock()
	dq, ok := store.Lists.Get(key)
	//dq, ok := store.Lists[key]
	store.LMutex.RUnlock()

	if !ok {
		res := "0"
		return resp.Value{
			Type: "bulk",
			Bulk: &res,
		}
	}

	dqObj, ok := dq.(*Deque)
	if !ok {
		errStr := "INTERNAL ERROR"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	res := strconv.Itoa(dqObj.Size)

	return resp.Value{
		Type: "bulk",
		Bulk: &res,
	}
}

func (store *Store) LRange(args []resp.Value) resp.Value {
	if len(args) != 3 {
		errStr := "wrong number of arguments for 'LRange'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	key := *args[0].Bulk
	lIndexStr := *args[1].Bulk
	rIndexStr := *args[2].Bulk

	store.LMutex.RLock()
	dq, ok := store.Lists.Get(key)
	//dq, ok := store.Lists[key]
	store.LMutex.RUnlock()

	if !ok {
		return resp.Value{
			Type: "null",
		}
	}

	dqObj, ok := dq.(*Deque)
	if !ok {
		errStr := "INTERNAL ERROR"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	lIndex, err := strconv.Atoi(lIndexStr)
	if err != nil {
		errStr := "range not a number for 'LRange'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}
	rIndex, err := strconv.Atoi(rIndexStr)
	if err != nil {
		errStr := "range not a number for 'LRange'"
		return resp.Value{
			Type:   "error",
			String: &errStr,
		}
	}

	lOffset := dqObj.Head + lIndex
	if lOffset < dqObj.Head {
		lOffset = dqObj.Head
	}

	rnge := (rIndex - lIndex) + 1
	var rOffset int
	if rnge > dqObj.Size {
		rOffset = dqObj.Tail
	} else {
		rOffset = dqObj.Head + rnge
	}

	res := []resp.Value{}

	for lOffset != rOffset {
		res = append(res, resp.Value{
			Type: "bulk",
			Bulk: &dqObj.Buffer[lOffset],
		})

		lOffset = dqObj.Wrap(lOffset + 1)
	}
	// for i := lOffset; i < rOffset; i++ {
	// 	res = append(res, resp.Value{
	// 		Type: "bulk",
	// 		Bulk: &dq.Buffer[i],
	// 	})
	// }

	return resp.Value{
		Type:  "array",
		Array: res,
	}

}
