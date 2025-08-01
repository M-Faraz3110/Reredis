package store

import "time"

func CleanUp(store *Store) {
	for {
		store.Mutex.RLock()
		for _, value := range store.Pairs.Buckets { //check normal key value pairs for expiry
			valObj, ok := value.Value.(ValueStringObj)
			if !ok {
				continue
			}
			if time.Now().After(valObj.ExpiresAt) {
				store.Mutex.Lock()
				store.Pairs.Delete(value.Key)
				//delete(store.Pairs, key)
				store.Mutex.Unlock()
			}
		}
		store.Mutex.RUnlock()

		store.HMutex.RLock()
		for _, value := range store.Hsets.Buckets { //check Hsets for expiry
			valObj, ok := value.Value.(HSet)
			if !ok {
				continue
			}
			if time.Now().After(valObj.ExpiresAt) {
				store.HMutex.Lock()
				store.Hsets.Delete(value.Key)
				//delete(store.Hsets, key)
				store.HMutex.Unlock()
			}
		}
		store.HMutex.RUnlock()

		time.Sleep(5 * time.Minute) //every 5 minutes i guess?
	}
}
