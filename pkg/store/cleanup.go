package store

import "time"

func CleanUp(store *Store) {
	for {
		store.Mutex.RLock()
		for key, value := range store.Pairs { //check normal key value pairs for expiry
			valObj := value.(ValueStringObj)
			if time.Now().After(valObj.ExpiresAt) {
				store.Mutex.Lock()
				delete(store.Pairs, key)
				store.Mutex.Unlock()
			}
		}
		store.Mutex.RUnlock()

		store.HMutex.RLock()
		for key, value := range store.Hsets { //check Hsets for expiry
			if time.Now().After(value.ExpiresAt) {
				store.HMutex.Lock()
				delete(store.Hsets, key)
				store.HMutex.Unlock()
			}
		}
		store.HMutex.RUnlock()

		time.Sleep(5 * time.Minute) //every 5 minutes i guess?
	}
}
