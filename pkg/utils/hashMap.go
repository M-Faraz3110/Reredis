package utils

type HashMap struct {
	Buckets []Entry
	Count   int
	Used    int
}

type Entry struct {
	Key       string
	Value     any
	Tombstone bool
}

func NewHashMap(size int) *HashMap {
	return &HashMap{
		Buckets: make([]Entry, size),
		Count:   0,
		Used:    0,
	}
}

func Hash(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func (hMap *HashMap) Set(key string, value any) {

	//check load factor
	if float64(hMap.Used)/float64(len(hMap.Buckets)) > 0.75 {
		hMap.Resize()
	}

	hIdx := int(Hash(key) % uint64(len(hMap.Buckets)))

	for {
		val := hMap.Buckets[hIdx]
		if val.Key == "" || val.Tombstone || val.Key == key { //no collision
			if val.Key == "" || val.Tombstone {
				hMap.Count++
			}

			hMap.Buckets[hIdx] = Entry{
				Key:       key,
				Value:     value,
				Tombstone: false,
			}
			hMap.Used++
			return
		}

		hIdx = (hIdx + 1) % len(hMap.Buckets)
	}
}

func (hMap *HashMap) Get(key string) (any, bool) {
	hIdx := int(Hash(key) % uint64(len(hMap.Buckets)))

	for {
		val := hMap.Buckets[hIdx]
		if val.Key == "" && !val.Tombstone {
			return nil, false
		}

		if val.Key == key && !val.Tombstone {
			return val.Value, true
		}

		hIdx = (hIdx + 1) % len(hMap.Buckets)
	}
}

func (hMap *HashMap) Delete(key string) {
	hIdx := int(Hash(key) % uint64(len(hMap.Buckets)))

	for {
		val := hMap.Buckets[hIdx]
		if val.Key == "" && !val.Tombstone {
			return
		}

		if val.Key == key && !val.Tombstone {
			hMap.Buckets[hIdx].Tombstone = true
			hMap.Count--
			return
		}

		hIdx = (hIdx + 1) % len(hMap.Buckets)
	}
}

func (hMap *HashMap) Resize() {
	oldBkts := hMap.Buckets
	hMap.Buckets = make([]Entry, len(oldBkts)*2)
	hMap.Count = 0

	for _, val := range oldBkts {
		if val.Key != "" || !val.Tombstone {
			hMap.Set(val.Key, val.Value)
		}
	}
}
