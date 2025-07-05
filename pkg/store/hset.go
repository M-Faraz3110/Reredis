package store

import "time"

type HSet struct {
	Hset      map[string]any
	ExpiresAt time.Time
}
