package store

import "time"

type ValueStringObj struct {
	Value     string
	ExpiresAt time.Time
}
