package store

import (
	"reredis/pkg/utils"
	"time"
)

type HSet struct {
	Hset      *utils.HashMap
	ExpiresAt time.Time
}
