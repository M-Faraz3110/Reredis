package handler

import (
	"reredis/pkg/resp"
	"reredis/pkg/store"
)

type Handler struct {
	HandlerFuncs map[string]func([]resp.Value) resp.Value
	Store        *store.Store
}

func NewHandler(store *store.Store) *Handler {
	return &Handler{
		HandlerFuncs: map[string]func([]resp.Value) resp.Value{
			"MULTI":   store.Multi,
			"EXEC":    store.Exec,
			"DISCARD": store.Discard,
			"PING":    store.Ping,
			"SET":     store.Set,
			"GET":     store.Get,
			"DEL":     store.Del,
			"HSET":    store.HSet,
			"HGET":    store.HGet,
			"HGETALL": store.HGetAll,
			"LPUSH":   store.LPush,
			"RPUSH":   store.RPush,
			"LPOP":    store.LPop,
			"RPOP":    store.RPop,
			"LLEN":    store.LLen,
			"LRANGE":  store.LRange,
		},
		Store: store,
	}
}

const (
	EXEC_CMD = "EXEC"
)
