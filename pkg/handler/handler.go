package handler

import (
	"reredis/pkg/resp"
	"reredis/pkg/store"
)

type Handler struct {
	HandlerFuncs map[string]func([]resp.Value) resp.Value
}

func NewHandler(store *store.Store) *Handler {
	return &Handler{
		HandlerFuncs: map[string]func([]resp.Value) resp.Value{
			"PING":    store.Ping,
			"SET":     store.Set,
			"GET":     store.Get,
			"DEL":     store.Del,
			"HSET":    store.HSet,
			"HGET":    store.HGet,
			"HGETALL": store.HGetAll,
		},
	}
}
