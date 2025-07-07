package store

import "reredis/pkg/resp"

type MultiQCmd struct {
	Fn   func([]resp.Value) resp.Value
	Args []resp.Value
}
