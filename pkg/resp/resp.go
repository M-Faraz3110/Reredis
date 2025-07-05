package resp

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	Type   string
	String *string
	Number *int64
	Bulk   *string
	Array  []Value
}

//type Array []Value
