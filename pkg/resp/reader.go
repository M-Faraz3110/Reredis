package resp

import (
	"bufio"
	"fmt"
	"strconv"
)

type Resp struct {
	reader *bufio.Reader
}

func NewResp(reader *bufio.Reader) *Resp {
	return &Resp{reader: reader}
}

func (resp *Resp) ReadLine() ([]byte, int, error) {
	numOfBytes := 0
	line := []byte{}
	for {
		by, err := resp.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		numOfBytes++
		line = append(line, by)

		if len(line) >= 2 && line[len(line)-2] == '\r' { //checking if we reached '\r' which means the end of a RESP statement
			break
		}

	}

	return line, numOfBytes, nil
}

func (resp *Resp) ReadInt() (int, int, error) {
	line, numOfBytes, err := resp.ReadLine()
	if err != nil {
		return 0, 0, err
	}

	num, err := strconv.Atoi(string(line))
	if err != nil {
		return 0, 0, err
	}

	return num, numOfBytes, nil
}

func (resp *Resp) Read() (Value, error) {
	byteType, err := resp.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch byteType {
	case ARRAY:
		return resp.ReadArray()
	case BULK:
		val, err := resp.ReadBulk()
		if err != nil {
			return Value{}, err
		}

		return val, nil
	default:
		fmt.Printf("Unknown type: %v", string(byteType))
		return Value{}, err
	}
}

func (resp *Resp) ReadArray() (Value, error) {
	val := Value{
		Type: "array",
	}

	length, _, err := resp.ReadInt() //num of elements kinda
	if err != nil {
		return Value{}, err
	}

	val.Array = make([]Value, length)

	for i := 0; i < length; i++ {
		value, err := resp.Read()
		if err != nil {
			return Value{}, err
		}

		val.Array[i] = value
	}

	return val, nil
}

func (resp *Resp) ReadBulk() (Value, error) {
	val := Value{
		Type: "bulk",
	}

	length, _, err := resp.ReadInt()
	if err != nil {
		return val, err
	}

	bulk := make([]byte, length)
	resp.reader.Read(bulk)

	bulkVal := string(bulk)
	val.Bulk = &bulkVal

	resp.ReadLine() //read till the end of the line

	return val, nil
}
