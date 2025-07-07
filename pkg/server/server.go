package server

import (
	"bufio"
	"fmt"
	"net"
	"reredis/pkg/handler"
	"reredis/pkg/resp"
	"reredis/pkg/store"
	"strings"
)

func StartServer() {
	fmt.Println("Listening on tcp:6379")

	//create
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	storeObj := store.NewStore()
	handlerObj := handler.NewHandler(storeObj)

	//cleanup goroutine goes here ig
	go store.CleanUp(storeObj)

	for {
		//listen and accept incoming connections
		conn, err := l.Accept() //this blocks
		if err != nil {
			fmt.Println(err)
			return
		}

		go handleConn(conn, handlerObj)
	}

}

func handleConn(conn net.Conn, handlerObj *handler.Handler) {
	defer conn.Close()
	//buf := make([]byte, 1024)

	for {
		r := resp.NewResp(bufio.NewReader(conn))
		value, err := r.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.Type != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.Array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(*value.Array[0].Bulk)
		args := value.Array[1:]

		writer := resp.NewWriter(conn)

		handlerFn, ok := handlerObj.HandlerFuncs[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			str := ""
			writer.Write(resp.Value{Type: "string", String: &str})
			continue
		}

		var result resp.Value

		if handlerObj.Store.InMulti && command != handler.EXEC_CMD {
			result = handlerObj.Store.QMultiCmd(handlerFn, args)
		} else {
			result = handlerFn(args)
		}
		writer.Write(result)
	}
}
