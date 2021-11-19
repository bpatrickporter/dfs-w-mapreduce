package main

import (
	"bufio"
	"bytes"
	"dfs/messages"
	"io"
	"log"
	"net"
	"os"
	"time"
)

func mapper() {
	return
}

func reducer() {
	return
}

func GetChunkConn(chunkName string, port string) (net.Conn, int) {
	msg := messages.GetRequest{
		FileName: chunkName}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetRequestMessage{
			GetRequestMessage: &msg},
	}

	var conn net.Conn
	var err error
	for {
		if conn, err = net.Dial("tcp", "localhost:" + port); err != nil {
			log.Println("trying conn again" + " localhost:" + port)
			time.Sleep(1000 * time.Millisecond)
		} else {
			break
		}
	}
	messageHandler := messages.NewMessageHandler(conn)
	messageHandler.Send(wrapper)
	chunkSize := WaitForChunkResponse(messageHandler)
	return messageHandler.GetConn(), chunkSize
}

func WaitForChunkResponse(messageHandler *messages.MessageHandler) int {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_GetResponseChunkMessage:
			return int(msg.GetResponseChunkMessage.ChunkMetadata.ChunkSize)
		default:
			continue
		}
	}
}

func SendMapCompleteAck(ackPort string, jobId string) {
	var conn net.Conn
	var err error
	for {
		if conn, err = net.Dial("tcp", "localhost:" + ackPort); err != nil {
			log.Println("trying conn again" + " localhost:" + ackPort)
			time.Sleep(1000 * time.Millisecond)
		} else {
			break
		}
	}
	msg := messages.MapCompleteAck{
		JobId: jobId,
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_MapCompleteAckMessage{
			MapCompleteAckMessage: &msg,
		},
	}
	messageHandler := messages.NewMessageHandler(conn)
	messageHandler.Send(wrapper)
	messageHandler.Close()
}

func HandleArgs() (string, string, string, string, string, string) {
	function := os.Args[1]
	ackPort := os.Args[2]
	chunkName := os.Args[3]
	resultsFilePath := os.Args[4]
	jobId := os.Args[5]
	nodeListeningPort := os.Args[6]
	return function, ackPort, chunkName, resultsFilePath, jobId, nodeListeningPort
}

func main() {
	_, ackPort, chunkName, resultsFilePath, jobId, nodeListeningPort := HandleArgs()
	file, _ := os.OpenFile(resultsFilePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	writer := bufio.NewWriter(file)

	//send get request and wait for response
	con, chunkSize := GetChunkConn(chunkName, nodeListeningPort)
	buffer := make([]byte, chunkSize)
	io.ReadFull(con, buffer)
	reader := bytes.NewReader(buffer)
	io.CopyN(writer, reader, int64(chunkSize))

	//writer.WriteString(function)
	//
	//writer.Flush()
	file.Close()

	SendMapCompleteAck(ackPort, jobId)
}