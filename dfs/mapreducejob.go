package main

import (
	"bufio"
	"dfs/messages"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func mapper(lineNumber int, line []byte) []keyValuePair {
	words := strings.Fields(string(line))
	results := make([]keyValuePair, 0)
	for i := range words {
		result := keyValuePair{
			key: []byte(words[i]),
			value: []byte("1"),
		}
		results = append(results, result)
	}
	return results
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

type keyValuePair struct {
	key []byte
	value []byte
}

func main() {
	delimiter := []byte(" <--> ")
	function, ackPort, chunkName, resultsFilePath, jobId, nodeListeningPort := HandleArgs()
	file, _ := os.OpenFile(resultsFilePath + nodeListeningPort, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	writer := io.Writer(file)

	//send get request and wait for response
	if function == "map" {
		con, _ := GetChunkConn(chunkName, nodeListeningPort)
		reader := bufio.NewReader(con)
		newLine := []byte{'\n'}

		counter := 0
		results := make([]byte, 0)
		for {
			line, _, err := reader.ReadLine()

			if err == io.EOF {
				break
			}
			keyValuePairs := mapper(counter, line)

			for i := range keyValuePairs {
				result := append(keyValuePairs[i].key, delimiter...)
				result = append(result, keyValuePairs[i].value...)
				//writer.Write(result)
				//writer.Write(newLine)
				result = append(result, newLine...)
				results = append(results, result...)
			}
			counter++
		}
		writer.Write(results)
		SendMapCompleteAck(ackPort, jobId)
	} else if function == "reduce" {

	} else {

	}

}