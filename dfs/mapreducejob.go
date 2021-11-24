package main

import (
	"bufio"
	"bytes"
	"dfs/messages"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func Map(lineNumber int, line []byte) []keyValuePair {
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

func Reduce(key []byte, values [][]byte) keyValuePair {
	total := 0
	for i := range values {
		myInt, _ := strconv.Atoi(string(values[i]))
		total = total + myInt
	}
	return keyValuePair{
		key: key,
		value: []byte(strconv.Itoa(total)),
	}
}

func Preprocess(inputFilePath string) []keyListOfValuesPair {
	logFile, _ := os.OpenFile(inputFilePath + "_log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	log.SetOutput(logFile)
	inputMap := make(map[string][]int)
	output := make([]keyListOfValuesPair, 0)
	inputs, err := os.ReadFile(inputFilePath)
	if err != nil {
		log.Fatalln(err.Error())
	}

	newLine := []byte{'\n'}
	delimiter := []byte(" <--> ")
	keyValuePairs := bytes.Split(inputs, newLine)
	log.Println("Building input map")
	for i := range keyValuePairs {
		if len(keyValuePairs[i]) < 4 {
			break
		}
		keyValuePair := bytes.Split(keyValuePairs[i], delimiter)
		key := string(keyValuePair[0])
		value, _ := strconv.Atoi(string(keyValuePair[1]))
		if values, ok := inputMap[key]; ok {
			values = append(values, value)
			inputMap[key] = values
		} else {
			inputMap[key] = []int{value}
		}
	}

	log.Println("Processing input map")
	for myString, myInts := range inputMap {
		key := []byte(myString)
		values := make([][]byte, 0)
		for i := range myInts {
			intAsString := strconv.Itoa(myInts[i])
			value := []byte(intAsString)
			values = append(values, value)
		}
		keyValuePair := keyListOfValuesPair{
			key: key,
			values: values,
		}
		output = append(output, keyValuePair)
	}
	return output
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

func HandleArgs() (string, string, string, string, string, string, string) {
	function := os.Args[1]
	ackPort := os.Args[2]
	chunkName := os.Args[3]
	resultsFilePath := os.Args[4]
	jobId := os.Args[5]
	nodeListeningPort := os.Args[6]
	rootDir := os.Args[7]
	return function, ackPort, chunkName, resultsFilePath, jobId, nodeListeningPort, rootDir
}

type keyValuePair struct {
	key []byte
	value []byte
}

type keyListOfValuesPair struct {
	key []byte
	values [][]byte
}

func main() {
	delimiter := []byte(" <--> ")
	newLine := []byte{'\n'}
	function, ackPort, chunkName, resultsFilePath, jobId, nodeListeningPort, rootDir := HandleArgs()

	if function == "map" {
		file, _ := os.OpenFile(resultsFilePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
		defer file.Close()
		writer := io.Writer(file)

		con, _ := GetChunkConn(chunkName, nodeListeningPort)
		reader := bufio.NewReader(con)

		counter := 0
		results := make([]byte, 0)
		for {
			line, _, err := reader.ReadLine()

			if err == io.EOF {
				break
			}
			keyValuePairs := Map(counter, line)

			for i := range keyValuePairs {
				result := append(keyValuePairs[i].key, delimiter...)
				result = append(result, keyValuePairs[i].value...)
				result = append(result, newLine...)
				results = append(results, result...)
			}
			counter++
		}
		writer.Write(results)
		SendMapCompleteAck(ackPort, jobId)
	} else if function == "reduce" {
		//inputFilePath := rootDir + "_" + function + "_inputs_" + jobId + nodeListeningPort
		outputFilePath := rootDir + "_" + function + "_results_" + jobId + nodeListeningPort
		file, _ := os.OpenFile(outputFilePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
		defer file.Close()
		writer := io.Writer(file)

		log.Println("Beginning Preprocessing")
		inputs := Preprocess(resultsFilePath)
		log.Println("Finished Preprocessing")
		log.Println("Inputs length: " + strconv.Itoa(len(inputs)))

		outputs := make([]byte, 0)
		log.Println("Beginning reduce phase")
		for i := range inputs {
			keyValuePair := Reduce(inputs[i].key, inputs[i].values)
			output := append(keyValuePair.key, delimiter...)
			output = append(output, keyValuePair.value...)
			output = append(output, newLine...)
			outputs = append(outputs, output...)
		}
		log.Println("Finished reducing")
		log.Println("Writing output to disk")
		writer.Write(outputs)
	} else {
		log.Fatalln("job must be map or reduce function")
	}
}