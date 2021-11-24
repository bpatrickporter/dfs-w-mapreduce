package main

import (
	"bufio"
	"bytes"
	"dfs/messages"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func HandleArgs() (string, string) {
	controller := os.Args[1]
	port := os.Args[2]
	return controller, port
}

func IsHostOrion() (string, bool) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalln()
	}
	shortHostName:= strings.Split(hostname, ".")[0]
	var isOrion bool
	if strings.HasPrefix(shortHostName, "orion") {
		isOrion = true
	} else {
		isOrion = false
	}
	return shortHostName, isOrion
}

func InitializeLogger() {
	var file *os.File
	var err error
	if _, isOrion := IsHostOrion(); isOrion {
		file, err = os.OpenFile(
			"/home/bpporter/P2-pport/dfs/logs/client_logs.txt",
			os.O_TRUNC|os.O_CREATE|os.O_WRONLY,
			0666)
	} else {
		file, err = os.OpenFile(
			"logs/client_logs.txt",
			os.O_TRUNC|os.O_CREATE|os.O_WRONLY,
			0666)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Client start up complete")
}

func GetMetadata(fileName string) (int, string, bool) {
	f, _ := os.Open(fileName)
	defer f.Close()
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	checkSum := messages.GetCheckSum(*f)
	isTextFile := IsTextFile(fileName)
	return int(fileSize), checkSum, isTextFile
}

func IsTextFile(filename string) bool {
	return true
}

func UnpackPutResponse(msg *messages.Wrapper_PutResponseMessage) (bool, []string, *messages.Metadata) {
	available := msg.PutResponseMessage.GetAvailable()
	nodes := msg.PutResponseMessage.GetNodes()
	metadata := msg.PutResponseMessage.GetMetadata()
	log.Println("Received put response status: " + strconv.FormatBool(available))
	return available, nodes, metadata
}

func UnpackDeleteResponse(msg *messages.Wrapper_DeleteResponseMessage) (bool, []string, []*messages.ListOfStrings){
	fileExists := msg.DeleteResponseMessage.Available
	chunks := msg.DeleteResponseMessage.Chunks
	nodeLists := msg.DeleteResponseMessage.NodeLists
	log.Println("Delete Response message received")
	log.Println("File exists: " + strconv.FormatBool(fileExists))
	for i := range chunks {
		log.Println(chunks[i] + " @ " + nodeLists[i].String())
	}
	if !fileExists {
		fmt.Println("File doesn't exist")
	}
	return fileExists, chunks, nodeLists
}

func UnpackGetResponse(msg *messages.Wrapper_GetResponseMessage) (bool, []string, []string) {
	fileExists := msg.GetResponseMessage.Exists
	chunks := msg.GetResponseMessage.Chunks
	nodes := msg.GetResponseMessage.Nodes
	log.Println("Get Response message received")
	log.Println("File exists: " + strconv.FormatBool(fileExists))
	for i := range chunks {
		log.Println(chunks[i] + " @ " + nodes[i])
	}
	if !fileExists {
		fmt.Println("File doesn't exist")
	}
	return fileExists, chunks, nodes
}

func UnpackComputeResponse(msg *messages.Wrapper_ComputeResponseMessage) (bool, []string, []string, string, string, string) {
	fileExists := msg.ComputeResponseMessage.InputFileExists
	chunks := msg.ComputeResponseMessage.Chunks
	nodes := msg.ComputeResponseMessage.Nodes
	inputFile := msg.ComputeResponseMessage.InputFile
	outputFile := msg.ComputeResponseMessage.OutputFile
	jobFile := msg.ComputeResponseMessage.JobFile
	log.Println("Compute Response message received")
	log.Println("FileExists: " + strconv.FormatBool(fileExists) + "\n" +
		"InputFile: " + inputFile + "\n" +
		"OutputFile: " + outputFile + "\n" +
		"JobFile: " + jobFile + "\n")
	log.Println("File exists: " + strconv.FormatBool(fileExists))
	for i := range chunks {
		log.Println(chunks[i] + " @ " + nodes[i])
	}
	if !fileExists {
		fmt.Println("File doesn't exist")
	}
	return fileExists, chunks, nodes, inputFile, outputFile, jobFile
}

func PackagePutRequest(fileName string) *messages.Wrapper {
	log.Println("Put input received")
	fileSize, checkSum, isTextFile := GetMetadata(fileName)
	metadata := &messages.Metadata{
		FileName: fileName,
		FileSize: int32(fileSize),
		CheckSum: checkSum,
		IsTextFile: isTextFile}
	msg := messages.PutRequest{
		Metadata: metadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{
			PutRequestMessage: &msg},
	}
	log.Println("Sending put request")
	return wrapper
}

func PackageDeleteRequest(fileName string) *messages.Wrapper {
	msg := messages.DeleteRequest{
		FileName: fileName}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteRequestMessage{
			DeleteRequestMessage: &msg},
	}
	log.Println("Sending delete request")
	return wrapper
}

func PackagePutRequestChunk(currentChunk string, offset int, metadata *messages.Metadata, chunkCheckSum string, numBytes int, forwardingList []string) *messages.Wrapper {
	fileMetadata := metadata
	chunkMetadata := &messages.ChunkMetadata{
		ChunkName: currentChunk,
		ChunkSize: int32(numBytes),
		ChunkCheckSum: chunkCheckSum,
		Offset: int32(offset)}
	msg := messages.PutRequest{
		Metadata: fileMetadata,
		ChunkMetadata: chunkMetadata,
		ForwardingList: forwardingList}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{
			PutRequestMessage: &msg},
	}
	log.Println("Packaging chunk: " + currentChunk)
	log.Println("ChunkSize: " + strconv.Itoa(int(msg.ChunkMetadata.ChunkSize)))
	return wrapper
}

func PackageGetRequest(fileName string) *messages.Wrapper {
	msg := messages.GetRequest{
		FileName: fileName}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetRequestMessage{
			GetRequestMessage: &msg},
	}
	return wrapper
}

func PackageMapJobRequest(chunk string, jobFile string, jobLength int, nodes []string, jobId string) *messages.Wrapper {
	msg := messages.MapJobRequest{
		ChunkName: chunk,
		JobFileName: jobFile,
		JobFileSize: int32(jobLength),
		ReducerCandidates: nodes,
		JobId: jobId}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_MapJobRequestMessage{
			MapJobRequestMessage: &msg},
	}
	return wrapper
}

func PackageComputationRequest(job string, input string, output string) *messages.Wrapper {
	msg := messages.ComputeRequest{
		JobFile: job,
		InputFile: input,
		OutputFile: output}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ComputeRequestMessage{
			ComputeRequestMessage: &msg},
	}
	return wrapper
}

func PackageCorruptFileNotice(node string, chunk string) *messages.Wrapper {
	msg := messages.CorruptFileNotice{
		Node: node,
		Chunk: chunk}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_CorruptFileNoticeMessage{
			CorruptFileNoticeMessage: &msg},
	}
	log.Println("Sending corrupt file notice")
	return wrapper
}

func PrintInfoResponse(response *messages.InfoResponse) {
	fmt.Println("Active Nodes")
	for node := range response.Nodes {
		fmt.Println(">" + response.Nodes[node])
	}
	fmt.Println("\nDisk Space: " + response.AvailableDiskSpace + "\n")
	fmt.Println("Requests Per Node")
	for _, pair := range response.RequestsPerNode {
		fmt.Println(">" + pair.Key + ": " + pair.Value)
	}
}

func GetParam(message string) string {
	words := strings.Split(message, " ")
	if len(words) < 2 {
		return ""
	} else {
		return words[1]
	}
}

func GetParams(message string) (string, string, string) {
	words := strings.Split(message, " ")
	if len(words) < 4 {
		fmt.Println("Try again. Correct format: compute <job.go> <input_file> <output_file")
		return "", "", ""
	} else {
		log.Println("Job: " + words[1] + "\n" +
			"Input: " + words[2] + "\n" +
			"Output" + words[3])
		return words[1], words[2], words[3]
	}
}

func GetChunkIndex(metadata *messages.Metadata, destinationNodes []string) map[string][]string {
	chunkToNodeListIndex := make(map[string][]string)
	for i := 0; i < int(metadata.NumChunks); i++ {
		moddedIndex := i % len(destinationNodes)
		node := destinationNodes[moddedIndex]
		currentChunkName := strconv.Itoa(i) + "_" + metadata.FileName
		chunkToNodeListIndex[currentChunkName] = []string{node}
		//add back up nodes
		forwardListIndex1 := (i + 1) % len(destinationNodes)
		forwardListIndex2 := (i + 2) % len(destinationNodes)
		forwardNode1 := destinationNodes[forwardListIndex1]
		forwardNode2 := destinationNodes[forwardListIndex2]
		chunkToNodeListIndex[currentChunkName] = append(chunkToNodeListIndex[currentChunkName], forwardNode1)
		chunkToNodeListIndex[currentChunkName] = append(chunkToNodeListIndex[currentChunkName], forwardNode2)
	}
	for chunk, nodeList := range chunkToNodeListIndex {
		log.Print("-> " + chunk + " ")
		for i := range nodeList {
			log.Print(nodeList[i] + " ")
		}
	}
	return chunkToNodeListIndex
}

func LogFileTransferStatus(status bool) {
	if status {
		log.Println("Chunk transfer successful")
	} else {
		log.Println("Chunk transfer unsuccessful: checksums don't match")
	}
}

func LogDestinationNodes(destinationNodes []string) {
	log.Println("Sending chunks to the following destinations: ")
	for node := range destinationNodes {
		log.Println(destinationNodes[node])
	}
}

func LogFileAlreadyExists() {
	fmt.Println("File with this name already exists, must delete first")
	log.Println("File already exists")
}

func DeleteChunks(chunks []string, nodeLists []*messages.ListOfStrings) {
	for i := range chunks {
		chunk := chunks[i]
		nodeList := nodeLists[i]
		for j := range nodeList.Strings {
			wrapper := PackageDeleteRequest(chunk)
			node := nodeList.Strings[j]
			go DeleteChunk(node, wrapper)
		}
	}
}

func DeleteChunk(node string, wrapper *messages.Wrapper) {
	for {
		if conn, err := net.Dial("tcp", node); err != nil {
			log.Println("Trying connection again to " + node)
		} else {
			messageHandler := messages.NewMessageHandler(conn)
			messageHandler.Send(wrapper)
			log.Println("Delete chunk request sent")
			messageHandler.Close()
			break
		}
	}
}

func SendByteChunks(metadata *messages.Metadata, destinationNodes []string) {
	chunkToNodeListIndex := GetChunkIndex(metadata, destinationNodes)
	f, err := os.Open(metadata.FileName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer f.Close()
	buffer := make([]byte, metadata.ChunkSize)
	counter := 0
	for {
		numBytes, err := f.Read(buffer)
		checkSum := messages.GetChunkCheckSum(buffer[:numBytes])
		reader := bytes.NewReader(buffer)
		if err != nil {
			break
		}
		currentChunk := strconv.Itoa(counter) + "_" + metadata.FileName
		nodeList := chunkToNodeListIndex[currentChunk]

		forwardingList := nodeList[1:]
		wrapper := PackagePutRequestChunk(
			currentChunk,
			0,
			metadata,
			checkSum,
			numBytes,
			forwardingList)
		var conn net.Conn
		for {
			if conn, err = net.Dial("tcp", nodeList[0]); err != nil {
				log.Println("trying conn again" + nodeList[0])
				time.Sleep(1000 * time.Millisecond)
			} else {
				break
			}
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		writer := bufio.NewWriter(conn)
		_, err = io.CopyN(writer, reader, int64(numBytes))
		if err != nil {
			fmt.Print(err.Error())
			break
		}
		log.Printf("%d bytes sent\n", numBytes)
		counter++
		messageHandler.Close()
	}
	fmt.Println("File saved")
}

func SendLineChunks(metadata *messages.Metadata, destinationNodes []string) {
	chunkToNodeListIndex := GetChunkIndex(metadata, destinationNodes)
	f, err := os.Open(metadata.FileName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer f.Close()
	counter := 0
	offset := 0

	log.Println("Offset: " + strconv.Itoa(offset))
	firstReader := bufio.NewReader(f)
	eof := false

	for {
		chunk := make([]byte, 0)
		for len(chunk) < int(metadata.ChunkSize) {
			line, _, err := firstReader.ReadLine()
			if err != nil {
				log.Println("breaking")
				log.Println(err.Error())
				eof = true
				break
			}
			line = append(line, '\n')
			chunk = append(chunk, line...)
		}
		log.Println("First reader read: " + strconv.Itoa(len(chunk)))


		checkSum := messages.GetChunkCheckSum(chunk)
		currentChunkSize := len(chunk)
		log.Println("Buffer size: " + strconv.Itoa(currentChunkSize))

		currentChunk := strconv.Itoa(counter) + "_" + metadata.FileName
		nodeList := chunkToNodeListIndex[currentChunk]

		forwardingList := nodeList[1:]
		wrapper := PackagePutRequestChunk(
			currentChunk,
			offset,
			metadata,
			checkSum,
			currentChunkSize,
			forwardingList)
		var conn net.Conn
		for {
			if conn, err = net.Dial("tcp", nodeList[0]); err != nil {
				log.Println("trying conn again" + nodeList[0])
				time.Sleep(1000 * time.Millisecond)
			} else {
				break
			}
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		reader := bytes.NewReader(chunk)
		writer := bufio.NewWriter(conn)
		n, err := io.CopyN(writer, reader, int64(currentChunkSize))
		if err != nil {
			fmt.Print(err.Error())
			break
		}
		log.Println("%d bytes found\n", currentChunkSize)
		log.Println("%d bytes sent\n", n)
		counter++
		offset = offset + currentChunkSize
		messageHandler.Close()
		if eof {
			break
		}
	}
	fmt.Println("File saved")
}

func SendChunks(metadata *messages.Metadata, destinationNodes []string) {
	if metadata.IsTextFile {
		log.Println("Sending line chunks")
		SendLineChunks(metadata, destinationNodes)
	} else {
		log.Println("Sending byte chunks")
		SendByteChunks(metadata, destinationNodes)
	}
}

func InitiateMapPhase(chunks []string, nodes []string, jobFile string) string {
	log.Println("Sending job")
	var wg sync.WaitGroup

	log.Println("Preparing to read: " + jobFile)
	f, err := os.ReadFile(jobFile)
	if err != nil {
		log.Fatalln(err.Error())
	}

	jobLength := len(f)
	jobId := strconv.Itoa(jobLength) + "_" + jobFile

	for i := range chunks {
		chunk := chunks[i]
		node := nodes[i]
		wrapper := PackageMapJobRequest(chunk, jobFile, jobLength, nodes, jobId)
		conn, err := net.Dial("tcp", node)
		if err != nil {
			log.Fatalln(err.Error())
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		writer := bufio.NewWriter(conn)
		reader := bytes.NewReader(f)
		_, err = io.CopyN(writer, reader, int64(jobLength))
		log.Println("job sent")
		wg.Add(1)
		go WaitForMappersToFinish(messageHandler, &wg, node)
	}
	wg.Wait()
	fmt.Println("Map Phase Complete")
	log.Println("Map Phase Complete")
	return jobId
}

func InitiateReducePhase(nodes []string, jobId string, jobFile string, outputFile string, context context) {
	log.Println("Reduce phase begun")
	var wg sync.WaitGroup

	msg := messages.ReduceJobRequest{
		JobId: jobId,
		JobFileName: jobFile,
		OutputFile: outputFile,
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ReduceJobRequestMessage{
			ReduceJobRequestMessage: &msg,
		},
	}
	for i := range nodes {
		var conn net.Conn
		var err error
		for {
			if conn, err = net.Dial("tcp", nodes[i]); err != nil {
				log.Println("trying conn again" + nodes[i])
				time.Sleep(1000 * time.Millisecond)
			} else {
				break
			}
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		log.Println("reduce notice sent")
		wg.Add(1)
		go WaitForReducersToFinish(messageHandler, &wg, nodes[i], context)
	}
	wg.Wait()
	fmt.Println("Reduce Phase Complete")
	log.Println("Reduce Phase Complete")
}

func ReceiveResults(messageHandler *messages.MessageHandler, outputFile string, fileSize int) {
	file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer file.Close()
	conn := messageHandler.GetConn()
	writer := bufio.NewWriter(file)
	n, err := io.CopyN(writer, conn, int64(fileSize))
	if err != nil {
		log.Println(err.Error())
	}
	log.Println("Wrote result: " + outputFile + " - " + strconv.Itoa(int(n)) + " bytes" )
}

func GetChunks(chunks []string, nodes []string, context context) {
	log.Println("Going to get chunks")
	var wg sync.WaitGroup

	for i := range chunks {
		chunk := chunks[i]
		node := nodes[i]
		wrapper := PackageGetRequest(chunk)
		conn, err := net.Dial("tcp", node)
		if err != nil {
			log.Fatalln(err.Error())
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		wg.Add(1)
		go HandleConnections(messageHandler, &wg, context, node)
	}
	wg.Wait()
	fmt.Println("File downloaded")
}

func GetIndexAndFileName(chunkName string) (string, string) {
	splitIndex := strings.Index(chunkName, "_")
	index := chunkName[0:splitIndex]
	fileName := chunkName[splitIndex + 1:]
	return index, fileName
}

func WriteChunk(chunkMetadata *messages.ChunkMetadata, fileMetadata *messages.Metadata, messageHandler *messages.MessageHandler) bool {
	log.Println(chunkMetadata.ChunkName + " incoming")

	_, fileName := GetIndexAndFileName(chunkMetadata.ChunkName)

	file, err := os.OpenFile("copy_" + fileName, os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		log.Fatalln(err.Error())
	}

	conn := messageHandler.GetConn()
	buffer := make([]byte, chunkMetadata.ChunkSize)
	log.Println("Chunk size: " + strconv.Itoa(len(buffer)))
	numBytes, err := io.ReadFull(conn, buffer)
	if err != nil {
		log.Println(err.Error())
	}
	log.Println(chunkMetadata.ChunkName + " read " + strconv.Itoa(numBytes) + " bytes")

	checkSum := messages.GetChunkCheckSum(buffer[:chunkMetadata.ChunkSize])
	oldCheckSum := chunkMetadata.ChunkCheckSum
	log.Println(chunkMetadata.ChunkName + "New Checksum: " + checkSum)
	log.Println(chunkMetadata.ChunkName + "Old Checksum: " + oldCheckSum)
	var corruptedFile bool
	if strings.Compare(checkSum, oldCheckSum) != 0 {
		corruptedFile = true
	} else {
		corruptedFile = false
	}

	n, err := file.WriteAt(buffer, int64(int(chunkMetadata.Offset)))
	log.Println("Wrote to offset: " + strconv.Itoa(int(chunkMetadata.Offset)))
	if err != nil {
		log.Println(err.Error())
	}
	log.Println(chunkMetadata.ChunkName + " wrote " + strconv.Itoa(n) + " bytes to file")
	if err != nil {
		log.Fatalln(err.Error())
	}
	f, err := file.Stat()
	if err != nil {
		log.Println(err.Error())
	}
	log.Println("FileSize: " + strconv.Itoa(int(f.Size())))
	return corruptedFile
}

func InitiateCorruptFileRecovery(chunk string, node string, context context) {
	fmt.Println("Downloaded file was corrupt. " +
		"Corrupt file recovery process initiated. " +
		"Try download again.")
	messageHandler := messages.EstablishConnection(
		context.controllerName + ":" + context.controllerPort)
	wrapper := PackageCorruptFileNotice(node, chunk)
	messageHandler.Send(wrapper)
	messageHandler.Close()
}

func WaitForMappersToFinish(messageHandler *messages.MessageHandler, waitGroup *sync.WaitGroup, node string) {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_MapCompleteAckMessage:
			defer waitGroup.Done()
			jobId := msg.MapCompleteAckMessage.JobId
			log.Println("Map job complete: " + jobId + " - " + node)
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func WaitForReducersToFinish(messageHandler *messages.MessageHandler, waitGroup *sync.WaitGroup, node string, context context) {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_MapReduceJobResponseMessage:
			defer waitGroup.Done()
			jobId := msg.MapReduceJobResponseMessage.JobId
			outputFile := msg.MapReduceJobResponseMessage.OutputFile
			resultSize := msg.MapReduceJobResponseMessage.ResultSize
			if jobFound := msg.MapReduceJobResponseMessage.JobFound; jobFound {
				log.Println("Map reduce job complete: " + jobId + " - " + node)
				ReceiveResults(messageHandler, outputFile, int(resultSize))
			} else {
				log.Println("No job found on " + node + " for job ID " + jobId)
			}
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func SaveResultsToDFS(messageHandler *messages.MessageHandler, fileName string, context context) {
	wrapper := PackagePutRequest(fileName)
	messageHandler.Send(wrapper)
	HandleConnection(messageHandler, context)
}

func HandleConnections(messageHandler *messages.MessageHandler, waitGroup *sync.WaitGroup, context context, node string) {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_GetResponseChunkMessage:
			defer waitGroup.Done()
			chunkMetadata := msg.GetResponseChunkMessage.ChunkMetadata
			fileMetadata := msg.GetResponseChunkMessage.Metadata
			if fileCorrupted := WriteChunk(chunkMetadata, fileMetadata, messageHandler); fileCorrupted {
				InitiateCorruptFileRecovery(chunkMetadata.ChunkName, node, context)
			}
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func HandleConnection(messageHandler *messages.MessageHandler, context context) {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_AcknowledgeMessage:
			status := msg.AcknowledgeMessage.GetCheckSumMatched()
			LogFileTransferStatus(status)
			messageHandler.Close()
			return
		case *messages.Wrapper_PutResponseMessage:
			available, destinationNodes, metadata := UnpackPutResponse(msg)
			if available {
				LogDestinationNodes(destinationNodes)
				SendChunks(metadata, destinationNodes)
			} else {
				LogFileAlreadyExists()
			}
			return
		case *messages.Wrapper_GetResponseMessage:
			fileExists, chunks, nodes := UnpackGetResponse(msg)
			if fileExists {
				GetChunks(chunks, nodes, context)
			}
			return
		case *messages.Wrapper_DeleteResponseMessage:
			fileExists, chunks, nodes := UnpackDeleteResponse(msg)
			if fileExists {
				DeleteChunks(chunks, nodes)
				fmt.Println("File deleted")
			}
			return
		case *messages.Wrapper_LsResponse:
			fmt.Print(msg.LsResponse.Listing)
			return
		case *messages.Wrapper_InfoResponse:
			PrintInfoResponse(msg.InfoResponse)
			return
		case *messages.Wrapper_ComputeResponseMessage:
			log.Println("Compute response received")
			inputFileExists, chunks, nodes, _, outputFile, jobFile := UnpackComputeResponse(msg)
			log.Println("job: " + jobFile)
			if inputFileExists {
				jobId := InitiateMapPhase(chunks, nodes, jobFile)
				InitiateReducePhase(nodes, jobId, jobFile, outputFile, context)
				SaveResultsToDFS(messageHandler, outputFile, context)
				log.Println("Sending map reduce results to DFS as " + outputFile)
			}
			return
		default:
			continue
		}
	}
}

func HandleInput(scanner *bufio.Scanner, controllerConn net.Conn, context context) {
	message := scanner.Text()
	if len(message) != 0 {
		var wrapper *messages.Wrapper
		controllerMessageHandler := messages.NewMessageHandler(controllerConn)

		if strings.HasPrefix(message, "put"){
			fileName := GetParam(message)
			wrapper = PackagePutRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler, context)
		} else if strings.HasPrefix(message, "get") {
			fileName := GetParam(message)
			wrapper = PackageGetRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler, context)
		} else if strings.HasPrefix(message, "delete") {
			fileName := GetParam(message)
			wrapper = PackageDeleteRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler, context)
		} else if strings.HasPrefix(message, "ls") {
			directory := GetParam(message)
			lsRequest := &messages.LSRequest{
				Directory: directory}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_LsRequest{
					LsRequest: lsRequest},
			}
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler, context)
		} else if strings.HasPrefix(message, "info") {
			infoRequest := &messages.InfoRequest{}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_InfoRequest{
					InfoRequest: infoRequest},
			}
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler, context)
		} else if strings.HasPrefix(message, "compute"){
			fmt.Println("Computing")
			job, input, output := GetParams(message)
			wrapper := PackageComputationRequest(job, input, output)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler, context)
		} else if strings.HasPrefix(message, "help"){
			fmt.Println("Available commands:\n" +
				"put <file_name>\n" +
				"get <file_name>\n" +
				"delete <file_name>\n" +
				"ls <directory>\n" +
				"info")
		} else {
			fmt.Println("error ")
		}
	}
}

func InitializeContext() context {
	controller, port := HandleArgs()
	return context{controller, port}
}

type context struct {
	controllerName string
	controllerPort string
}

func main() {
	context := InitializeContext()
	InitializeLogger()
	controllerConn, err := net.Dial("tcp", context.controllerName + ":" + context.controllerPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer controllerConn.Close()
	fmt.Println("Connected to controller at " + context.controllerName + ":" + context.controllerPort)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">")
		if result := scanner.Scan(); result != false {
			HandleInput(scanner, controllerConn, context)
		}
	}
}

