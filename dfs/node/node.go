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
	"time"
)

func HandleArgs() (string, string, string, string) {
	listeningPort := os.Args[1]
	rootDir := os.Args[2]
	controllerName := os.Args[3]
	controllerPort := os.Args[4]
	return listeningPort, rootDir, controllerName, controllerPort
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

	if host, isOrion := IsHostOrion(); isOrion {
		file, err = os.OpenFile(
			"/home/bpporter/P2-pport/dfs/logs/" + host + "_logs.txt",
			os.O_TRUNC|os.O_CREATE|os.O_WRONLY,
			0666)
	} else {
		file, err = os.OpenFile(
			"logs/node_logs.txt",
			os.O_TRUNC|os.O_CREATE|os.O_WRONLY,
			0666)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Node start up complete")
}

func SendHeartBeats(context context) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalln()
	}

	for {
		if conn, err := net.Dial("tcp", context.controllerName + ":" + context.controllerPort); err == nil {
			log.Println("Connected to controller: " + context.controllerName + ":" + context.controllerPort)
			messageHandler := messages.NewMessageHandler(conn)
			wrapper := PackageHeartBeat(hostname, context.listeningPort)
			for {
				messageHandler.Send(wrapper)
				//log.Println("Heart beat sent to controller")
				time.Sleep(5 * time.Second)
			}
		}
		fmt.Println("Trying connection to controller again")
		time.Sleep(2 * time.Second)
	}
}

func PackageHeartBeat(hostName string, port string) *messages.Wrapper {
	heartBeat := &messages.Heartbeat{
		Node: hostName + ":" + port}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_HeartbeatMessage{
			HeartbeatMessage: heartBeat},
	}
	return wrapper
}

func UnpackMetadata(metadata *messages.Metadata) (string, int, int, int, string, bool) {
	fileName := metadata.GetFileName()
	fileSize := int(metadata.GetFileSize())
	numChunks := int(metadata.GetNumChunks())
	chunkSize := int(metadata.GetChunkSize())
	checkSum := metadata.GetCheckSum()
	isTextFile := metadata.IsTextFile
	return fileName, fileSize, numChunks, chunkSize, checkSum, isTextFile
}

func UnpackChunkMetadata(metadata *messages.ChunkMetadata) (string, int, string, int) {
	chunkName := metadata.ChunkName
	chunkSize := metadata.ChunkSize
	checkSum := metadata.ChunkCheckSum
	offset := metadata.Offset
	return chunkName, int(chunkSize), checkSum, int(offset)
}

func PackageMetadata(context context, chunkName string) (*messages.Metadata, *messages.ChunkMetadata){
	contents, err := os.ReadFile(context.rootDir + "meta_" + chunkName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	string := string(contents)
	slices := strings.Split(string, ",")
	fileName := slices[0]
	fileSize, _ := strconv.Atoi(slices[1])
	numChunks, _ := strconv.Atoi(slices[2])
	standardChunkSize, _ := strconv.Atoi(slices[3])
	actualChunkSize, _ := strconv.Atoi(slices[4])
	checkSum := slices[5]
	chunkCheckSum := slices[6]
	isTextFile, _ := strconv.ParseBool(slices[7])
	offset, _ := strconv.Atoi(slices[8])
	metadata := &messages.Metadata{
		FileName: fileName,
		FileSize: int32(fileSize),
		NumChunks: int32(numChunks),
		ChunkSize: int32(standardChunkSize),
		CheckSum: checkSum,
		IsTextFile: isTextFile}
	chunkMetadata := &messages.ChunkMetadata{
		ChunkName: chunkName,
		ChunkSize: int32(actualChunkSize),
		ChunkCheckSum: chunkCheckSum,
		Offset: int32(offset)}

	return metadata, chunkMetadata
}

func WriteMetadataFile(metadata *messages.Metadata, chunkMetadata *messages.ChunkMetadata, context context) error {
	fileName, fileSize, numChunks, standardChunkSize, checkSum, isTextFile := UnpackMetadata(metadata)
	chunkName, actualChunkSize, chunkCheckSum, offset := UnpackChunkMetadata(chunkMetadata)

	file, err := os.Create(context.rootDir + "meta_"+ chunkName)
	defer file.Close()
	if err != nil {
		fmt.Println(err.Error())
		log.Println(err.Error())
		return err
	}

	metadataBytes := []byte(fileName + "," +
		strconv.Itoa(fileSize) + "," +
		strconv.Itoa(numChunks) + "," +
		strconv.Itoa(int(standardChunkSize)) + "," +
		strconv.Itoa(int(actualChunkSize)) + "," +
		checkSum + "," +
		chunkCheckSum + "," +
		strconv.FormatBool(isTextFile) + "," +
		strconv.Itoa(int(offset)))

	w := bufio.NewWriter(file)
	w.Write(metadataBytes)
	w.Flush()
	return err
}

func ReadChunk(fileMetadata *messages.Metadata, chunkMetadata *messages.ChunkMetadata, messageHandler *messages.MessageHandler, context context) {
	conn := messageHandler.GetConn()
	chunkName, _, _, _ := UnpackChunkMetadata(chunkMetadata)
	err := WriteMetadataFile(fileMetadata, chunkMetadata, context)
	if err != nil {
		log.Fatalln(err.Error())
	}

	file, err := os.OpenFile(context.rootDir + chunkName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		fmt.Println(err.Error())
		log.Println(err.Error())
	}
	log.Println("writiing " + chunkName)

	writer := bufio.NewWriter(file)
	buffer := make([]byte, chunkMetadata.ChunkSize)
	numBytes, err := io.ReadFull(conn, buffer)
	if err != nil {
		log.Println(err.Error())
	}
	log.Println(chunkMetadata.ChunkName + " read " + strconv.Itoa(numBytes) + " bytes from connection")

	log.Println(chunkMetadata.ChunkName + "expecting chunkSize of" + strconv.Itoa(int(chunkMetadata.ChunkSize)))
	checkSum := messages.GetChunkCheckSum(buffer[:chunkMetadata.ChunkSize])
	oldCheckSum := chunkMetadata.ChunkCheckSum
	log.Println(chunkMetadata.ChunkName + " New Checksum: " + checkSum)
	log.Println(chunkMetadata.ChunkName + " Old Checksum: " + oldCheckSum)

	reader := bytes.NewReader(buffer)
	n, err := io.CopyN(writer, reader, int64(chunkMetadata.ChunkSize))
	if err != nil {
		log.Println(err.Error())
	}
	log.Println(chunkMetadata.ChunkName + " wrote " + strconv.Itoa(int(n)) + " bytes to file")
}

func ReadJob(jobFile string, jobLength int, messageHandler *messages.MessageHandler, context context) {
	log.Println("Reading job: " + jobFile)
	file, err := os.OpenFile(context.rootDir + jobFile, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()
	if err != nil {
		fmt.Println(err.Error())
		log.Println(err.Error())
	}

	conn := messageHandler.GetConn()
	writer := bufio.NewWriter(file)
	buffer := make([]byte, jobLength)
	numBytes, err := io.ReadFull(conn, buffer)
	if err != nil {
		log.Println(err.Error())

	}
	log.Println("Read job: " + jobFile + " - " + strconv.Itoa(numBytes) + " bytes")

	reader := bytes.NewReader(buffer)
	n, err := io.CopyN(writer, reader, int64(jobLength))
	if err != nil {
		log.Println(err.Error())
	}
	log.Println("Wrote job: " + jobFile + " - " + strconv.Itoa(int(n)) + " bytes" )
}

func RunMapJob(chunk string, jobFile string, messageHandler *messages.MessageHandler, context context, reducerCandidates []string, jobId string) {
	log.Println("Running map job")
	//run map job
	//store temp results
	//iterate temp results and send to reducers
	//add reducers to reducer list
	msg := messages.ReduceJobInput{}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ReduceJobInputMessage{
			ReduceJobInputMessage: &msg,
		},
	}
	for i := range reducerCandidates {
		//send something to reducer
		var conn net.Conn
		var err error
		for {
			if conn, err = net.Dial("tcp", reducerCandidates[i]); err != nil {
				log.Println("trying conn again" + reducerCandidates[i])
				time.Sleep(1000 * time.Millisecond)
			} else {
				break
			}
		}
		reducerMessageHandler := messages.NewMessageHandler(conn)
		reducerMessageHandler.Send(wrapper)
		reducerMessageHandler.Close()
	}
	//send ack to comp manager
	msg2 := messages.MapCompleteAck{
		JobId: jobId,
	}
	wrapper2 := &messages.Wrapper{
		Msg: &messages.Wrapper_MapCompleteAckMessage{
			MapCompleteAckMessage: &msg2,
		},
	}
	messageHandler.Send(wrapper2)
	log.Println("Map job complete: " + jobFile)
}

func DeleteChunk(chunkName string, context context) {
	log.Println("Delete chunk request received for " + chunkName)
	err := os.Remove(context.rootDir + chunkName)
	if err != nil {
		log.Println(err.Error())
	}
	err = os.Remove(context.rootDir + "meta_" + chunkName)
	if err != nil {
		log.Println(err.Error())
	}
	if err == nil {
		log.Println("File deleted")
	}
}

func SendChunk(chunkName string, context context, messageHandler *messages.MessageHandler, sendingToStorageNode bool) {
	log.Println(chunkName)
	file, err := os.Open(context.rootDir + chunkName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer file.Close()

	metadata, chunkMetadata := PackageMetadata(context, chunkName)
	var wrapper *messages.Wrapper
	if sendingToStorageNode {
		log.Println("Sending chunks to other storage node")
		wrapper = PackagePutRequestChunk(chunkMetadata, metadata, make([]string, 0))
	} else {
		wrapper = PackageGetResponseChunk(chunkMetadata, metadata)
	}
	messageHandler.Send(wrapper)
	writer := bufio.NewWriter(messageHandler.GetConn())
	bytes, err := io.CopyN(writer, file, int64(chunkMetadata.ChunkSize))
	log.Println("Sent " + strconv.Itoa(int(bytes)) + " bytes")
}

func ForwardChunk(fileMetadata *messages.Metadata, chunkMetadata *messages.ChunkMetadata, forwardingList []string, context context) {
	var nextUp string
	if len(forwardingList) == 0 {
		return
	} else if len(forwardingList) == 1 {
		nextUp = forwardingList[0]
		forwardingList = make([]string, 0)
	} else {
		nextUp = forwardingList[0]
		forwardingList = forwardingList[1:]
	}
	wrapper := PackagePutRequestChunk(chunkMetadata, fileMetadata, forwardingList)
	file, err := os.Open(context.rootDir + chunkMetadata.ChunkName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer file.Close()

	f, _ := file.Stat()
	log.Println("FileSize: " + strconv.Itoa(int(f.Size())))
	buffer := make([]byte, chunkMetadata.ChunkSize)
	_, err = file.Read(buffer)
	if err != nil {
		log.Fatalln()
	}
	reader := bytes.NewReader(buffer)
	var conn net.Conn
	for {
		if conn, err = net.Dial("tcp", nextUp); err != nil {
			log.Println("trying conn again " + nextUp)
			time.Sleep(1000 * time.Millisecond)
		} else {
			messageHandler := messages.NewMessageHandler(conn)
			messageHandler.Send(wrapper)
			writer := bufio.NewWriter(conn)
			n, err := io.CopyN(writer, reader, int64(chunkMetadata.ChunkSize))
			log.Println("Forwarding " + strconv.Itoa(int(n)) + "/" + strconv.Itoa(int(chunkMetadata.ChunkSize)))
			if err != nil {
				fmt.Print(err.Error())
			}
			messageHandler.Close()
			break
		}
	}

}

func PackagePutRequestChunk(chunkMetadata *messages.ChunkMetadata, fileMetadata *messages.Metadata, forwardingList []string) *messages.Wrapper {
	msg := messages.PutRequest{
		Metadata: fileMetadata,
		ChunkMetadata: chunkMetadata,
		ForwardingList: forwardingList}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{
			PutRequestMessage: &msg},
	}
	return wrapper
}

func PackageGetResponseChunk(chunkMetadata *messages.ChunkMetadata, fileMetadata *messages.Metadata) *messages.Wrapper {
	message := &messages.GetResponseChunk{
		ChunkMetadata: chunkMetadata,
		Metadata: fileMetadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetResponseChunkMessage{
			GetResponseChunkMessage: message},
	}
	return wrapper
}

func HandleConnection(conn net.Conn, context context) {
	messageHandler := messages.NewMessageHandler(conn)
	for {
		request, _ := messageHandler.Receive()
		switch msg := request.Msg.(type) {
		case *messages.Wrapper_PutRequestMessage:
			metadata := msg.PutRequestMessage.Metadata
			chunkMetadata := msg.PutRequestMessage.ChunkMetadata
			forwardingList := msg.PutRequestMessage.ForwardingList
			ReadChunk(metadata, chunkMetadata, messageHandler, context)
			ForwardChunk(metadata, chunkMetadata, forwardingList, context)
			messageHandler.Close()
			return
		case *messages.Wrapper_MapJobRequestMessage:
			chunk := msg.MapJobRequestMessage.ChunkName
			job := msg.MapJobRequestMessage.JobFileName
			jobSize := msg.MapJobRequestMessage.JobFileSize
			reducerCandidates := msg.MapJobRequestMessage.ReducerCandidates
			jobId := msg.MapJobRequestMessage.JobId
			ReadJob(job, int(jobSize), messageHandler, context)
			RunMapJob(chunk, job, messageHandler, context, reducerCandidates, jobId)
			messageHandler.Close()
			return
		case *messages.Wrapper_ReduceJobInputMessage:
			log.Println("Reduce job input received")
			log.Println("Saving results to disk and waiting for message from comp manager")
			messageHandler.Close()
			return
		case *messages.Wrapper_ReduceJobRequestMessage:
			jobId := msg.ReduceJobRequestMessage.JobId
			fileName := msg.ReduceJobRequestMessage.JobFileName
			log.Println("Reduce job request received: " + jobId + " - " + fileName)
			log.Println("Sending response to client")
			//check if file exists, run reduce job
			msg2 := messages.MapReduceJobResponse{
				JobId: jobId,
				JobFound: true,
			}
			wrapper2 := &messages.Wrapper{
				Msg: &messages.Wrapper_MapReduceJobResponseMessage{
					MapReduceJobResponseMessage: &msg2},
			}
			messageHandler.Send(wrapper2)
			log.Println("Response sent to client")
			return
		case *messages.Wrapper_DeleteRequestMessage:
			chunkName := msg.DeleteRequestMessage.FileName
			DeleteChunk(chunkName, context)
			messageHandler.Close()
			return
		case *messages.Wrapper_GetRequestMessage:
			chunkName := msg.GetRequestMessage.FileName
			SendChunk(chunkName, context, messageHandler, false)
			messageHandler.Close()
			return
		case *messages.Wrapper_RecoveryInstructionMessage:
			log.Println("Received recovery instruction message")
			receiver := msg.RecoveryInstructionMessage.Receiver
			chunkName := msg.RecoveryInstructionMessage.Chunk
			log.Println(receiver +  ":" + chunkName)
			nodeMessageHandler := messages.EstablishConnection(receiver)
			SendChunk(chunkName, context, nodeMessageHandler, true)
			nodeMessageHandler.Close()
			messageHandler.Close()
			return
		case nil:
			log.Println("nil")
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func InitializeContext() context {
	listeningPort, rootDir, controllerName, controllerPort := HandleArgs()
	return context{
		rootDir: rootDir,
		listeningPort: listeningPort,
		controllerName: controllerName,
		controllerPort: controllerPort}
}

type context struct {
	rootDir string
	listeningPort string
	controllerName string
	controllerPort string
}

func main() {
	context := InitializeContext()
	InitializeLogger()

	go SendHeartBeats(context)

	listener, err := net.Listen("tcp", ":" + context.listeningPort)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for {
		if conn, err := listener.Accept(); err == nil {
			go HandleConnection(conn, context)
		}
	}
}
