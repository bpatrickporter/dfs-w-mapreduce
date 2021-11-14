package messages

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net"
	"os"
	"time"
)

func GetCheckSum(f os.File) string {
	hasher := sha256.New()
	if _, err := io.Copy(hasher, &f); err != nil {
		log.Fatal(err)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func GetChunkCheckSum(chunk []byte) string {
	hasher := sha256.New()
	reader := bytes.NewReader(chunk)
	if _, err := io.Copy(hasher, reader); err != nil {
		log.Fatal(err)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func EstablishConnection(receiver string) *MessageHandler {
	var conn net.Conn
	var err error
	for {
		if conn, err = net.Dial("tcp", receiver); err != nil {
			log.Println("trying conn again" + receiver)
			time.Sleep(1000 * time.Millisecond)
		} else {
			break
		}
	}
	return NewMessageHandler(conn)
}