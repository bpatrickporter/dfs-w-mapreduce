package messages

import (
	"encoding/binary"
	"google.golang.org/protobuf/proto"
	"net"
)

type MessageHandler struct {
	conn net.Conn
}

func NewMessageHandler(conn net.Conn) *MessageHandler {
	m := &MessageHandler{
		conn: conn,
	}

	return m
}

func (m *MessageHandler) Send(wrapper *Wrapper) error {
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	m.conn.Write(prefix)
	m.conn.Write(serialized)

	return nil
}

func (m *MessageHandler) Receive() (*Wrapper, error) {
	prefix := make([]byte, 8)
	m.conn.Read(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	m.conn.Read(payload)

	wrapper := &Wrapper{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

func (m *MessageHandler) GetConn() net.Conn {
	return m.conn
}

func (m *MessageHandler) GetRemote() string {
	address := m.conn.RemoteAddr().String()
	return address
}

func (m *MessageHandler) Close() {
	m.conn.Close()
}