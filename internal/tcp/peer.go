package tcp

import (
	"log"
	"net"
)

type PeerMessage struct {
        Data []byte
        Peer *Peer
}

type Peer struct {
	Conn  net.Conn
	msgCh chan<- PeerMessage
}

func NewPeer(conn net.Conn, msgCh chan<- PeerMessage) *Peer {
	return &Peer{
		Conn:  conn,
		msgCh: msgCh,
	}
}

func (p *Peer) ReadLoop() {
	buf := make([]byte, 1024)
	for {
		n, err := p.Conn.Read(buf)
		if err != nil {
			log.Printf("Peer disconnected: %s", p.Conn.RemoteAddr())
			p.Conn.Close()
			return
		}

		log.Printf("Received from %s: %s", p.Conn.RemoteAddr(), string(buf[:n]))
		msg := make([]byte, n)
		copy(msg, buf[:n])
		query, err := parseCommand(msg)
		if err != nil {
			log.Printf("Failed to parse command: %v", err)
			continue
		}

		log.Printf("Parsed query: %s", query.Query())
		response := make([]byte, len(query.Query()))
		copy(response, query.Query())
		p.Conn.Write(response)
	}
}
