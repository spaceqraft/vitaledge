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
		p.msgCh <- PeerMessage{Data: msg, Peer: p}
	}
}
