package tcp

import (
	"errors"
	"log"
	"net"
	"strings"

	cypherparser "github.com/paegun/vitaledge/internal/cypher/parser"
)

const (
	readBufferSize = 1024
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
	buf := make([]byte, readBufferSize)
	pending := make([]byte, 0, readBufferSize)
	for {
		n, err := p.Conn.Read(buf)
		if err != nil {
			if len(pending) > 0 {
				payload := strings.TrimSpace(string(pending))
				pending = pending[:0]
				if payload != "" {
					log.Printf("Received from %s: %s", p.Conn.RemoteAddr(), payload)
					p.msgCh <- PeerMessage{Data: []byte(payload), Peer: p}
				}
			}
			log.Printf("Peer disconnected: %s", p.Conn.RemoteAddr())
			p.Conn.Close()
			return
		}

		pending = append(pending, buf[:n]...)
		payload := strings.TrimSpace(string(pending))
		if payload == "" {
			continue
		}
		if !isTerminalCypherMessage(payload) {
			continue
		}

		log.Printf("Received from %s: %s", p.Conn.RemoteAddr(), payload)
		p.msgCh <- PeerMessage{Data: []byte(payload), Peer: p}
		pending = pending[:0]
	}
}

func isTerminalCypherMessage(payload string) bool {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return false
	}

	_, err := cypherparser.ParseStatement(payload)
	if err == nil {
		return true
	}

	var parseErr *cypherparser.ParseError
	if !errors.As(err, &parseErr) {
		return true
	}
	if parseErr.Kind != cypherparser.ParseErrorSyntax {
		return true
	}
	if looksIncompleteCypher(payload, parseErr) {
		return false
	}
	return true
}

func looksIncompleteCypher(payload string, parseErr *cypherparser.ParseError) bool {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return true
	}
	singleLine := !strings.ContainsAny(payload, "\r\n")
	if hasTrailingContinuation(payload) {
		return true
	}
	if !singleLine && hasUnclosedCypherDelimiter(payload) {
		return true
	}
	if parseErr == nil {
		return false
	}
	msg := strings.ToLower(parseErr.Message)
	if strings.Contains(msg, "<eof>") || strings.Contains(msg, "eof") {
		return true
	}
	lastLine := payload
	if i := strings.LastIndexAny(payload, "\r\n"); i >= 0 && i < len(payload)-1 {
		lastLine = payload[i+1:]
	}
	if parseErr.Column > 0 && parseErr.Column >= len(lastLine) {
		return true
	}
	return false
}

func hasTrailingContinuation(payload string) bool {
	if payload == "" {
		return false
	}
	switch payload[len(payload)-1] {
	case ',', '(', '[', '{', ':':
		return true
	default:
		return false
	}
}

func hasUnclosedCypherDelimiter(payload string) bool {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	inBacktick := false

	for i := 0; i < len(payload); i++ {
		ch := payload[i]

		if inSingle {
			if ch == '\'' {
				if i+1 < len(payload) && payload[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '\\' {
				i++
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				if i+1 < len(payload) && payload[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			depthParen++
		case ')':
			depthParen--
		case '[':
			depthBracket++
		case ']':
			depthBracket--
		case '{':
			depthBrace++
		case '}':
			depthBrace--
		}
	}

	return inSingle || inDouble || inBacktick || depthParen > 0 || depthBracket > 0 || depthBrace > 0
}
