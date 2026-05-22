package main

import (
	"flag"
	"log"
	"net"
	"os"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/tcp"
)

const defaultListenAddress = ":6379"
const indexSchemaConfigEnv = "VITALEDGE_INDEX_SCHEMA_CONFIG"

type Config struct {
	ListenAddress   string
	IndexConfigPath string
	IndexCatalog    *indexschema.Catalog
}

type Server struct {
	Config
	ln        net.Listener
	peers     map[*tcp.Peer]bool
	addPeerCh chan *tcp.Peer
	quitCh    chan struct{}
	msgCh     chan tcp.PeerMessage
	indexDDL  *indexschema.Catalog
}

func NewServer(config Config) *Server {
	if len(config.ListenAddress) == 0 {
		config.ListenAddress = defaultListenAddress
	}

	return &Server{
		Config:    config,
		peers:     make(map[*tcp.Peer]bool),
		addPeerCh: make(chan *tcp.Peer),
		quitCh:    make(chan struct{}),
		msgCh:     make(chan tcp.PeerMessage),
		indexDDL:  config.IndexCatalog,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.ListenAddress)
	if err != nil {
		return err
	}

	s.ln = ln

	log.Printf("Server is listening on %s", s.ListenAddress)
	go s.loop()

	return s.accept()
}

func (s *Server) loop() {
	for {
		select {
		case peer := <-s.addPeerCh:
			s.peers[peer] = true
		case msg := <-s.msgCh:
			if err := s.handleMessage(msg); err != nil {
				log.Printf("Error handling message: %v", err)
			}
		case <-s.quitCh:
			return
		}
	}
}

func (s *Server) accept() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			continue
		}

		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	peer := tcp.NewPeer(conn, s.msgCh)
	s.addPeerCh <- peer

	log.Printf("New peer connected: %s", conn.RemoteAddr())

	peer.ReadLoop()
}

func (s *Server) handleMessage(msg tcp.PeerMessage) error {
	log.Printf("Handling message from %s: %s", msg.Peer.Conn.RemoteAddr(), string(msg.Data))
	return nil
}

func main() {
	config, err := loadConfigFromStartup()
	if err != nil {
		log.Fatalf("startup config error: %v", err)
	}

	server := NewServer(config)
	if config.IndexCatalog != nil {
		log.Printf("Loaded index schema catalog")
	}
	server.Start()
}

func loadConfigFromStartup() (Config, error) {
	var listenAddress string
	var indexConfigPath string

	flag.StringVar(&listenAddress, "listen", defaultListenAddress, "Listen address")
	flag.StringVar(&indexConfigPath, "index-schema-config", "", "Path to index schema configuration JSON file")
	flag.Parse()

	if strings.TrimSpace(indexConfigPath) == "" {
		indexConfigPath = strings.TrimSpace(os.Getenv(indexSchemaConfigEnv))
	}

	cfg := Config{ListenAddress: listenAddress, IndexConfigPath: indexConfigPath}
	if strings.TrimSpace(indexConfigPath) == "" {
		return cfg, nil
	}

	catalog, err := indexschema.LoadCatalogFromFile(indexConfigPath)
	if err != nil {
		return Config{}, err
	}
	cfg.IndexCatalog = catalog
	return cfg, nil
}
