package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// TCPTransport manages outbound connections to peers and
// exposes an inbound listener for incoming RPCs.
//
// Frame format: [MsgType: 1B][BodyLen: 4B LE][gob body]
type TCPTransport struct {
	listenAddr string
	listener   net.Listener

	mu    sync.Mutex
	conns map[NodeID]net.Conn // one active conn per peer (lazy connect)

	stopCh chan struct{}

	// Callbacks set by Node after transport is created.
	OnRequestVote     func(NodeID, RequestVoteArgs) (RequestVoteReply, error)
	OnAppendEntries   func(NodeID, AppendEntriesArgs) (AppendEntriesReply, error)
	OnInstallSnapshot func(NodeID, InstallSnapshotArgs) (InstallSnapshotReply, error)
}

// NewTCPTransport binds the TCP listener. Does NOT start accepting yet.
func NewTCPTransport(listenAddr string) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("raft transport listen %s: %w", listenAddr, err)
	}
	return &TCPTransport{
		listenAddr: listenAddr,
		listener:   ln,
		conns:      make(map[NodeID]net.Conn),
		stopCh:     make(chan struct{}),
	}, nil
}

// Start starts the accept loop in a background goroutine.
func (t *TCPTransport) Start() error {
	go t.acceptLoop()
	return nil
}

func (t *TCPTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				// Transient error; keep accepting.
				continue
			}
		}
		go t.handleConn(conn)
	}
}

// handleConn handles inbound RPCs on a single connection (server side).
func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		msgType, body, err := readFrame(conn)
		if err != nil {
			return
		}

		switch msgType {
		case MsgRequestVote:
			var args RequestVoteArgs
			if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&args); err != nil {
				return
			}
			var reply RequestVoteReply
			if t.OnRequestVote != nil {
				reply, _ = t.OnRequestVote("", args)
			}
			replyFrame, err := encodeFrame(MsgRequestVoteReply, reply)
			if err != nil {
				return
			}
			if _, err := conn.Write(replyFrame); err != nil {
				return
			}

		case MsgAppendEntries:
			var args AppendEntriesArgs
			if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&args); err != nil {
				return
			}
			var reply AppendEntriesReply
			if t.OnAppendEntries != nil {
				reply, _ = t.OnAppendEntries("", args)
			}
			replyFrame, err := encodeFrame(MsgAppendEntriesReply, reply)
			if err != nil {
				return
			}
			if _, err := conn.Write(replyFrame); err != nil {
				return
			}

		case MsgInstallSnapshot:
			var args InstallSnapshotArgs
			if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&args); err != nil {
				return
			}
			var reply InstallSnapshotReply
			if t.OnInstallSnapshot != nil {
				reply, _ = t.OnInstallSnapshot("", args)
			}
			replyFrame, err := encodeFrame(MsgInstallSnapshotReply, reply)
			if err != nil {
				return
			}
			if _, err := conn.Write(replyFrame); err != nil {
				return
			}

		default:
			// Unknown message type; close connection.
			return
		}
	}
}

// SendRequestVote sends a RequestVote RPC to the given peer.
func (t *TCPTransport) SendRequestVote(peer NodeID, addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	respBytes, err := t.send(peer, addr, MsgRequestVote, args)
	if err != nil {
		return RequestVoteReply{}, err
	}
	var reply RequestVoteReply
	if err := gob.NewDecoder(bytes.NewReader(respBytes)).Decode(&reply); err != nil {
		return RequestVoteReply{}, fmt.Errorf("decode RequestVoteReply: %w", err)
	}
	return reply, nil
}

// SendAppendEntries sends an AppendEntries RPC to the given peer.
func (t *TCPTransport) SendAppendEntries(peer NodeID, addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	respBytes, err := t.send(peer, addr, MsgAppendEntries, args)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	var reply AppendEntriesReply
	if err := gob.NewDecoder(bytes.NewReader(respBytes)).Decode(&reply); err != nil {
		return AppendEntriesReply{}, fmt.Errorf("decode AppendEntriesReply: %w", err)
	}
	return reply, nil
}

// SendInstallSnapshot sends an InstallSnapshot RPC to the given peer.
func (t *TCPTransport) SendInstallSnapshot(peer NodeID, addr string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	respBytes, err := t.send(peer, addr, MsgInstallSnapshot, args)
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	var reply InstallSnapshotReply
	if err := gob.NewDecoder(bytes.NewReader(respBytes)).Decode(&reply); err != nil {
		return InstallSnapshotReply{}, fmt.Errorf("decode InstallSnapshotReply: %w", err)
	}
	return reply, nil
}

// Close closes the transport and all open connections.
// Addr returns the actual listen address of the transport (host:port).
// When the transport was created with ":0" this returns the OS-assigned port.
func (t *TCPTransport) Addr() string {
	return t.listener.Addr().String()
}

func (t *TCPTransport) Close() error {
	close(t.stopCh)
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
	t.conns = make(map[NodeID]net.Conn)
	return t.listener.Close()
}

// getConn returns or lazily creates a connection to peer at addr.
func (t *TCPTransport) getConn(peer NodeID, addr string) (net.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return c, nil
	}
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	t.conns[peer] = c
	return c, nil
}

// send encodes the body as a frame, writes to peer's conn, and reads reply.
func (t *TCPTransport) send(peer NodeID, addr string, msgType uint8, body interface{}) ([]byte, error) {
	frame, err := encodeFrame(msgType, body)
	if err != nil {
		return nil, err
	}

	conn, err := t.getConn(peer, addr)
	if err != nil {
		return nil, err
	}

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.removeConn(peer)
		return nil, err
	}

	if _, err := conn.Write(frame); err != nil {
		t.removeConn(peer)
		return nil, fmt.Errorf("write to %s: %w", peer, err)
	}

	_, respBody, err := readFrame(conn)
	if err != nil {
		t.removeConn(peer)
		return nil, fmt.Errorf("read reply from %s: %w", peer, err)
	}

	return respBody, nil
}

// removeConn removes and closes the connection for peer.
func (t *TCPTransport) removeConn(peer NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		_ = c.Close()
		delete(t.conns, peer)
	}
}

// encodeFrame encodes msgType + gob-encoded body into a frame.
func encodeFrame(msgType uint8, body interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(body); err != nil {
		return nil, fmt.Errorf("gob encode: %w", err)
	}

	bodyBytes := buf.Bytes()
	frame := make([]byte, 1+4+len(bodyBytes))
	frame[0] = msgType
	binary.LittleEndian.PutUint32(frame[1:5], uint32(len(bodyBytes)))
	copy(frame[5:], bodyBytes)
	return frame, nil
}

// readFrame reads a frame from conn: [MsgType:1B][BodyLen:4B LE][body].
func readFrame(conn net.Conn) (msgType uint8, body []byte, err error) {
	var header [5]byte
	if _, err = io.ReadFull(conn, header[:]); err != nil {
		return 0, nil, err
	}
	msgType = header[0]
	bodyLen := binary.LittleEndian.Uint32(header[1:5])
	body = make([]byte, bodyLen)
	if _, err = io.ReadFull(conn, body); err != nil {
		return 0, nil, err
	}
	return msgType, body, nil
}
