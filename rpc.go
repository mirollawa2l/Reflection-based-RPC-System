package rpc

// Package rpc is an intentionally incomplete RMI assignment template.

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
type Server struct {
	receiver   any
	listener   net.Listener
	stopped    chan struct{}
	acceptDone chan struct{}
	workers    sync.WaitGroup
	mu         sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}
	value := reflect.ValueOf(receiver)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}
	return &Server{receiver: receiver}, nil
}

// TODO 1: Create the TCP listener, reject duplicate starts, save lifecycle state,
// and launch the accept loop without blocking the caller.
func (s *Server) Start(address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	//  Benet2kd an el server msh sh8al asln (law el listener msh nil).
	// Dah bymn3 en 7ad y3ml start marra tanya w hwa sh8al (reject duplicate starts).
	if s.listener != nil {
		return errors.New("server is already running")
	}

	//  Benft7 TCP listener 3ala el address el matloob (lw b3t "127.0.0.1:0",
	// el OS hwa elly bykhtar port fadya we ye7gezha).
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	//  Ben7fez el listener gwa el server struct w ben3ml initialization ll channels
	// elly btsa3dna nenazelem el accept loop w n2flha b aman.
	s.listener = listener
	s.stopped = make(chan struct{})
	s.acceptDone = make(chan struct{})

	//  Benbda2 el accept loop fe background goroutine 3ashan el function teRga3
	// men gher ma ta3ml block ll caller w tefdal tst2bel connections gdeda.
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	defer close(s.acceptDone)
	for {
		s.mu.Lock()
		listener := s.listener
		s.mu.Unlock()

		if listener == nil {
			return
		}

		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			select {
			case <-s.stopped:
				s.mu.Unlock()
				return
			default:
				s.mu.Unlock()
				return
			}
		}

		s.mu.Lock()
		if s.listener == nil {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.workers.Add(1)
		s.mu.Unlock()

		go func(c net.Conn) {
			defer s.workers.Done()
			s.handle(c)
		}(conn)
	}
}

// Task 2: `Server.Address` (ageeb 3la el address elly el server sh8al 3aleih)

func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Law el server msh sh8al (listener == nil), benrg3 empty string
	// ( abl el start aw ba3d el stop).
	if s.listener == nil {
		return ""
	}

	// Benrg3 el string bta3 el address elly el listener sh8al 3aleih
	// (w da by3rafna el port el 7a2i2y law knt ba3t port 0 fe el Start).
	return s.listener.Addr().String()
}

//Task 3: `Server.Stop` (Ezaay n2fl el Server  w nstany el active requests tkhls)

func (s *Server) Stop() error {
	s.mu.Lock()

	// Law el server asln msh sh8al, benrg3  (safe to call before start or multiple times).
	if s.listener == nil {
		s.mu.Unlock()
		return nil
	}

	listener := s.listener
	s.listener = nil // Benfeady el listener w nsfro
	stoppedChan := s.stopped
	s.mu.Unlock()

	// 1. Benbe3t signal ll accept loop enha tetla3 (close el stopped channel)
	// w ben2fl el listener 3ashan el Accept() yetkaser wmayfdalsh mstny.
	close(stoppedChan)
	listener.Close()

	// 2. Benstany el accept loop tetla3 tamam
	<-s.acceptDone

	// 3. Benstany kol el active workers (connections elly betaamalha handle gowa)
	// enha tkheles sh8alha tamam (workers.Wait). Dah bykhly el server yestany el active requests tkhls.
	s.workers.Wait()

	return nil
}

// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.
func (s *Server) handle(connection net.Conn) {
	defer connection.Close()
	encoder := json.NewEncoder(connection)
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = encoder.Encode(response{Error: fmt.Sprintf("remote method panicked: %v", recovered)})
		}
	}()

	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, maxRequestBytes+1)))
	var req request
	if err := decoder.Decode(&req); err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("invalid request: %v", err)})
		return
	}
	if err := validateRequest(req); err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	result, err := s.invoke(req)
	if err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("encode result: %v", err)})
		return
	}
	_ = encoder.Encode(response{OK: true, Value: payload})
}

// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.
func validateRequest(req request) error {
	return errors.New("TODO: implement validateRequest")
}

// TODO 6: Study and test this starter reflection bridge. Your required work is to
// support and test exported methods with zero arguments, typed arguments, no
// return value, one return value, and a final error return; unknown methods,
// wrong argument counts, invalid JSON arguments, returned errors, and panics must
// become clear errors instead of crashing the server. Extend the bridge only when
// your chosen assignment protocol requires another result or argument rule.
// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
func (s *Server) invoke(req request) (any, error) {
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
	typeOfMethod := method.Type()
	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}
	arguments := make([]reflect.Value, len(req.Args))
	for index, raw := range req.Args {
		argument := reflect.New(typeOfMethod.In(index))
		if err := json.Unmarshal(raw, argument.Interface()); err != nil {
			return nil, fmt.Errorf("argument %d: %v", index, err)
		}
		arguments[index] = argument.Elem()
	}
	outputs, err := callMethodSafely(method, arguments)
	if err != nil {
		return nil, err
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	if last := outputs[len(outputs)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}
		outputs = outputs[:len(outputs)-1]
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs[0].Interface(), nil
}

func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote method panicked: %v", recovered)
		}
	}()
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
type Client struct {
	Address string
	Timeout time.Duration
}

// NewClient creates a client for a server address.
func NewClient(address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.
func (c *Client) Call(method string, out any, args ...any) error {
	return errors.New("TODO: implement Client.Call")
}
