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
	"unicode"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

// const (
// 	requestReadTimeout   = 5 * time.Second
// 	responseWriteTimeout = 5 * time.Second
// 	drainTimeout         = time.Second
// )

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
	mu         sync.Mutex // guards listener, stopped and acceptDone (held only briefly
    lifecycle  sync.Mutex // serializes Start and Stop so they never overlap
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
	s.lifecycle.Lock()          // Acquire the lock to prevent other goroutines from accessing the protected resource simultaneously.
defer s.lifecycle.Unlock()  // Ensure the lock is released when the function returns, even if it returns early.
	
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

// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.
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

// TODO 3: Close the listener, signal the accept loop, wait for its workers,
// and leave the server in a state where Start can be called again.
// Stop stops accepting new connections and waits for active handlers to finish.

func (s *Server) Stop() error {
	// Holding lifecycle for the whole shutdown means a concurrent Start waits
	// for Stop to finish and a concurrent second Stop becomes a no-op.
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	
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

const readTimeout = 5 * time.Second
	const writeTimeout = 5 * time.Second
	const drainTimeout= time.Second

func (s *Server) handle(connection net.Conn) {
	// Close the connection when the handler finishes.
	defer connection.Close()

	// Prepare the JSON encoder used to send the response.
	encoder := json.NewEncoder(connection)

	// Track whether unread client input should be drained before closing.
	drainInput := false

	// sendResponse writes one response and marks failed requests for draining.
	sendResponse := func(resp response) {
		if !resp.OK {
			drainInput = true
		}

		// Prevent writing the response from blocking indefinitely.
		_ = connection.SetWriteDeadline(
			time.Now().Add(writeTimeout),
		)

		_ = encoder.Encode(resp)
	}

	// Deferred calls run last-in, first-out:
	// recover first, then drain unread input, then close the connection.
	defer func() {
		if drainInput {
			drainConnection(connection)
		}
	}()

	// Recover from unexpected panics so they do not crash the server.
	defer func() {
		if recovered := recover(); recovered != nil {
			sendResponse(response{
				Error: fmt.Sprintf(
					"remote method panicked: %v",
					recovered,
				),
			})
		}
	}()

	// Set a deadline so reading the request cannot block indefinitely.
	if err := connection.SetReadDeadline(
		time.Now().Add(readTimeout),
	); err != nil {
		sendResponse(response{
			Error: fmt.Sprintf("set read deadline: %v", err),
		})
		return
	}

	// Read at most maxRequestBytes + 1 bytes to detect oversized requests.
	decoder := json.NewDecoder(
		bufio.NewReader(
			io.LimitReader(connection, maxRequestBytes+1),
		),
	)

	var req request

	if err := decoder.Decode(&req); err != nil {
		sendResponse(response{
			Error: fmt.Sprintf("invalid request: %v", err),
		})
		return
	}

	// Clear the read deadline after receiving the request.
	_ = connection.SetReadDeadline(time.Time{})

	// Reject oversized JSON requests.
	if decoder.InputOffset() > maxRequestBytes {
		sendResponse(response{
			Error: "request is larger than allowed",
		})
		return
	}

	// Validate the request before invoking the remote method.
	if err := validateRequest(req); err != nil {
		sendResponse(response{
			Error: err.Error(),
		})
		return
	}

	// Find and execute the requested method.
	result, err := s.invoke(req)
	if err != nil {
		sendResponse(response{
			Error: err.Error(),
		})
		return
	}

	// Convert the method result into JSON.
	payload, err := json.Marshal(result)
	if err != nil {
		sendResponse(response{
			Error: fmt.Sprintf("encode result: %v", err),
		})
		return
	}

	// Send the successful response.
	sendResponse(response{
		OK:    true,
		Value: payload,
	})
}


// drainConnection discards (a bounded amount of) leftover client input.
func drainConnection(connection net.Conn) {
	_ = connection.SetReadDeadline(time.Now().Add(drainTimeout))
	_, _ = io.Copy(io.Discard, io.LimitReader(connection, 2*maxRequestBytes))
}


// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.
func validateRequest(req request) error {

	// 1. Check if the method name is empty.
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}

	// 2. Check if the method name contains whitespace or control characters.
	for _, ch := range req.Method {
		if unicode.IsSpace(ch) || unicode.IsControl(ch) {
			return errors.New("method contains whitespace or control characters")
		}
	}

	// 3. Check if the number of arguments exceeds the maximum allowed.
	if len(req.Args) > maxArguments {
		return errors.New("too many arguments")
	}

	// 4. Check the size of each argument and calculate the total size.
	totalSize := 0

	for _, arg := range req.Args {

		// Reject an individual argument that exceeds the size limit.
		if len(arg) > maxArgumentBytes {
			return errors.New("argument is larger than maxArgumentBytes")
		}

		// Add the argument size to the total.
		totalSize += len(arg)
	}

	// 5. Reject requests whose combined argument size exceeds the limit.
	if totalSize > maxRequestBytes {
		return errors.New("request is larger than maxRequestBytes")
	}

	// All validation checks passed.
	return nil
}

// TODO 6: Study and test this starter reflection bridge. Your required work is to
// support and test exported methods with zero arguments, typed arguments, no
// return value, one return value, and a final error return; unknown methods,
// wrong argument counts, invalid JSON arguments, returned errors, and panics must
// become clear errors instead of crashing the server. Extend the bridge only when
// your chosen assignment protocol requires another result or argument rule.
// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
func (s *Server) invoke(req request) (any, error) {
	//find the method name by relection
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	//if meth not exist err
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
	//get meth typr info and check if the number of arguments matches the number of arguments in the request
	typeOfMethod := method.Type()

	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}

	// Check supported return signatures.
	errorType := reflect.TypeOf((*error)(nil)).Elem()

	switch typeOfMethod.NumOut() {
	case 0, 1:
		// Supported: no result, one result, or error only.

	case 2:
		// Two results require the final result to be an error.
		if !typeOfMethod.Out(1).Implements(errorType) {
			return nil, fmt.Errorf(
				"method %q has unsupported return signature",
				req.Method,
			)
		}

	default:
		return nil, fmt.Errorf(
			"method %q has unsupported return signature",
			req.Method,
		)
	}

	// Convert JSON arguments into Go values.
	arguments := make([]reflect.Value, len(req.Args))
	for index, raw := range req.Args {
		//find expected arg type and allocate value
		argument := reflect.New(typeOfMethod.In(index))
		//decode raw JSOn into that type
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
		outputs = outputs[:len(outputs)-1] // Remove the nil error from the results.
	}
	//Handle methods returning only a nil error.
	if len(outputs) == 0 {
		return nil, nil
	}
	//Return the successful method result.
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
// Client.Call invokes a remote method and decodes its result into out. Pass nil for no result.

func (c *Client) Call(method string, out any, args ...any) error {
	if method == "" {
		return errors.New("method cannot be empty")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
 
	// Encode first: a bad argument fails fast without touching the network.
	encoded := make([]json.RawMessage, len(args)) // never nil, so it is sent as []
	for index, argument := range args {
		raw, err := json.Marshal(argument)
		if err != nil {
			return fmt.Errorf("encode argument %d: %w", index, err)
		}
		encoded[index] = raw
	}
 
	deadline := time.Now().Add(timeout)
	dialer := net.Dialer{Deadline: deadline}
	connection, err := dialer.Dial("tcp", c.Address)
	if err != nil {
		return describeNetError(fmt.Sprintf("connect to %s", c.Address), err, timeout)
	}
	defer connection.Close()
	// One deadline covers the whole exchange: sending, the remote execution and reading.
	if err := connection.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}
 
	if err := json.NewEncoder(connection).Encode(request{Method: method, Args: encoded}); err != nil {
		return describeNetError("send request", err, timeout)
	}
 
	var reply response
	if err := json.NewDecoder(bufio.NewReader(connection)).Decode(&reply); err != nil {
		return describeNetError("read response", err, timeout)
	}
	if !reply.OK {
		if reply.Error == "" {
			return errors.New("remote call failed")
		}
		return errors.New(reply.Error)
	}
 
	if out == nil || len(reply.Value) == 0 {
		return nil
	}
	if err := json.Unmarshal(reply.Value, out); err != nil {
		return fmt.Errorf("decode result into %T: %w", out, err)
	}
	return nil
}
 
// describeNetError labels an I/O failure and names timeouts explicitly.
func describeNetError(action string, err error, timeout time.Duration) error {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%s: timed out after %v: %w", action, timeout, err)
	}
	return fmt.Errorf("%s: %w", action, err)
}