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

// request represents a method invocation sent from a client to the server.
// The method name identifies the receiver's method, while Args contains
// the method arguments encoded as raw JSON messages.
type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

// response represents the server's reply to a client request.
// OK indicates whether the call succeeded.
// Value contains the JSON-encoded return value on success.
// Error contains an error message when the call fails.
type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over a TCP connection.
// It accepts client requests, invokes the requested methods using reflection,
// and sends the results back to the clients.
type Server struct {
	// receiver is the object whose exported methods will be available remotely.
	receiver any

	// listener accepts incoming TCP connections from RPC clients.
	listener net.Listener

	// stopped is closed when the server begins shutting down.
	// The accept loop uses it to detect a requested shutdown.
	stopped chan struct{}

	// acceptDone is closed when the accept loop exits.
	// Stop waits on this channel to ensure the accept loop has finished.
	acceptDone chan struct{}

	// workers tracks active connection handlers.
	// Stop waits for all active handlers to finish before returning.
	workers sync.WaitGroup

	// mu protects shared server state, including listener, stopped,
	// and acceptDone. It is held briefly when accessing these fields.
	mu sync.Mutex

	// lifecycle serializes Start and Stop operations.
	// This prevents starting and stopping the server simultaneously.
	lifecycle sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	// Reject a nil receiver because there is no object to expose remotely.
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}

	// Get reflection information about the receiver to inspect its type and value.
	value := reflect.ValueOf(receiver)

	// Require a non-nil pointer so the server can expose the receiver's methods
	// and modify its underlying object when needed.
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}

	// Create and return a server that stores the receiver for remote method calls.
	return &Server{receiver: receiver}, nil
}

// TODO 1: Create the TCP listener, reject duplicate starts, save lifecycle state,
// and launch the accept loop without blocking the caller.
func (s *Server) Start(address string) error {
	s.lifecycle.Lock()         // Acquire the lifecycle lock to prevent Start and Stop from running simultaneously.
	defer s.lifecycle.Unlock() // Ensure the lifecycle lock is released when the function returns, even if it returns early.

	s.mu.Lock()         // Lock shared server state before checking or modifying it.
	defer s.mu.Unlock() // Automatically release the state lock when Start returns.

	//  Benet2kd an el server msh sh8al asln (law el listener msh nil).
	// Dah bymn3 en 7ad y3ml start marra tanya w hwa sh8al (reject duplicate starts).
	if s.listener != nil {
		return errors.New("server is already running") // Reject starting the server if it is already running.
	}

	//  Benft7 TCP listener 3ala el address el matloob (lw b3t "127.0.0.1:0",
	// el OS hwa elly bykhtar port fadya we ye7gezha).
	listener, err := net.Listen("tcp", address) // Open a TCP listener on the requested address.
	if err != nil {
		return err // Return the error if the address cannot be bound or the listener cannot be created.
	}

	//  Ben7fez el listener gwa el server struct w ben3ml initialization ll channels
	// elly btsa3dna nenazelem el accept loop w n2flha b aman.
	s.listener = listener              // Store the listener so other server methods can access it.
	s.stopped = make(chan struct{})    // Create a channel to signal that the server is stopping.
	s.acceptDone = make(chan struct{}) // Create a channel that signals when the accept loop has finished.

	//  Benbda2 el accept loop fe background goroutine 3ashan el function teRga3
	// men gher ma ta3ml block ll caller w tefdal tst2bel connections gdeda.
	go s.acceptLoop() // Run the accept loop concurrently so Start can return without waiting for connections.
	return nil        // Report that the server started successfully.
}

// acceptLoop continuously accepts incoming TCP connections until the listener is closed.
func (s *Server) acceptLoop() {
	defer close(s.acceptDone) // Signal that the accept loop has finished, even if it exits early.

	for {
		s.mu.Lock()            // Lock shared state before reading the listener.
		listener := s.listener // Copy the current listener while the state is protected.
		s.mu.Unlock()          // Release the lock before performing network operations.

		if listener == nil {
			return // Exit if there is no active listener.
		}

		conn, err := listener.Accept() // Wait for an incoming connection; this blocks until a connection arrives or an error occurs.
		if err != nil {
			s.mu.Lock() // Lock shared state before checking the server's stopping status.
			select {
			case <-s.stopped: // Check whether Stop has signaled that the server is shutting down.
				s.mu.Unlock() // Release the lock before returning.
				return        // Exit because the server has been stopped.
			default:
				s.mu.Unlock() // Release the lock when no stop signal is available.
				return        // Exit the accept loop when Accept fails.
			}
		}

		s.mu.Lock() // Lock shared state before checking the listener and registering a worker.
		if s.listener == nil {
			s.mu.Unlock() // Release the lock before closing the rejected connection.
			conn.Close()  // Close the connection because the server is no longer running.
			return        // Exit because the server has stopped accepting connections.
		}

		s.workers.Add(1) // Register this connection handler so Stop can wait for it to finish.
		s.mu.Unlock()    // Release the lock before starting the handler goroutine.

		go func(c net.Conn) {
			defer s.workers.Done() // Decrease the worker count when this handler finishes.
			s.handle(c)            // Process the request received over this connection.
		}(conn) // Pass the accepted connection to the new goroutine.
	}
}

// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.
func (s *Server) Address() string {
	s.mu.Lock()         // Lock the server state to safely access the listener while other goroutines may modify it.
	defer s.mu.Unlock() // Ensure the mutex is released when the function returns.

	// Law el server msh sh8al (listener == nil), benrg3 empty string
	// ( abl el start aw ba3d el stop).
	if s.listener == nil {
		return "" // Return an empty string if the server has not started or has already stopped.
	}

	// Benrg3 el string bta3 el address elly el listener sh8al 3aleih
	// (w da by3rafna el port el 7a2i2y law knt ba3t port 0 fe el Start).
	return s.listener.Addr().String() // Return the actual listening address, including the assigned port if Start used port 0.
}

// TODO 3: Close the listener, signal the accept loop, wait for its workers,
// and leave the server in a state where Start can be called again.
// Stop stops accepting new connections and waits for active handlers to finish.

func (s *Server) Stop() error {
	// Holding lifecycle for the whole shutdown means a concurrent Start waits
	// for Stop to finish and a concurrent second Stop becomes a no-op.
	s.lifecycle.Lock()         // Acquire the lifecycle lock so Start and Stop cannot execute simultaneously.
	defer s.lifecycle.Unlock() // Release the lifecycle lock when Stop returns.

	s.mu.Lock() // Lock shared server state before checking or modifying it.

	// Law el server asln msh sh8al, benrg3  (safe to call before start or multiple times).
	if s.listener == nil {
		s.mu.Unlock() // Release the mutex before returning.
		return nil    // Nothing needs to be stopped, so return successfully.
	}

	listener := s.listener   // Save the listener locally so we can close it after releasing the mutex.
	s.listener = nil         // Benfeady el listener w nsfro; mark the server as stopped so new connections are not accepted.
	stoppedChan := s.stopped // Save the stop signal channel for the shutdown process.
	s.mu.Unlock()            // Release the mutex before performing shutdown operations.

	// 1. Benbe3t signal ll accept loop enha tetla3 (close el stopped channel)
	// w ben2fl el listener 3ashan el Accept() yetkaser wmayfdalsh mstny.
	close(stoppedChan)   // Notify the accept loop that the server is stopping.
	_ = listener.Close() // Close the TCP listener to unblock any pending Accept() call.

	// 2. Benstany el accept loop tetla3 tamam
	<-s.acceptDone // Wait until the accept loop has completely exited.

	// 3. Benstany kol el active workers (connections elly betaamalha handle gowa)
	// enha tkheles sh8alha tamam (workers.Wait). Dah bykhly el server yestany el active requests tkhls.
	s.workers.Wait() // Wait for all active connection handlers to finish.

	return nil // Report that shutdown completed successfully.
}

// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.

// Maximum time allowed for reading a request from the client.
const readTimeout = 5 * time.Second

// Maximum time allowed for writing a response to the client.
const writeTimeout = 5 * time.Second

// Maximum time allowed to drain unread client input before closing the connection.
const drainTimeout = time.Second

func (s *Server) handle(connection net.Conn) {
	// Close the connection when the handler finishes.
	defer connection.Close() // Release the network connection regardless of how the handler exits.

	// Prepare the JSON encoder used to send the response.
	encoder := json.NewEncoder(connection) // Convert response structs into JSON and write them to the connection.

	// Track whether unread client input should be drained before closing.
	drainInput := false // Start with no need to drain; set to true when an error response is sent.

	// sendResponse writes one response and marks failed requests for draining.
	sendResponse := func(resp response) {
		if !resp.OK {
			drainInput = true // Remember to consume unread input before closing after an error response.
		}

		// Prevent writing the response from blocking indefinitely.
		_ = connection.SetWriteDeadline(
			time.Now().Add(writeTimeout), // Set the deadline to the current time plus the allowed write duration.
		)

		_ = encoder.Encode(resp) // Encode the response as JSON and send it to the client.
	}

	// Deferred calls run last-in, first-out:
	// recover first, then drain unread input, then close the connection.
	defer func() {
		if drainInput {
			drainConnection(connection) // Consume a bounded amount of unread input to help the client receive the response before closure.
		}
	}()

	// Recover from unexpected panics so they do not crash the server.
	defer func() {
		if recovered := recover(); recovered != nil { // Detect a panic and retrieve its value.
			sendResponse(response{
				Error: fmt.Sprintf(
					"remote method panicked: %v", // Explain that the remote method panicked and include the panic value.
					recovered,
				),
			})
		}
	}()

	// Set a deadline so reading the request cannot block indefinitely.
	if err := connection.SetReadDeadline(
		time.Now().Add(readTimeout), // Limit how long the server waits for the client to send the request.
	); err != nil {
		sendResponse(response{
			Error: fmt.Sprintf("set read deadline: %v", err), // Return an error if the read deadline could not be configured.
		})
		return // Stop processing this connection because the read deadline could not be set.
	}

	// Limit the amount of data the decoder can read to one byte
	// more than the maximum allowed request size.
	limited := &io.LimitedReader{
		R: connection,
		N: maxRequestBytes + 1,
	}

	// Decode one JSON request without waiting for the client to close
	// its connection.
	decoder := json.NewDecoder(bufio.NewReader(limited))

	var req request
	if err := decoder.Decode(&req); err != nil {
		sendResponse(response{
			Error: fmt.Sprintf("invalid request: %v", err),
		})
		return
	}

	// If the reader used the extra byte, the request exceeded the limit.
	if limited.N == 0 {
		sendResponse(response{
			Error: "request is larger than allowed",
		})
		return
	}

	// Clear the read deadline after successfully reading the request.
	_ = connection.SetReadDeadline(time.Time{})

	// Validate the request before invoking the remote method.
	if err := validateRequest(req); err != nil {
		sendResponse(response{
			Error: err.Error(), // Send the validation failure to the client.
		})
		return // Stop before reflection or method execution if validation fails.
	}

	// Find and execute the requested method.
	result, err := s.invoke(req) // Use reflection to call the requested method with its decoded arguments.
	if err != nil {
		sendResponse(response{
			Error: err.Error(), // Send unknown-method, argument, or remote-method errors to the client.
		})
		return // Stop because the remote method did not complete successfully.
	}

	// Convert the method result into JSON.
	payload, err := json.Marshal(result) // Serialize the method's result so it can be transmitted over TCP.
	if err != nil {
		sendResponse(response{
			Error: fmt.Sprintf("encode result: %v", err), // Explain that the result could not be serialized.
		})
		return // Stop because the result cannot be encoded.
	}

	// Send the successful response.
	sendResponse(response{
		OK:    true,    // Indicate that the remote call succeeded.
		Value: payload, // Include the JSON-encoded result in the response.
	})
}

// drainConnection discards (a bounded amount of) leftover client input.
func drainConnection(connection net.Conn) {
	_ = connection.SetReadDeadline(time.Now().Add(drainTimeout)) // Ensure draining cannot wait indefinitely for more client data.
	_, _ = io.Copy(
		io.Discard, // Discard the bytes instead of storing them in memory.
		io.LimitReader(connection, 2*maxRequestBytes), // Bound the amount of data consumed while draining.
	)
}

// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.
func validateRequest(req request) error {

	// 1. Check whether the method name is empty.
	// An empty name cannot identify a method to invoke on the server.
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}

	// 2. Check whether the method name contains whitespace or control characters.
	// This helps reject malformed names before they are used for reflection.
	// The loop checks each Unicode character in the method name.
	for _, ch := range req.Method {
		if unicode.IsSpace(ch) || unicode.IsControl(ch) {
			return errors.New("method contains whitespace or control characters")
		}
	}

	// 3. Check whether the number of arguments exceeds the maximum allowed.
	// This prevents requests from supplying more arguments than the RPC system supports.
	if len(req.Args) > maxArguments {
		return errors.New("too many arguments")
	}

	// 4. Check the size of each argument and calculate the total size.
	// RawMessage stores each argument as its original JSON bytes.
	// We count bytes rather than characters to enforce memory and payload limits.
	totalSize := 0

	for _, arg := range req.Args {

		// Reject an individual argument that exceeds the size limit.
		// This prevents any single argument from being excessively large.
		if len(arg) > maxArgumentBytes {
			return errors.New("argument is larger than maxArgumentBytes")
		}

		// Add this argument's byte length to the total request argument size.
		// The accumulated size is checked after all arguments are examined.
		totalSize += len(arg)
	}

	// 5. Reject requests whose combined argument size exceeds the limit.
	// Several individually valid arguments could still exceed the total limit together.
	if totalSize > maxRequestBytes {
		return errors.New("request is larger than maxRequestBytes")
	}

	// All validation checks passed.
	// Returning nil means the request passed these validation checks.
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
	// Use reflection to find the requested method on the registered receiver.
	// MethodByName can find exported methods; if the method does not exist,
	// it returns an invalid reflect.Value.
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)

	// If the method was not found, return a descriptive error instead of
	// attempting to call an invalid method and potentially panicking.
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}

	// Get the method's type information so we can inspect its parameters
	// and return values before calling it.
	typeOfMethod := method.Type()

	// Check whether the number of arguments supplied by the client matches
	// the number of parameters expected by the method.
	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}

	// Check supported return signatures before invoking the method.
	// The error interface type is used to identify methods that return errors.
	errorType := reflect.TypeOf((*error)(nil)).Elem()

	switch typeOfMethod.NumOut() {
	case 0, 1:
		// Supported: no result, one result, or error only.
		// A single return value may itself be an error.
		// If it is an error, the handling below checks whether it is nil.

	case 2:
		// Two results require the final result to implement the error interface.
		// This supports the common Go pattern: (result, error).
		if !typeOfMethod.Out(1).Implements(errorType) {
			return nil, fmt.Errorf(
				"method %q has unsupported return signature",
				req.Method,
			)
		}

	default:
		// Reject methods returning more than two values because their
		// return signature is not supported by this RPC implementation.
		return nil, fmt.Errorf(
			"method %q has unsupported return signature",
			req.Method,
		)
	}

	// Convert the JSON arguments into Go values of the exact parameter types
	// expected by the requested method.
	// Each reflect.Value will be passed to method.Call in the correct order.
	arguments := make([]reflect.Value, len(req.Args))

	for index, raw := range req.Args {
		// Find the expected type of this parameter and allocate a pointer to it.
		// For example, if the method expects an int, this creates a *int.
		argument := reflect.New(typeOfMethod.In(index))

		// Decode the raw JSON argument into the allocated Go value.
		// Interface() exposes the pointer so json.Unmarshal can populate it.
		// If the JSON cannot be converted to the expected type, return an error
		// identifying which argument failed.
		if err := json.Unmarshal(raw, argument.Interface()); err != nil {
			return nil, fmt.Errorf("argument %d: %v", index, err)
		}

		// Dereference the pointer to obtain the actual value expected by
		// the method, rather than passing the pointer itself.
		arguments[index] = argument.Elem()
	}

	// Call the method through the panic-safe helper.
	// The helper returns the method's output values or an error if the
	// method panics during execution.
	outputs, err := callMethodSafely(method, arguments)

	// If the method panicked, propagate the recovered error to the caller.
	if err != nil {
		return nil, err
	}

	// If the method returned no values, it completed successfully without
	// producing a result, so return nil as the RPC result.
	if len(outputs) == 0 {
		return nil, nil
	}

	// Check whether the final return value implements the error interface.
	// If it is a non-nil error, report it instead of returning a successful result.
	if last := outputs[len(outputs)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		// A non-nil error means the remote method reported failure.
		// isNilValue also handles typed nil values stored in interfaces.
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}

		// The final error is nil, so remove it from the outputs.
		// Only the actual result, if any, should be returned to the client.
		outputs = outputs[:len(outputs)-1]
	}

	// Handle methods returning only a nil error.
	// After removing the nil error, no result values remain.
	if len(outputs) == 0 {
		return nil, nil
	}

	// Return the successful method result.
	// The assignment supports at most one non-error result value.
	return outputs[0].Interface(), nil
}

// isNilValue determines whether a reflected value can safely be checked for nil.
// Calling IsNil on a non-nilable type, such as int or string, would panic.
func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	// These kinds can hold nil values in Go, so IsNil is valid for them.
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		// Other kinds cannot be nil, so they are treated as non-nil.
		return false
	}
}

// callMethodSafely invokes a reflected method and converts any panic into an error.
// This prevents a panic inside a remote method from crashing the server process.
func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	// A deferred function runs when this function returns, including when
	// method.Call causes a panic. recover captures the panic value.
	defer func() {
		if recovered := recover(); recovered != nil {
			// Convert the panic into a descriptive error that invoke can handle.
			err = fmt.Errorf("remote method panicked: %v", recovered)
		}
	}()

	// Call the method with the prepared arguments and return its reflected outputs.
	// If the method panics, the deferred recovery function sets err.
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
type Client struct {
	// Address is the network address of the RPC server to connect to.
	Address string

	// Timeout is the maximum allowed duration for a remote call.
	Timeout time.Duration
}

// NewClient creates a client for a server address.
func NewClient(address string) (*Client, error) {
	// Reject an empty address because the client needs a destination to connect to.
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}

	// Create the client with the supplied server address and the default call timeout.
	// The caller can change Timeout later if a different duration is needed.
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.
// Client.Call invokes a remote method and decodes its result into out. Pass nil for no result.

/*
Call performs the following steps:
1. Validate the method name and choose a timeout.
2. Encode the arguments into JSON.
3. Calculate a deadline and connect to the server.
4. Defer closing the connection and set its deadline.
5. Send the RPC request to the server.
6. Read and decode the server's response.
7. Check for remote errors.
8. Decode the returned value into `out`.
9. Return `nil` if successful; otherwise, return the appropriate error.
*/
func (c *Client) Call(method string, out any, args ...any) error {

	// Reject an empty method name before doing any network operations.
	// There is no method for the server to look up if the name is empty.
	if method == "" {
		return errors.New("method cannot be empty")
	}

	// Use the client's configured timeout.
	// If the timeout is zero or negative, fall back to the default timeout.
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}

	// Encode first: a bad argument fails fast without touching the network.
	// Create a slice to hold the JSON representation of each argument.
	// Making the slice with make ensures zero arguments are represented as []
	// instead of null when the request is encoded.
	encoded := make([]json.RawMessage, len(args)) // never nil, so it is sent as []

	// Convert each Go argument into JSON before connecting to the server.
	// This catches unsupported argument types early and identifies the
	// index of the argument that could not be encoded.
	for index, argument := range args {
		raw, err := json.Marshal(argument)
		if err != nil {
			return fmt.Errorf("encode argument %d: %w", index, err)
		}

		// Store the JSON bytes so they can be included in the RPC request.
		encoded[index] = raw
	}

	// Calculate one absolute deadline for the entire remote call.
	// This prevents connection setup, sending, and receiving from each
	// getting a separate full timeout.
	deadline := time.Now().Add(timeout)

	// Configure the dialer to stop trying to establish the connection
	// once the overall deadline is reached.
	dialer := net.Dialer{Deadline: deadline}

	// Open a TCP connection to the server using the configured address.
	// If connecting fails, return a descriptive error, including timeout
	// information when the network operation timed out.
	connection, err := dialer.Dial("tcp", c.Address)
	if err != nil {
		return describeNetError(fmt.Sprintf("connect to %s", c.Address), err, timeout)
	}

	// Ensure the connection is closed when Call returns, whether the call
	// succeeds or encounters an error.
	defer connection.Close()

	// One deadline covers the whole exchange: sending, the remote execution and reading.
	// Apply the deadline to both reads and writes on this connection.
	if err := connection.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	// Encode and send the request as one JSON value followed by a newline.
	// The request contains the method name and its JSON-encoded arguments.
	// If writing fails, return a descriptive network error.
	if err := json.NewEncoder(connection).Encode(request{Method: method, Args: encoded}); err != nil {
		return describeNetError("send request", err, timeout)
	}

	// Prepare a variable to receive the server's response.
	var reply response

	// Read and decode one JSON response from the server.
	// bufio.Reader provides buffering, while json.Decoder parses the response
	// into the reply structure.
	if err := json.NewDecoder(bufio.NewReader(connection)).Decode(&reply); err != nil {
		return describeNetError("read response", err, timeout)
	}

	// Check whether the server reported a successful remote call.
	// If the call failed, return the server's error message to the caller.
	if !reply.OK {
		// Handle an unsuccessful response that does not contain an error message.
		// This provides a useful fallback instead of returning an empty error.
		if reply.Error == "" {
			return errors.New("remote call failed")
		}

		// Return the specific error message sent by the server.
		return errors.New(reply.Error)
	}

	// If the caller does not need a result, or the response contains no result,
	// there is nothing to decode. The remote call has still completed successfully.
	if out == nil || len(reply.Value) == 0 {
		return nil
	}

	// Decode the JSON result into the caller-provided output variable.
	// The caller must pass a pointer to a value of the expected result type.
	// If the result cannot be decoded into that type, return a descriptive error.
	if err := json.Unmarshal(reply.Value, out); err != nil {
		return fmt.Errorf("decode result into %T: %w", out, err)
	}

	// The request succeeded, and the result was decoded successfully.
	return nil
}

// describeNetError labels an I/O failure and names timeouts explicitly.
func describeNetError(action string, err error, timeout time.Duration) error {

	// Declare a variable that can hold an error implementing net.Error.
	// Network errors commonly implement this interface, which provides
	// information such as whether the failure was caused by a timeout.
	var netErr net.Error

	// errors.As searches the error chain for a net.Error.
	// If one is found and it reports a timeout, return an error that clearly
	// states which operation failed and how long the timeout was.
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%s: timed out after %v: %w", action, timeout, err)
	}

	// For other network or I/O errors, identify the failed operation and
	// wrap the original error so callers can still inspect it with errors.Is
	// or errors.As.
	return fmt.Errorf("%s: %w", action, err)
}
