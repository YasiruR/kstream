package proxy

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/tryfix/log"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// RequestInfo tracks information about in-flight requests
type RequestInfo struct {
	APIKey        KafkaAPIKey
	APIVersion    int16
	CorrelationID int32
	Timestamp     time.Time
}

// ErrorInjection configures error code injection for responses
type ErrorInjection struct {
	ErrorCode KafkaErrorCode // Kafka error code to inject
	Remaining int            // Number of responses to inject (0 = unlimited)
}

// FakeResponse configures fake response generation for dropped requests.
// When a request is dropped AND a fake response rule is configured, the proxy
// will generate a fake response with the specified error code and send it back
// to the client WITHOUT forwarding the request to Kafka.
// This enables split-brain testing scenarios where the client thinks operations
// succeed but the broker never receives them.
type FakeResponse struct {
	ErrorCode KafkaErrorCode // Kafka error code to include in fake response
	Remaining int            // Number of requests to fake (0 = unlimited)
}

// KafkaProtocolProxy is a TCP proxy that understands Kafka protocol
// and can selectively drop or delay responses based on API key
type KafkaProtocolProxy struct {
	listenAddr   string
	targetAddr   string
	listener     net.Listener
	dropAPIKeys  map[KafkaAPIKey]bool           // Which API responses to drop (all)
	dropFirstN   map[KafkaAPIKey]int            // Drop first N responses then let through
	delayAPIKeys map[KafkaAPIKey]time.Duration  // Which API responses to delay
	injectErrors map[KafkaAPIKey]ErrorInjection // Inject specific error codes

	// Request manipulation
	dropReqAPIKeys      map[KafkaAPIKey]bool        // Which API requests to drop (all)
	dropReqFirstN       map[KafkaAPIKey]int         // Drop first N requests then let through
	fakeResponseOnDrop  map[KafkaAPIKey]FakeResponse // Generate fake response when dropping request

	// Track in-flight requests by correlation ID
	inFlightMu   sync.RWMutex
	inFlightReqs map[int32]RequestInfo

	// Stats
	statsMu             sync.Mutex
	requestCount        map[KafkaAPIKey]int
	droppedCount        map[KafkaAPIKey]int
	droppedRequestCount map[KafkaAPIKey]int
	injectedCount       map[KafkaAPIKey]int
	fakeResponseCount   map[KafkaAPIKey]int

	// Logging
	verboseLogging bool
	logger         log.Logger

	// Connection management
	// When true, close the client connection after dropping a response.
	// This prevents librdkafka crashes from "Invalid transaction state transition"
	// which occur when the state machine times out but the connection is still alive.
	closeOnDrop bool

	// Control
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewKafkaProtocolProxy creates a new Kafka protocol-aware proxy
func NewKafkaProtocolProxy(listenAddr, targetAddr string) *KafkaProtocolProxy {
	return &KafkaProtocolProxy{
		listenAddr:          listenAddr,
		targetAddr:          targetAddr,
		dropAPIKeys:         make(map[KafkaAPIKey]bool),
		dropFirstN:          make(map[KafkaAPIKey]int),
		delayAPIKeys:        make(map[KafkaAPIKey]time.Duration),
		injectErrors:        make(map[KafkaAPIKey]ErrorInjection),
		dropReqAPIKeys:      make(map[KafkaAPIKey]bool),
		dropReqFirstN:       make(map[KafkaAPIKey]int),
		fakeResponseOnDrop:  make(map[KafkaAPIKey]FakeResponse),
		inFlightReqs:        make(map[int32]RequestInfo),
		requestCount:        make(map[KafkaAPIKey]int),
		droppedCount:        make(map[KafkaAPIKey]int),
		droppedRequestCount: make(map[KafkaAPIKey]int),
		injectedCount:       make(map[KafkaAPIKey]int),
		fakeResponseCount:   make(map[KafkaAPIKey]int),
		stopCh:              make(chan struct{}),
	}
}

// EnableVerboseLogging enables detailed logging of all requests and responses
func (p *KafkaProtocolProxy) EnableVerboseLogging() {
	p.verboseLogging = true
}

// DisableVerboseLogging disables detailed logging
func (p *KafkaProtocolProxy) DisableVerboseLogging() {
	p.verboseLogging = false
}

// SetLogger sets the logger for the proxy
func (p *KafkaProtocolProxy) SetLogger(logger log.Logger) {
	p.logger = logger
}

// DropResponsesFor configures the proxy to drop responses for the given API key.
// If count > 0, only the first N responses are dropped, then subsequent responses are forwarded.
// If count == 0, all responses are dropped until ClearDropRules is called.
func (p *KafkaProtocolProxy) DropResponsesFor(apiKey KafkaAPIKey, count int) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	if count > 0 {
		p.dropFirstN[apiKey] = count
	} else {
		p.dropAPIKeys[apiKey] = true
	}
}

// DelayResponsesFor configures the proxy to delay responses for the given API key
func (p *KafkaProtocolProxy) DelayResponsesFor(apiKey KafkaAPIKey, delay time.Duration) {
	p.delayAPIKeys[apiKey] = delay
}

// ClearDropRules removes all response drop rules
func (p *KafkaProtocolProxy) ClearDropRules() {
	p.dropAPIKeys = make(map[KafkaAPIKey]bool)
}

// DropRequestsFor configures the proxy to drop requests for the given API key.
// If count > 0, only the first N requests are dropped, then subsequent requests are forwarded.
// If count == 0, all requests are dropped until ClearRequestDropRules is called.
// (dropped requests are not forwarded to Kafka)
func (p *KafkaProtocolProxy) DropRequestsFor(apiKey KafkaAPIKey, count int) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	if count > 0 {
		p.dropReqFirstN[apiKey] = count
	} else {
		p.dropReqAPIKeys[apiKey] = true
	}
}

// ClearRequestDropRules removes all request drop rules
func (p *KafkaProtocolProxy) ClearRequestDropRules() {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.dropReqAPIKeys = make(map[KafkaAPIKey]bool)
	p.dropReqFirstN = make(map[KafkaAPIKey]int)
}

// ClearDelayRules removes all delay rules
func (p *KafkaProtocolProxy) ClearDelayRules() {
	p.delayAPIKeys = make(map[KafkaAPIKey]time.Duration)
}

// InjectErrorFor configures the proxy to replace the error code in responses
// for the given API key. If count > 0, only the first N responses are modified.
// If count == 0, all responses are modified until ClearErrorInjection is called.
// This allows testing how the client handles specific error codes.
func (p *KafkaProtocolProxy) InjectErrorFor(apiKey KafkaAPIKey, errorCode KafkaErrorCode, count int) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.injectErrors[apiKey] = ErrorInjection{
		ErrorCode: errorCode,
		Remaining: count,
	}
}

// ClearErrorInjection removes all error injection rules
func (p *KafkaProtocolProxy) ClearErrorInjection() {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.injectErrors = make(map[KafkaAPIKey]ErrorInjection)
}

// EnableCloseOnDrop configures the proxy to close client connections after dropping
// a response. This prevents librdkafka's "Invalid transaction state transition" crash
// which occurs when its internal state machine times out but the connection remains alive.
func (p *KafkaProtocolProxy) EnableCloseOnDrop() {
	p.closeOnDrop = true
}

// DisableCloseOnDrop disables connection closing after dropping responses
func (p *KafkaProtocolProxy) DisableCloseOnDrop() {
	p.closeOnDrop = false
}

// GetRequestCount returns the number of requests received for an API key
func (p *KafkaProtocolProxy) GetRequestCount(apiKey KafkaAPIKey) int {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.requestCount[apiKey]
}

// GetDroppedCount returns the number of dropped responses for an API key
func (p *KafkaProtocolProxy) GetDroppedCount(apiKey KafkaAPIKey) int {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.droppedCount[apiKey]
}

// GetInjectedCount returns the number of error-injected responses for an API key
func (p *KafkaProtocolProxy) GetInjectedCount(apiKey KafkaAPIKey) int {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.injectedCount[apiKey]
}

// GetDroppedRequestCount returns the number of dropped requests for an API key
func (p *KafkaProtocolProxy) GetDroppedRequestCount(apiKey KafkaAPIKey) int {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.droppedRequestCount[apiKey]
}

// GetFakeResponseCount returns the number of fake responses sent for an API key
func (p *KafkaProtocolProxy) GetFakeResponseCount(apiKey KafkaAPIKey) int {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.fakeResponseCount[apiKey]
}

// ResetCounters resets all request/response counters to zero
func (p *KafkaProtocolProxy) ResetCounters() {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.requestCount = make(map[KafkaAPIKey]int)
	p.droppedCount = make(map[KafkaAPIKey]int)
	p.droppedRequestCount = make(map[KafkaAPIKey]int)
	p.injectedCount = make(map[KafkaAPIKey]int)
	p.fakeResponseCount = make(map[KafkaAPIKey]int)
}

// DropRequestAndFakeResponse configures the proxy to drop requests for the given API key
// AND send back a fake response with the specified error code to the client.
// The request is NOT forwarded to Kafka, but the client receives a response as if it was.
// This enables split-brain testing where the client thinks operations succeed but
// the broker never receives them.
//
// If count > 0, only the first N requests are handled this way.
// If count == 0, all requests are handled this way until ClearFakeResponseRules is called.
//
// Currently supported APIs: Heartbeat
func (p *KafkaProtocolProxy) DropRequestAndFakeResponse(apiKey KafkaAPIKey, errorCode KafkaErrorCode, count int) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.fakeResponseOnDrop[apiKey] = FakeResponse{
		ErrorCode: errorCode,
		Remaining: count,
	}
}

// ClearFakeResponseRules removes all fake response rules
func (p *KafkaProtocolProxy) ClearFakeResponseRules() {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.fakeResponseOnDrop = make(map[KafkaAPIKey]FakeResponse)
}

// ListenAddr returns the address the proxy is listening on
func (p *KafkaProtocolProxy) ListenAddr() string {
	if p.listener != nil {
		return p.listener.Addr().String()
	}
	return p.listenAddr
}

// Start begins accepting connections
func (p *KafkaProtocolProxy) Start() error {
	var err error
	p.listener, err = net.Listen("tcp", p.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", p.listenAddr, err)
	}

	p.running = true
	p.wg.Add(1)
	go p.acceptLoop()

	return nil
}

// Stop gracefully shuts down the proxy
func (p *KafkaProtocolProxy) Stop() error {
	if !p.running {
		return nil
	}

	p.running = false
	close(p.stopCh)

	if p.listener != nil {
		p.listener.Close()
	}

	p.wg.Wait()
	return nil
}

func (p *KafkaProtocolProxy) acceptLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		default:
		}

		// Set accept deadline so we can check stopCh periodically
		if tcpListener, ok := p.listener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
		}

		conn, err := p.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if !p.running {
				return
			}
			continue
		}

		p.wg.Add(1)
		go p.handleConnection(conn)
	}
}

func (p *KafkaProtocolProxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()
	defer clientConn.Close()

	// Connect to Kafka
	kafkaConn, err := net.DialTimeout("tcp", p.targetAddr, 10*time.Second)
	if err != nil {
		p.logger.Printf("proxy: Failed to connect to Kafka at %s: %v", p.targetAddr, err)
		return
	}
	defer kafkaConn.Close()

	// Create channels for coordination
	done := make(chan struct{})

	// Forward requests from client to Kafka (and track them)
	go func() {
		p.forwardRequests(clientConn, kafkaConn)
		close(done)
	}()

	// Forward responses from Kafka to client (with filtering)
	p.forwardResponses(kafkaConn, clientConn, done)
}

func (p *KafkaProtocolProxy) forwardRequests(client, kafka net.Conn) {
	buf := make([]byte, 64*1024)

	for {
		// Check if proxy is stopping
		if !p.running {
			return
		}

		// Set read deadline to allow periodic stop checks
		client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		// Read the message length (4 bytes)
		_, err := io.ReadFull(client, buf[:4])
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Timeout is expected, check stop condition and retry
			}
			return
		}

		msgLen := int(binary.BigEndian.Uint32(buf[:4]))
		if msgLen <= 0 || msgLen > len(buf)-4 {
			return
		}

		// Read the rest of the message with a longer deadline
		client.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(client, buf[4:4+msgLen])
		if err != nil {
			return
		}

		// Parse the request header
		// Format: api_key (2) + api_version (2) + correlation_id (4) + client_id (variable)
		if msgLen >= 8 {
			apiKey := KafkaAPIKey(binary.BigEndian.Uint16(buf[4:6]))
			apiVersion := binary.BigEndian.Uint16(buf[6:8])
			correlationID := int32(binary.BigEndian.Uint32(buf[8:12]))

			// Track this request
			p.inFlightMu.Lock()
			p.inFlightReqs[correlationID] = RequestInfo{
				APIKey:        apiKey,
				APIVersion:    int16(apiVersion),
				CorrelationID: correlationID,
				Timestamp:     time.Now(),
			}
			p.inFlightMu.Unlock()

			// Increment request count
			p.statsMu.Lock()
			p.requestCount[apiKey]++
			p.statsMu.Unlock()

			if p.verboseLogging {
				p.logger.Printf("[PROXY] ──► REQUEST  api=%-20s version=%d corr_id=%-4d size=%d bytes",
					apiKey.String(), apiVersion, correlationID, msgLen)
				// Parse and log content for key APIs
				content := parseRequestContent(apiKey, apiVersion, buf[4:4+msgLen])
				if content != "" {
					p.logger.Printf("%s", content)
				}
				// Show hex dump for key transaction APIs
				if apiKey == APIKeyEndTxn || apiKey == APIKeyInitProducerId || apiKey == APIKeyAddPartitionsToTxn || apiKey == APIKeyProduce {
					p.logger.Printf("  [RAW HEX] %s", hex.EncodeToString(buf[4:4+msgLen]))
				}
			} else if apiKey == APIKeyEndTxn {
				p.logger.Printf("proxy: Intercepted EndTxn request (correlation_id=%d)", correlationID)
			}

			// Check if we should drop this request AND send a fake response (split-brain testing)
			p.statsMu.Lock()
			if fakeResp, ok := p.fakeResponseOnDrop[apiKey]; ok {
				shouldFake := fakeResp.Remaining == 0 || fakeResp.Remaining > 0
				if shouldFake && fakeResp.Remaining > 0 {
					// Decrement remaining count
					fakeResp.Remaining--
					if fakeResp.Remaining == 0 {
						delete(p.fakeResponseOnDrop, apiKey)
					} else {
						p.fakeResponseOnDrop[apiKey] = fakeResp
					}
				}

				if shouldFake {
					p.droppedRequestCount[apiKey]++
					p.fakeResponseCount[apiKey]++
					p.statsMu.Unlock()

					// Generate and send fake response based on API type
					var fakeResponse []byte
					var err error

					switch apiKey {
					case APIKeyHeartbeat:
						fakeResponse, err = createFakeHeartbeatResponse(correlationID, int16(apiVersion), fakeResp.ErrorCode.Int16())
					default:
						err = fmt.Errorf("fake response not supported for API %s", apiKey.String())
					}

					if err != nil {
						p.logger.Printf("proxy: Failed to create fake response for %s: %v", apiKey.String(), err)
					} else {
						// Send fake response directly to client
						_, writeErr := client.Write(fakeResponse)
						if writeErr != nil {
							p.logger.Printf("proxy: Failed to send fake response to client: %v", writeErr)
							return
						}

						// Remove from in-flight tracking since we're responding directly
						p.inFlightMu.Lock()
						delete(p.inFlightReqs, correlationID)
						p.inFlightMu.Unlock()

						if p.verboseLogging {
							p.logger.Printf("[PROXY] ──► REQUEST  api=%-20s version=%d corr_id=%-4d size=%d bytes ACTION=DROP_AND_FAKE_RESPONSE error=%s",
								apiKey.String(), apiVersion, correlationID, msgLen, fakeResp.ErrorCode.String())
						} else {
							p.logger.Printf("proxy: SPLIT-BRAIN: Dropping %s request and sending fake %s response (correlation_id=%d)",
								apiKey.String(), fakeResp.ErrorCode.String(), correlationID)
						}
					}
					continue // Don't forward this request
				}
			}

			// Check if we should drop this request (all requests for this API key)
			if p.dropReqAPIKeys[apiKey] {
				p.droppedRequestCount[apiKey]++
				p.statsMu.Unlock()

				if p.verboseLogging {
					p.logger.Printf("[PROXY] ──► REQUEST  api=%-20s version=%d corr_id=%-4d size=%d bytes ACTION=DROPPED",
						apiKey.String(), apiVersion, correlationID, msgLen)
				} else {
					p.logger.Printf("proxy: DROPPING %s request (correlation_id=%d)",
						apiKey.String(), correlationID)
				}
				continue // Don't forward this request
			}

			// Check if we should drop only the first N requests
			if remaining, ok := p.dropReqFirstN[apiKey]; ok && remaining > 0 {
				p.dropReqFirstN[apiKey] = remaining - 1
				p.droppedRequestCount[apiKey]++
				p.statsMu.Unlock()

				if p.verboseLogging {
					p.logger.Printf("[PROXY] ──► REQUEST  api=%-20s version=%d corr_id=%-4d size=%d bytes ACTION=DROPPED (%d more)",
						apiKey.String(), apiVersion, correlationID, msgLen, remaining-1)
				} else {
					p.logger.Printf("proxy: DROPPING %s request (correlation_id=%d) [%d more to drop]",
						apiKey.String(), correlationID, remaining-1)
				}
				continue // Don't forward this request
			}
			p.statsMu.Unlock()
		}

		// Forward to Kafka
		_, err = kafka.Write(buf[:4+msgLen])
		if err != nil {
			return
		}
	}
}

func (p *KafkaProtocolProxy) forwardResponses(kafka, client net.Conn, done <-chan struct{}) {
	buf := make([]byte, 64*1024)

	for {
		// Check if proxy is stopping
		if !p.running {
			return
		}

		select {
		case <-done:
			return
		default:
		}

		// Set read deadline
		kafka.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		// Read the message length (4 bytes)
		_, err := io.ReadFull(kafka, buf[:4])
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}

		msgLen := int(binary.BigEndian.Uint32(buf[:4]))
		if msgLen <= 0 || msgLen > len(buf)-4 {
			return
		}

		// Read the rest of the message
		kafka.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(kafka, buf[4:4+msgLen])
		if err != nil {
			return
		}

		// Parse the response header
		// Format: correlation_id (4) + ...
		if msgLen >= 4 {
			correlationID := int32(binary.BigEndian.Uint32(buf[4:8]))

			// Look up the original request
			p.inFlightMu.RLock()
			reqInfo, found := p.inFlightReqs[correlationID]
			p.inFlightMu.RUnlock()

			if found {
				latency := time.Since(reqInfo.Timestamp)

				// Remove from tracking
				p.inFlightMu.Lock()
				delete(p.inFlightReqs, correlationID)
				p.inFlightMu.Unlock()

				// Check if we should drop this response (all responses for this API key)
				if p.dropAPIKeys[reqInfo.APIKey] {
					p.statsMu.Lock()
					p.droppedCount[reqInfo.APIKey]++
					p.statsMu.Unlock()

					if p.verboseLogging {
						p.logger.Printf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=DROPPED",
							reqInfo.APIKey.String(), correlationID, msgLen, latency)
						// Parse and log content for key APIs
						content := parseResponseContent(reqInfo.APIKey, buf[4:4+msgLen])
						if content != "" {
							p.logger.Printf("%s", content)
						}
						// Show hex dump for key transaction APIs
						if reqInfo.APIKey == APIKeyEndTxn || reqInfo.APIKey == APIKeyInitProducerId {
							p.logger.Printf("  [RAW HEX] %s", hex.EncodeToString(buf[4:4+msgLen]))
						}
					} else {
						p.logger.Printf("proxy: DROPPING %s response (correlation_id=%d)",
							reqInfo.APIKey.String(), correlationID)
					}
					// If closeOnDrop is enabled, terminate the connection to prevent
					// librdkafka's "Invalid transaction state transition" crash
					if p.closeOnDrop {
						p.logger.Printf("proxy: Closing connection after dropping response")
						return
					}
					continue // Don't forward this response
				}

				// Check if we should drop only the first N responses
				p.statsMu.Lock()
				if remaining, ok := p.dropFirstN[reqInfo.APIKey]; ok && remaining > 0 {
					p.dropFirstN[reqInfo.APIKey] = remaining - 1
					p.droppedCount[reqInfo.APIKey]++
					p.statsMu.Unlock()

					if p.verboseLogging {
						p.logger.Printf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=DROPPED (%d more)",
							reqInfo.APIKey.String(), correlationID, msgLen, latency, remaining-1)
						// Parse and log content for key APIs
						content := parseResponseContent(reqInfo.APIKey, buf[4:4+msgLen])
						if content != "" {
							p.logger.Printf("%s", content)
						}
						// Show hex dump for key transaction APIs
						if reqInfo.APIKey == APIKeyEndTxn || reqInfo.APIKey == APIKeyInitProducerId {
							p.logger.Printf("  [RAW HEX] %s", hex.EncodeToString(buf[4:4+msgLen]))
						}
					} else {
						p.logger.Printf("proxy: DROPPING %s response (correlation_id=%d) [%d more to drop]",
							reqInfo.APIKey.String(), correlationID, remaining-1)
					}
					// If closeOnDrop is enabled, terminate the connection to prevent
					// librdkafka's "Invalid transaction state transition" crash
					if p.closeOnDrop {
						p.logger.Printf("proxy: Closing connection after dropping response")
						return
					}
					continue // Don't forward this response
				}
				p.statsMu.Unlock()

				// Check if we should delay this response
				if delay, ok := p.delayAPIKeys[reqInfo.APIKey]; ok {
					if p.verboseLogging {
						p.logger.Printf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=DELAYING %v",
							reqInfo.APIKey.String(), correlationID, msgLen, latency, delay)
						// Parse and log content for key APIs
						content := parseResponseContent(reqInfo.APIKey, buf[4:4+msgLen])
						if content != "" {
							p.logger.Printf("%s", content)
						}
					} else {
						p.logger.Printf("proxy: DELAYING %s response by %v (correlation_id=%d)",
							reqInfo.APIKey.String(), delay, correlationID)
					}
					time.Sleep(delay)
				}

				// Check if we should inject an error code
				p.statsMu.Lock()
				if injection, ok := p.injectErrors[reqInfo.APIKey]; ok {
					shouldInject := injection.Remaining == 0 || injection.Remaining > 0
					if shouldInject && injection.Remaining > 0 {
						// Decrement remaining count
						injection.Remaining--
						if injection.Remaining == 0 {
							delete(p.injectErrors, reqInfo.APIKey)
						} else {
							p.injectErrors[reqInfo.APIKey] = injection
						}
					}
					p.statsMu.Unlock()

					if shouldInject {
						var modifiedBuf []byte
						var originalError int16
						var err error

						switch reqInfo.APIKey {
						case APIKeyEndTxn:
							// Use kmsg to properly decode/encode EndTxn response
							// This handles flexible protocol versions correctly
							modifiedBuf, originalError, err = injectEndTxnError(buf[4:4+msgLen], reqInfo.APIVersion, injection.ErrorCode.Int16())
						case APIKeyAddPartitionsToTxn:
							// Use kmsg to properly decode/encode AddPartitionsToTxn response
							// Error codes are nested inside partition arrays, not at top level
							modifiedBuf, originalError, err = injectAddPartitionsToTxnError(buf[4:4+msgLen], reqInfo.APIVersion, injection.ErrorCode.Int16())
						case APIKeyProduce:
							// Use kmsg to properly decode/encode Produce response
							// Error codes are nested inside topic/partition arrays, not at top level
							modifiedBuf, originalError, err = injectProduceError(buf[4:4+msgLen], reqInfo.APIVersion, injection.ErrorCode.Int16())
						case APIKeyTxnOffsetCommit:
							// Use kmsg to properly decode/encode TxnOffsetCommit response
							// Error codes are nested inside topic/partition arrays, not at top level
							modifiedBuf, originalError, err = injectTxnOffsetCommitError(buf[4:4+msgLen], reqInfo.APIVersion, injection.ErrorCode.Int16())
						// Consumer group APIs
						case APIKeyJoinGroup:
							// Use kmsg to properly decode/encode JoinGroup response
							// JoinGroup has a top-level error code
							modifiedBuf, originalError, err = injectJoinGroupError(buf[4:4+msgLen], reqInfo.APIVersion, injection.ErrorCode.Int16())
						case APIKeySyncGroup:
							// Use kmsg to properly decode/encode SyncGroup response
							// SyncGroup has a top-level error code
							modifiedBuf, originalError, err = injectSyncGroupError(buf[4:4+msgLen], reqInfo.APIVersion, injection.ErrorCode.Int16())
						case APIKeyHeartbeat:
							// Use kmsg to properly decode/encode Heartbeat response
							// Heartbeat has a top-level error code
							modifiedBuf, originalError, err = injectHeartbeatError(buf[4:4+msgLen], reqInfo.APIVersion, injection.ErrorCode.Int16())
						default:
							// Use generic byte-level manipulation for other APIs
							modifiedBuf, originalError, err = injectGenericError(buf[4:4+msgLen], reqInfo.APIKey, reqInfo.APIVersion, injection.ErrorCode.Int16())
						}

						if err != nil {
							p.logger.Printf("proxy: Failed to inject error into %s: %v", reqInfo.APIKey.String(), err)
						} else {
							// Update the buffer with modified response
							// Write new length
							newLen := len(modifiedBuf)
							binary.BigEndian.PutUint32(buf[0:4], uint32(newLen))
							// Copy modified response
							copy(buf[4:], modifiedBuf)
							msgLen = newLen

							// Track injected count
							p.statsMu.Lock()
							p.injectedCount[reqInfo.APIKey]++
							p.statsMu.Unlock()

							if p.verboseLogging {
								p.logger.Printf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=INJECTED_ERROR %d->%s",
									reqInfo.APIKey.String(), correlationID, msgLen, latency,
									originalError, injection.ErrorCode.String())
							} else {
								p.logger.Printf("proxy: INJECTING ERROR %d (%s) into %s response (was %d, correlation_id=%d)",
									injection.ErrorCode.Int16(), injection.ErrorCode.String(),
									reqInfo.APIKey.String(), originalError, correlationID)
							}
						}
					}
				} else {
					p.statsMu.Unlock()
				}

				// Log forwarded response
				if p.verboseLogging {
					p.logger.Printf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=FORWARDED",
						reqInfo.APIKey.String(), correlationID, msgLen, latency)
					// Parse and log content for key APIs
					content := parseResponseContent(reqInfo.APIKey, buf[4:4+msgLen])
					if content != "" {
						p.logger.Printf("%s", content)
					}
					// Show hex dump for key APIs including Fetch and ListOffsets for debugging
					if reqInfo.APIKey == APIKeyEndTxn || reqInfo.APIKey == APIKeyInitProducerId || reqInfo.APIKey == APIKeyFetch || reqInfo.APIKey == APIKeyListOffsets || reqInfo.APIKey == APIKeyProduce {
						p.logger.Printf("  [RAW HEX] %s", hex.EncodeToString(buf[4:4+msgLen]))
					}
				}
			} else if p.verboseLogging {
				// Response for unknown request
				p.logger.Printf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes (no matching request)",
					"UNKNOWN", correlationID, msgLen)
			}
		}

		// Forward to client
		_, err = client.Write(buf[:4+msgLen])
		if err != nil {
			return
		}
	}
}

// parseRequestContent parses and returns a human-readable description of the request content
func parseRequestContent(apiKey KafkaAPIKey, apiVersion uint16, data []byte) string {
	// data starts after the 4-byte length field
	// Request header: api_key(2) + api_version(2) + correlation_id(4) + client_id(variable)
	if len(data) < 12 {
		return ""
	}

	// Skip api_key(2), api_version(2), correlation_id(4)
	offset := 8

	// Parse client_id (nullable string in v1+, regular string otherwise)
	clientID, newOffset := parseString(data, offset)
	offset = newOffset
	if offset < 0 {
		return fmt.Sprintf("  client_id=%s", clientID)
	}

	switch apiKey {
	case APIKeyEndTxn:
		return parseEndTxnRequest(data, offset, clientID)
	case APIKeyInitProducerId:
		return parseInitProducerIdRequest(data, offset, clientID)
	case APIKeyProduce:
		return parseProduceRequest(data, offset, clientID)
	case APIKeyAddPartitionsToTxn:
		return parseAddPartitionsRequest(data, offset, clientID)
	default:
		return fmt.Sprintf("  client_id=%s", clientID)
	}
}

// parseResponseContent parses and returns a human-readable description of the response content
func parseResponseContent(apiKey KafkaAPIKey, data []byte) string {
	// data starts after the 4-byte length field
	// Response header: correlation_id(4) + ...
	if len(data) < 4 {
		return ""
	}

	offset := 4 // skip correlation_id

	switch apiKey {
	case APIKeyEndTxn:
		return parseEndTxnResponse(data, offset)
	case APIKeyInitProducerId:
		return parseInitProducerIdResponse(data, offset)
	case APIKeyProduce:
		return parseProduceResponse(data, offset)
	default:
		return ""
	}
}

// parseEndTxnRequest parses EndTxn request content
// Format: transactional_id (STRING) + producer_id (INT64) + producer_epoch (INT16) + committed (BOOL)
func parseEndTxnRequest(data []byte, offset int, clientID string) string {
	if offset+2 > len(data) {
		return fmt.Sprintf("  client_id=%s [truncated]", clientID)
	}

	txnID, newOffset := parseString(data, offset)
	offset = newOffset
	if offset < 0 || offset+11 > len(data) {
		return fmt.Sprintf("  client_id=%s txn_id=%s [truncated]", clientID, txnID)
	}

	producerID := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	producerEpoch := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	committed := data[offset] != 0

	action := "ABORT"
	if committed {
		action = "COMMIT"
	}

	return fmt.Sprintf("  client_id=%s txn_id=%s producer_id=%d epoch=%d action=%s",
		clientID, txnID, producerID, producerEpoch, action)
}

// parseEndTxnResponse parses EndTxn response content
// Format: throttle_time_ms (INT32) + error_code (INT16)
func parseEndTxnResponse(data []byte, offset int) string {
	if offset+6 > len(data) {
		return "  [truncated]"
	}

	throttleTime := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	errorCode := int16(binary.BigEndian.Uint16(data[offset : offset+2]))

	errorName := kafkaErrorName(errorCode)
	return fmt.Sprintf("  throttle=%dms error_code=%d (%s)", throttleTime, errorCode, errorName)
}

// parseInitProducerIdRequest parses InitProducerId request content
func parseInitProducerIdRequest(data []byte, offset int, clientID string) string {
	if offset+2 > len(data) {
		return fmt.Sprintf("  client_id=%s [truncated]", clientID)
	}

	txnID, newOffset := parseNullableString(data, offset)
	offset = newOffset
	if offset < 0 || offset+4 > len(data) {
		return fmt.Sprintf("  client_id=%s txn_id=%s [truncated]", clientID, txnID)
	}

	txnTimeout := int32(binary.BigEndian.Uint32(data[offset : offset+4]))

	return fmt.Sprintf("  client_id=%s txn_id=%s txn_timeout=%dms", clientID, txnID, txnTimeout)
}

// parseInitProducerIdResponse parses InitProducerId response content
func parseInitProducerIdResponse(data []byte, offset int) string {
	if offset+14 > len(data) {
		return "  [truncated]"
	}

	throttleTime := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	errorCode := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	producerID := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	producerEpoch := int16(binary.BigEndian.Uint16(data[offset : offset+2]))

	errorName := kafkaErrorName(errorCode)
	return fmt.Sprintf("  throttle=%dms error_code=%d (%s) producer_id=%d epoch=%d",
		throttleTime, errorCode, errorName, producerID, producerEpoch)
}

// parseProduceRequest parses Produce request content (simplified - just shows topic and partition count)
func parseProduceRequest(data []byte, offset int, clientID string) string {
	if offset+6 > len(data) {
		return fmt.Sprintf("  client_id=%s [truncated]", clientID)
	}

	// Skip transactional_id for transactional produces
	txnID, newOffset := parseNullableString(data, offset)
	offset = newOffset
	if offset < 0 || offset+6 > len(data) {
		return fmt.Sprintf("  client_id=%s txn_id=%s [truncated]", clientID, txnID)
	}

	acks := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	timeout := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+4 > len(data) {
		return fmt.Sprintf("  client_id=%s txn_id=%s acks=%d timeout=%dms", clientID, txnID, acks, timeout)
	}

	topicCount := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	topics := []string{}
	for i := int32(0); i < topicCount && offset < len(data); i++ {
		topicName, newOffset := parseString(data, offset)
		offset = newOffset
		if offset < 0 {
			break
		}
		if offset+4 > len(data) {
			break
		}
		partitionCount := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		topics = append(topics, fmt.Sprintf("%s[%d partitions]", topicName, partitionCount))

		// Skip partition data
		for j := int32(0); j < partitionCount && offset < len(data); j++ {
			if offset+4 > len(data) {
				break
			}
			offset += 4 // partition index
			if offset+4 > len(data) {
				break
			}
			recordSize := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			offset += int(recordSize) // skip record batch
		}
	}

	if len(topics) > 0 {
		return fmt.Sprintf("  client_id=%s txn_id=%s acks=%d timeout=%dms topics=%v",
			clientID, txnID, acks, timeout, topics)
	}
	return fmt.Sprintf("  client_id=%s txn_id=%s acks=%d timeout=%dms topic_count=%d",
		clientID, txnID, acks, timeout, topicCount)
}

// parseProduceResponse parses Produce response content (simplified)
func parseProduceResponse(data []byte, offset int) string {
	if offset+4 > len(data) {
		return "  [truncated]"
	}

	topicCount := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	results := []string{}
	for i := int32(0); i < topicCount && offset < len(data); i++ {
		topicName, newOffset := parseString(data, offset)
		offset = newOffset
		if offset < 0 || offset+4 > len(data) {
			break
		}
		partitionCount := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		for j := int32(0); j < partitionCount && offset+18 <= len(data); j++ {
			partition := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			errorCode := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
			offset += 2
			baseOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
			offset += 8
			// Skip remaining fields
			offset += 8 // log_append_time
			offset += 4 // log_start_offset (if version >= 5)

			errorName := kafkaErrorName(errorCode)
			results = append(results, fmt.Sprintf("%s[%d]:offset=%d,err=%s", topicName, partition, baseOffset, errorName))
		}
	}

	if len(results) > 0 {
		return fmt.Sprintf("  results=%v", results)
	}
	return fmt.Sprintf("  topic_count=%d", topicCount)
}

// parseAddPartitionsRequest parses AddPartitionsToTxn request content
func parseAddPartitionsRequest(data []byte, offset int, clientID string) string {
	if offset+2 > len(data) {
		return fmt.Sprintf("  client_id=%s [truncated]", clientID)
	}

	txnID, newOffset := parseString(data, offset)
	offset = newOffset
	if offset < 0 || offset+10 > len(data) {
		return fmt.Sprintf("  client_id=%s txn_id=%s [truncated]", clientID, txnID)
	}

	producerID := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	producerEpoch := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if offset+4 > len(data) {
		return fmt.Sprintf("  client_id=%s txn_id=%s producer_id=%d epoch=%d",
			clientID, txnID, producerID, producerEpoch)
	}

	topicCount := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	topics := []string{}
	for i := int32(0); i < topicCount && offset < len(data); i++ {
		topicName, newOffset := parseString(data, offset)
		offset = newOffset
		if offset < 0 || offset+4 > len(data) {
			break
		}
		partitionCount := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		topics = append(topics, fmt.Sprintf("%s[%d]", topicName, partitionCount))
		offset += int(partitionCount) * 4 // skip partition indices
	}

	return fmt.Sprintf("  client_id=%s txn_id=%s producer_id=%d epoch=%d topics=%v",
		clientID, txnID, producerID, producerEpoch, topics)
}

// parseString parses a Kafka STRING (length-prefixed)
func parseString(data []byte, offset int) (string, int) {
	if offset+2 > len(data) {
		return "", -1
	}
	length := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if length < 0 {
		return "<null>", offset
	}
	if offset+int(length) > len(data) {
		return "<truncated>", -1
	}
	str := string(data[offset : offset+int(length)])
	return str, offset + int(length)
}

// parseNullableString parses a Kafka NULLABLE_STRING
func parseNullableString(data []byte, offset int) (string, int) {
	if offset+2 > len(data) {
		return "", -1
	}
	length := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if length < 0 {
		return "<null>", offset
	}
	if offset+int(length) > len(data) {
		return "<truncated>", -1
	}
	str := string(data[offset : offset+int(length)])
	return str, offset + int(length)
}

// kafkaErrorName returns a human-readable name for Kafka error codes
// This is a convenience function that wraps KafkaErrorCode.String()
func kafkaErrorName(code int16) string {
	return KafkaErrorCode(code).String()
}

// getFlexibleVersion returns the minimum version at which the API uses flexible encoding
// Returns -1 if the API never uses flexible encoding (or is unknown)
func getFlexibleVersion(apiKey KafkaAPIKey) int16 {
	// Based on Kafka protocol specification
	// https://kafka.apache.org/protocol#protocol_api_keys
	switch apiKey {
	case APIKeyFindCoordinator:
		return 3
	case APIKeyInitProducerId:
		return 2
	case APIKeyEndTxn:
		return 3
	case APIKeyAddPartitionsToTxn:
		return 3
	case APIKeyAddOffsetsToTxn:
		return 3
	case APIKeyTxnOffsetCommit:
		return 3
	case APIKeyProduce:
		return 9
	case APIKeyMetadata:
		return 9
	// Consumer group APIs
	case APIKeyJoinGroup:
		return 6
	case APIKeySyncGroup:
		return 4
	case APIKeyHeartbeat:
		return 4
	case APIKeyLeaveGroup:
		return 4
	default:
		return -1 // Unknown, assume non-flexible
	}
}

// injectGenericError injects an error code into a Kafka response using byte-level manipulation.
// This works for responses that follow the common pattern:
//   - Response header: correlation_id (4 bytes) [+ tagged_fields for flexible versions]
//   - Response body: throttle_time_ms (4 bytes) + error_code (2 bytes) + ...
//
// Returns the modified response bytes, the original error code, and any error.
func injectGenericError(data []byte, apiKey KafkaAPIKey, apiVersion int16, newErrorCode int16) ([]byte, int16, error) {
	if len(data) < 10 {
		return nil, 0, fmt.Errorf("response too short: %d bytes", len(data))
	}

	// Determine if this is a flexible version
	flexibleMinVersion := getFlexibleVersion(apiKey)
	isFlexible := flexibleMinVersion >= 0 && apiVersion >= flexibleMinVersion

	// Calculate the offset to the error_code field
	// Response structure:
	// - correlation_id: 4 bytes
	// - [flexible only] tagged_fields: variable, but usually 1 byte (0x00 for empty)
	// - throttle_time_ms: 4 bytes
	// - error_code: 2 bytes

	var errorCodeOffset int
	if isFlexible {
		// Flexible version: skip correlation_id (4) + tagged_fields (assume 1 byte) + throttle_time (4)
		// Tagged fields start with a varint count; if 0x00, it means no tagged fields
		taggedFieldsLen := 1 // Assume single byte (0x00 = no tagged fields)
		if len(data) > 4 && data[4] != 0x00 {
			// Non-empty tagged fields - for now, we'll try to parse the varint
			// But for simplicity, assume it's just 1 byte
			taggedFieldsLen = 1
		}
		errorCodeOffset = 4 + taggedFieldsLen + 4 // correlation_id + tagged_fields + throttle_time
	} else {
		// Non-flexible version: skip correlation_id (4) + throttle_time (4)
		errorCodeOffset = 4 + 4
	}

	if errorCodeOffset+2 > len(data) {
		return nil, 0, fmt.Errorf("response too short for error code at offset %d: %d bytes", errorCodeOffset, len(data))
	}

	// Read original error code
	originalError := int16(binary.BigEndian.Uint16(data[errorCodeOffset : errorCodeOffset+2]))

	// Create a copy of the data and modify the error code
	result := make([]byte, len(data))
	copy(result, data)
	binary.BigEndian.PutUint16(result[errorCodeOffset:errorCodeOffset+2], uint16(newErrorCode))

	return result, originalError, nil
}

// injectEndTxnError decodes an EndTxn response using kmsg, modifies the error code,
// and re-encodes it. This properly handles all protocol versions including flexible versions.
// Returns the modified response bytes, the original error code, and any error.
func injectEndTxnError(data []byte, apiVersion int16, newErrorCode int16) ([]byte, int16, error) {
	// The data includes the full response (correlation_id + response body)
	// We need to:
	// 1. Extract correlation_id (first 4 bytes)
	// 2. Decode the response body with kmsg
	// 3. Modify the error code
	// 4. Re-encode with the same version

	if len(data) < 4 {
		return nil, 0, fmt.Errorf("response too short")
	}

	correlationID := binary.BigEndian.Uint32(data[0:4])

	// For flexible versions (v3+), there's a TAG_BUFFER after correlation_id
	// kmsg handles this internally, but we need to skip the response header
	var headerSize int
	if apiVersion >= 3 {
		// Flexible version: correlation_id (4) + empty tagged fields (1 byte = 0x00)
		headerSize = 5
		// But the TAG_BUFFER could be longer, so we need to parse it
		// For simplicity, assume empty tagged fields (0x00)
		if len(data) > 4 && data[4] == 0x00 {
			headerSize = 5
		} else {
			// Non-empty tagged fields, this is more complex
			// For now, just use the simple case
			headerSize = 5
		}
	} else {
		// Non-flexible version: just correlation_id
		headerSize = 4
	}

	if len(data) < headerSize {
		return nil, 0, fmt.Errorf("response too short for header")
	}

	// Decode the response body using kmsg
	var resp kmsg.EndTxnResponse
	resp.SetVersion(apiVersion)

	// ReadFrom reads from a byte slice
	if err := resp.ReadFrom(data[headerSize:]); err != nil {
		return nil, 0, fmt.Errorf("failed to decode EndTxn response: %w", err)
	}

	originalError := resp.ErrorCode

	// Modify the error code
	resp.ErrorCode = newErrorCode

	// Re-encode the response
	encoded := resp.AppendTo(nil)

	// Rebuild the full response with header
	result := make([]byte, headerSize+len(encoded))
	binary.BigEndian.PutUint32(result[0:4], correlationID)
	if apiVersion >= 3 {
		result[4] = 0x00 // Empty tagged fields
	}
	copy(result[headerSize:], encoded)

	return result, originalError, nil
}

// injectAddPartitionsToTxnError decodes an AddPartitionsToTxn response using kmsg,
// modifies all partition error codes, and re-encodes it.
// This properly handles all protocol versions including flexible versions.
// Returns the modified response bytes, the original first partition error code, and any error.
func injectAddPartitionsToTxnError(data []byte, apiVersion int16, newErrorCode int16) ([]byte, int16, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("response too short")
	}

	correlationID := binary.BigEndian.Uint32(data[0:4])

	// Determine header size based on version
	// AddPartitionsToTxn is flexible from v3+
	var headerSize int
	if apiVersion >= 3 {
		headerSize = 5 // correlation_id (4) + empty tagged fields (1)
		if len(data) > 4 && data[4] != 0x00 {
			headerSize = 5 // Assume simple case
		}
	} else {
		headerSize = 4 // Just correlation_id
	}

	if len(data) < headerSize {
		return nil, 0, fmt.Errorf("response too short for header")
	}

	// Decode the response body using kmsg
	var resp kmsg.AddPartitionsToTxnResponse
	resp.SetVersion(apiVersion)

	if err := resp.ReadFrom(data[headerSize:]); err != nil {
		return nil, 0, fmt.Errorf("failed to decode AddPartitionsToTxn response: %w", err)
	}

	// Get original error code from first partition (for logging)
	var originalError int16 = 0
	if len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 {
		originalError = resp.Topics[0].Partitions[0].ErrorCode
	}

	// Inject error code into ALL partitions
	for i := range resp.Topics {
		for j := range resp.Topics[i].Partitions {
			resp.Topics[i].Partitions[j].ErrorCode = newErrorCode
		}
	}

	// Re-encode the response
	encoded := resp.AppendTo(nil)

	// Rebuild the full response with header
	resultBuf := make([]byte, headerSize+len(encoded))
	binary.BigEndian.PutUint32(resultBuf[0:4], correlationID)
	if apiVersion >= 3 {
		resultBuf[4] = 0x00 // Empty tagged fields
	}
	copy(resultBuf[headerSize:], encoded)

	return resultBuf, originalError, nil
}

// injectProduceError decodes a Produce response using kmsg, modifies all partition
// error codes, and re-encodes it. Produce responses have error codes nested inside
// topic/partition arrays, not at the top level.
func injectProduceError(data []byte, apiVersion int16, newErrorCode int16) ([]byte, int16, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("response too short")
	}

	correlationID := binary.BigEndian.Uint32(data[0:4])

	// Determine header size based on version
	// Produce is flexible from v9+
	var headerSize int
	if apiVersion >= 9 {
		headerSize = 5 // correlation_id (4) + empty tagged fields (1)
		if len(data) > 4 && data[4] != 0x00 {
			headerSize = 5 // Assume simple case
		}
	} else {
		headerSize = 4 // Just correlation_id
	}

	if len(data) < headerSize {
		return nil, 0, fmt.Errorf("response too short for header")
	}

	// Decode the response body using kmsg
	var resp kmsg.ProduceResponse
	resp.SetVersion(apiVersion)

	if err := resp.ReadFrom(data[headerSize:]); err != nil {
		return nil, 0, fmt.Errorf("failed to decode Produce response: %w", err)
	}

	// Get original error code from first partition (for logging)
	var originalError int16 = 0
	if len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 {
		originalError = resp.Topics[0].Partitions[0].ErrorCode
	}

	// Inject error code into ALL partitions
	for i := range resp.Topics {
		for j := range resp.Topics[i].Partitions {
			resp.Topics[i].Partitions[j].ErrorCode = newErrorCode
		}
	}

	// Re-encode the response
	encoded := resp.AppendTo(nil)

	// Rebuild the full response with header
	resultBuf := make([]byte, headerSize+len(encoded))
	binary.BigEndian.PutUint32(resultBuf[0:4], correlationID)
	if apiVersion >= 9 {
		resultBuf[4] = 0x00 // Empty tagged fields
	}
	copy(resultBuf[headerSize:], encoded)

	return resultBuf, originalError, nil
}

// injectTxnOffsetCommitError decodes a TxnOffsetCommit response using kmsg, modifies all
// partition error codes, and re-encodes it. TxnOffsetCommit responses have error codes
// nested inside topic/partition arrays, not at the top level.
func injectTxnOffsetCommitError(data []byte, apiVersion int16, newErrorCode int16) ([]byte, int16, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("response too short")
	}

	correlationID := binary.BigEndian.Uint32(data[0:4])

	// Determine header size based on version
	// TxnOffsetCommit is flexible from v3+
	var headerSize int
	if apiVersion >= 3 {
		headerSize = 5 // correlation_id (4) + empty tagged fields (1)
		if len(data) > 4 && data[4] != 0x00 {
			headerSize = 5 // Assume simple case
		}
	} else {
		headerSize = 4 // Just correlation_id
	}

	if len(data) < headerSize {
		return nil, 0, fmt.Errorf("response too short for header")
	}

	// Decode the response body using kmsg
	var resp kmsg.TxnOffsetCommitResponse
	resp.SetVersion(apiVersion)

	if err := resp.ReadFrom(data[headerSize:]); err != nil {
		return nil, 0, fmt.Errorf("failed to decode TxnOffsetCommit response: %w", err)
	}

	// Get original error code from first partition (for logging)
	var originalError int16 = 0
	if len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 {
		originalError = resp.Topics[0].Partitions[0].ErrorCode
	}

	// Inject error code into ALL partitions
	for i := range resp.Topics {
		for j := range resp.Topics[i].Partitions {
			resp.Topics[i].Partitions[j].ErrorCode = newErrorCode
		}
	}

	// Re-encode the response
	encoded := resp.AppendTo(nil)

	// Rebuild the full response with header
	resultBuf := make([]byte, headerSize+len(encoded))
	binary.BigEndian.PutUint32(resultBuf[0:4], correlationID)
	if apiVersion >= 3 {
		resultBuf[4] = 0x00 // Empty tagged fields
	}
	copy(resultBuf[headerSize:], encoded)

	return resultBuf, originalError, nil
}

// =============================================================================
// Consumer Group API Error Injection Functions
// =============================================================================

// injectJoinGroupError decodes a JoinGroup response using kmsg, modifies the error code,
// and re-encodes it. JoinGroup has a top-level error code.
// Returns the modified response bytes, the original error code, and any error.
func injectJoinGroupError(data []byte, apiVersion int16, newErrorCode int16) ([]byte, int16, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("response too short")
	}

	correlationID := binary.BigEndian.Uint32(data[0:4])

	// Determine header size based on version
	// JoinGroup is flexible from v6+
	var headerSize int
	if apiVersion >= 6 {
		headerSize = 5 // correlation_id (4) + empty tagged fields (1)
		if len(data) > 4 && data[4] != 0x00 {
			headerSize = 5 // Assume simple case
		}
	} else {
		headerSize = 4 // Just correlation_id
	}

	if len(data) < headerSize {
		return nil, 0, fmt.Errorf("response too short for header")
	}

	// Decode the response body using kmsg
	var resp kmsg.JoinGroupResponse
	resp.SetVersion(apiVersion)

	if err := resp.ReadFrom(data[headerSize:]); err != nil {
		return nil, 0, fmt.Errorf("failed to decode JoinGroup response: %w", err)
	}

	originalError := resp.ErrorCode

	// Modify the error code
	resp.ErrorCode = newErrorCode

	// Re-encode the response
	encoded := resp.AppendTo(nil)

	// Rebuild the full response with header
	result := make([]byte, headerSize+len(encoded))
	binary.BigEndian.PutUint32(result[0:4], correlationID)
	if apiVersion >= 6 {
		result[4] = 0x00 // Empty tagged fields
	}
	copy(result[headerSize:], encoded)

	return result, originalError, nil
}

// injectSyncGroupError decodes a SyncGroup response using kmsg, modifies the error code,
// and re-encodes it. SyncGroup has a top-level error code.
// Returns the modified response bytes, the original error code, and any error.
func injectSyncGroupError(data []byte, apiVersion int16, newErrorCode int16) ([]byte, int16, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("response too short")
	}

	correlationID := binary.BigEndian.Uint32(data[0:4])

	// Determine header size based on version
	// SyncGroup is flexible from v4+
	var headerSize int
	if apiVersion >= 4 {
		headerSize = 5 // correlation_id (4) + empty tagged fields (1)
		if len(data) > 4 && data[4] != 0x00 {
			headerSize = 5 // Assume simple case
		}
	} else {
		headerSize = 4 // Just correlation_id
	}

	if len(data) < headerSize {
		return nil, 0, fmt.Errorf("response too short for header")
	}

	// Decode the response body using kmsg
	var resp kmsg.SyncGroupResponse
	resp.SetVersion(apiVersion)

	if err := resp.ReadFrom(data[headerSize:]); err != nil {
		return nil, 0, fmt.Errorf("failed to decode SyncGroup response: %w", err)
	}

	originalError := resp.ErrorCode

	// Modify the error code
	resp.ErrorCode = newErrorCode

	// Re-encode the response
	encoded := resp.AppendTo(nil)

	// Rebuild the full response with header
	result := make([]byte, headerSize+len(encoded))
	binary.BigEndian.PutUint32(result[0:4], correlationID)
	if apiVersion >= 4 {
		result[4] = 0x00 // Empty tagged fields
	}
	copy(result[headerSize:], encoded)

	return result, originalError, nil
}

// createFakeHeartbeatResponse creates a complete Heartbeat response from scratch.
// This is used for split-brain testing where we drop the request to Kafka but send
// a fake success response to the client. The client thinks it's alive, but the broker
// never received the heartbeat.
//
// Returns the complete response bytes including:
// - Length prefix (4 bytes)
// - Correlation ID (4 bytes)
// - Tagged fields header (1 byte for flexible versions)
// - Response body (throttle_time + error_code + tagged fields)
func createFakeHeartbeatResponse(correlationID int32, apiVersion int16, errorCode int16) ([]byte, error) {
	// Create the response using kmsg
	var resp kmsg.HeartbeatResponse
	resp.SetVersion(apiVersion)
	resp.ThrottleMillis = 0
	resp.ErrorCode = errorCode

	// Encode the response body
	encoded := resp.AppendTo(nil)

	// Determine if this is a flexible version (v4+)
	isFlexible := apiVersion >= 4

	// Build the complete response with header
	var headerSize int
	if isFlexible {
		headerSize = 5 // correlation_id (4) + empty tagged fields (1)
	} else {
		headerSize = 4 // just correlation_id
	}

	// Total message size (excluding the length prefix itself)
	msgLen := headerSize + len(encoded)

	// Build complete response: length prefix + header + body
	result := make([]byte, 4+msgLen)

	// Length prefix (4 bytes, big-endian)
	binary.BigEndian.PutUint32(result[0:4], uint32(msgLen))

	// Correlation ID (4 bytes, big-endian)
	binary.BigEndian.PutUint32(result[4:8], uint32(correlationID))

	// Tagged fields header (for flexible versions)
	if isFlexible {
		result[8] = 0x00 // Empty tagged fields
	}

	// Response body
	copy(result[4+headerSize:], encoded)

	return result, nil
}

// injectHeartbeatError decodes a Heartbeat response using kmsg, modifies the error code,
// and re-encodes it. Heartbeat has a top-level error code.
// Returns the modified response bytes, the original error code, and any error.
func injectHeartbeatError(data []byte, apiVersion int16, newErrorCode int16) ([]byte, int16, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("response too short")
	}

	correlationID := binary.BigEndian.Uint32(data[0:4])

	// Determine header size based on version
	// Heartbeat is flexible from v4+
	var headerSize int
	if apiVersion >= 4 {
		headerSize = 5 // correlation_id (4) + empty tagged fields (1)
		if len(data) > 4 && data[4] != 0x00 {
			headerSize = 5 // Assume simple case
		}
	} else {
		headerSize = 4 // Just correlation_id
	}

	if len(data) < headerSize {
		return nil, 0, fmt.Errorf("response too short for header")
	}

	// Decode the response body using kmsg
	var resp kmsg.HeartbeatResponse
	resp.SetVersion(apiVersion)

	if err := resp.ReadFrom(data[headerSize:]); err != nil {
		return nil, 0, fmt.Errorf("failed to decode Heartbeat response: %w", err)
	}

	originalError := resp.ErrorCode

	// Modify the error code
	resp.ErrorCode = newErrorCode

	// Re-encode the response
	encoded := resp.AppendTo(nil)

	// Rebuild the full response with header
	result := make([]byte, headerSize+len(encoded))
	binary.BigEndian.PutUint32(result[0:4], correlationID)
	if apiVersion >= 4 {
		result[4] = 0x00 // Empty tagged fields
	}
	copy(result[headerSize:], encoded)

	return result, originalError, nil
}
