package librd

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaAPIKey represents Kafka protocol API keys
type KafkaAPIKey int16

const (
	APIKeyProduce         KafkaAPIKey = 0
	APIKeyFetch           KafkaAPIKey = 1
	APIKeyListOffsets     KafkaAPIKey = 2
	APIKeyMetadata        KafkaAPIKey = 3
	APIKeyOffsetCommit    KafkaAPIKey = 8
	APIKeyOffsetFetch     KafkaAPIKey = 9
	APIKeyFindCoordinator KafkaAPIKey = 10
	APIKeyJoinGroup       KafkaAPIKey = 11
	APIKeyHeartbeat       KafkaAPIKey = 12
	APIKeyLeaveGroup      KafkaAPIKey = 13
	APIKeySyncGroup       KafkaAPIKey = 14
	APIKeyInitProducerId  KafkaAPIKey = 22
	APIKeyAddPartitions   KafkaAPIKey = 24
	APIKeyAddOffsets      KafkaAPIKey = 25
	APIKeyEndTxn          KafkaAPIKey = 26 // This is the one we want to intercept
	APIKeyTxnOffsetCommit KafkaAPIKey = 28
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
	ErrorCode int16 // Kafka error code to inject
	Remaining int   // Number of responses to inject (0 = unlimited)
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

	// Track in-flight requests by correlation ID
	inFlightMu   sync.RWMutex
	inFlightReqs map[int32]RequestInfo

	// Stats
	statsMu      sync.Mutex
	droppedCount map[KafkaAPIKey]int

	// Logging
	verboseLogging bool

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
		listenAddr:   listenAddr,
		targetAddr:   targetAddr,
		dropAPIKeys:  make(map[KafkaAPIKey]bool),
		dropFirstN:   make(map[KafkaAPIKey]int),
		delayAPIKeys: make(map[KafkaAPIKey]time.Duration),
		injectErrors: make(map[KafkaAPIKey]ErrorInjection),
		inFlightReqs: make(map[int32]RequestInfo),
		droppedCount: make(map[KafkaAPIKey]int),
		stopCh:       make(chan struct{}),
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

// DropFirstNResponsesFor configures the proxy to drop only the first N responses
// for the given API key, then let subsequent responses through
func (p *KafkaProtocolProxy) DropFirstNResponsesFor(apiKey KafkaAPIKey, n int) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	p.dropFirstN[apiKey] = n
}

// DropResponsesFor configures the proxy to drop responses for the given API key
func (p *KafkaProtocolProxy) DropResponsesFor(apiKey KafkaAPIKey) {
	p.dropAPIKeys[apiKey] = true
}

// DelayResponsesFor configures the proxy to delay responses for the given API key
func (p *KafkaProtocolProxy) DelayResponsesFor(apiKey KafkaAPIKey, delay time.Duration) {
	p.delayAPIKeys[apiKey] = delay
}

// ClearDropRules removes all drop rules
func (p *KafkaProtocolProxy) ClearDropRules() {
	p.dropAPIKeys = make(map[KafkaAPIKey]bool)
}

// ClearDelayRules removes all delay rules
func (p *KafkaProtocolProxy) ClearDelayRules() {
	p.delayAPIKeys = make(map[KafkaAPIKey]time.Duration)
}

// InjectErrorFor configures the proxy to replace the error code in responses
// for the given API key. If count > 0, only the first N responses are modified.
// If count == 0, all responses are modified until ClearErrorInjection is called.
// This allows testing how the client handles specific error codes.
func (p *KafkaProtocolProxy) InjectErrorFor(apiKey KafkaAPIKey, errorCode int16, count int) {
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

// GetDroppedCount returns the number of dropped responses for an API key
func (p *KafkaProtocolProxy) GetDroppedCount(apiKey KafkaAPIKey) int {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return p.droppedCount[apiKey]
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
		fmt.Println(fmt.Sprintf("proxy: Failed to connect to Kafka at %s: %v\t", p.targetAddr, err))
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

			if p.verboseLogging {
				fmt.Println(fmt.Sprintf("[PROXY] ──► REQUEST  api=%-20s version=%d corr_id=%-4d size=%d bytes\t",
					apiKeyName(apiKey), apiVersion, correlationID, msgLen))
				// Parse and log content for key APIs
				content := parseRequestContent(apiKey, apiVersion, buf[4:4+msgLen])
				if content != "" {
					fmt.Println(fmt.Sprintf("%s\t", content))
				}
				// Show hex dump for key transaction APIs
				if apiKey == APIKeyEndTxn || apiKey == APIKeyInitProducerId || apiKey == APIKeyAddPartitions || apiKey == APIKeyProduce {
					fmt.Println(fmt.Sprintf("  [RAW HEX] %s\t", hex.EncodeToString(buf[4:4+msgLen])))
				}
			} else if apiKey == APIKeyEndTxn {
				fmt.Println(fmt.Sprintf("proxy: Intercepted EndTxn request (correlation_id=%d)\t", correlationID))
			}
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
						fmt.Println(fmt.Sprintf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=DROPPED\t",
							apiKeyName(reqInfo.APIKey), correlationID, msgLen, latency))
						// Parse and log content for key APIs
						content := parseResponseContent(reqInfo.APIKey, buf[4:4+msgLen])
						if content != "" {
							fmt.Println(fmt.Sprintf("%s\t", content))
						}
						// Show hex dump for key transaction APIs
						if reqInfo.APIKey == APIKeyEndTxn || reqInfo.APIKey == APIKeyInitProducerId {
							fmt.Println(fmt.Sprintf("  [RAW HEX] %s\t", hex.EncodeToString(buf[4:4+msgLen])))
						}
					} else {
						fmt.Println(fmt.Sprintf("proxy: DROPPING %s response (correlation_id=%d)\t",
							apiKeyName(reqInfo.APIKey), correlationID))
					}
					// If closeOnDrop is enabled, terminate the connection to prevent
					// librdkafka's "Invalid transaction state transition" crash
					if p.closeOnDrop {
						fmt.Println("proxy: Closing connection after dropping response")
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
						fmt.Println(fmt.Sprintf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=DROPPED (%d more)\t",
							apiKeyName(reqInfo.APIKey), correlationID, msgLen, latency, remaining-1))
						// Parse and log content for key APIs
						content := parseResponseContent(reqInfo.APIKey, buf[4:4+msgLen])
						if content != "" {
							fmt.Println(fmt.Sprintf("%s\t", content))
						}
						// Show hex dump for key transaction APIs
						if reqInfo.APIKey == APIKeyEndTxn || reqInfo.APIKey == APIKeyInitProducerId {
							fmt.Println(fmt.Sprintf("  [RAW HEX] %s\t", hex.EncodeToString(buf[4:4+msgLen])))
						}
					} else {
						fmt.Println(fmt.Sprintf("proxy: DROPPING %s response (correlation_id=%d) [%d more to drop]\t",
							apiKeyName(reqInfo.APIKey), correlationID, remaining-1))
					}
					// If closeOnDrop is enabled, terminate the connection to prevent
					// librdkafka's "Invalid transaction state transition" crash
					if p.closeOnDrop {
						fmt.Println("proxy: Closing connection after dropping response")
						return
					}
					continue // Don't forward this response
				}
				p.statsMu.Unlock()

				// Check if we should delay this response
				if delay, ok := p.delayAPIKeys[reqInfo.APIKey]; ok {
					if p.verboseLogging {
						fmt.Println(fmt.Sprintf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=DELAYING %v\t",
							apiKeyName(reqInfo.APIKey), correlationID, msgLen, latency, delay))
						// Parse and log content for key APIs
						content := parseResponseContent(reqInfo.APIKey, buf[4:4+msgLen])
						if content != "" {
							fmt.Println(fmt.Sprintf("%s\t", content))
						}
					} else {
						fmt.Println(fmt.Sprintf("proxy: DELAYING %s response by %v (correlation_id=%d)\t",
							apiKeyName(reqInfo.APIKey), delay, correlationID))
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

					if shouldInject && reqInfo.APIKey == APIKeyEndTxn {
						// Use kmsg to properly decode/encode EndTxn response
						// This handles flexible protocol versions correctly
						modifiedBuf, originalError, err := injectEndTxnError(buf[4:4+msgLen], reqInfo.APIVersion, injection.ErrorCode)
						if err != nil {
							fmt.Println(fmt.Sprintf("proxy: Failed to inject error: %v\t", err))
						} else {
							// Update the buffer with modified response
							// Write new length
							newLen := len(modifiedBuf)
							binary.BigEndian.PutUint32(buf[0:4], uint32(newLen))
							// Copy modified response
							copy(buf[4:], modifiedBuf)
							msgLen = newLen

							if p.verboseLogging {
								fmt.Println(fmt.Sprintf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=INJECTED_ERROR %d->%d (%s)\t",
									apiKeyName(reqInfo.APIKey), correlationID, msgLen, latency,
									originalError, injection.ErrorCode, kafkaErrorName(injection.ErrorCode)))
							} else {
								fmt.Println(fmt.Sprintf("proxy: INJECTING ERROR %d (%s) into %s response (was %d, correlation_id=%d)\t",
									injection.ErrorCode, kafkaErrorName(injection.ErrorCode),
									apiKeyName(reqInfo.APIKey), originalError, correlationID))
							}
						}
					}
				} else {
					p.statsMu.Unlock()
				}

				// Log forwarded response
				if p.verboseLogging {
					fmt.Println(fmt.Sprintf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes latency=%v ACTION=FORWARDED\t",
						apiKeyName(reqInfo.APIKey), correlationID, msgLen, latency))
					// Parse and log content for key APIs
					content := parseResponseContent(reqInfo.APIKey, buf[4:4+msgLen])
					if content != "" {
						fmt.Println(fmt.Sprintf("%s\t", content))
					}
					// Show hex dump for key APIs including Fetch and ListOffsets for debugging
					if reqInfo.APIKey == APIKeyEndTxn || reqInfo.APIKey == APIKeyInitProducerId || reqInfo.APIKey == APIKeyFetch || reqInfo.APIKey == APIKeyListOffsets || reqInfo.APIKey == APIKeyProduce {
						fmt.Println(fmt.Sprintf("  [RAW HEX] %s\t", hex.EncodeToString(buf[4:4+msgLen])))
					}
				}
			} else if p.verboseLogging {
				// Response for unknown request
				fmt.Println(fmt.Sprintf("[PROXY] ◄── RESPONSE api=%-20s corr_id=%-4d size=%d bytes (no matching request)\t",
					"UNKNOWN", correlationID, msgLen))
			}
		}

		// Forward to client
		_, err = client.Write(buf[:4+msgLen])
		if err != nil {
			return
		}
	}
}

func apiKeyName(key KafkaAPIKey) string {
	names := map[KafkaAPIKey]string{
		0:  "Produce",
		1:  "Fetch",
		2:  "ListOffsets",
		3:  "Metadata",
		4:  "LeaderAndIsr",
		5:  "StopReplica",
		6:  "UpdateMetadata",
		7:  "ControlledShutdown",
		8:  "OffsetCommit",
		9:  "OffsetFetch",
		10: "FindCoordinator",
		11: "JoinGroup",
		12: "Heartbeat",
		13: "LeaveGroup",
		14: "SyncGroup",
		15: "DescribeGroups",
		16: "ListGroups",
		17: "SaslHandshake",
		18: "ApiVersions",
		19: "CreateTopics",
		20: "DeleteTopics",
		21: "DeleteRecords",
		22: "InitProducerId",
		23: "OffsetForLeaderEpoch",
		24: "AddPartitionsToTxn",
		25: "AddOffsetsToTxn",
		26: "EndTxn",
		27: "WriteTxnMarkers",
		28: "TxnOffsetCommit",
		29: "DescribeAcls",
		30: "CreateAcls",
		31: "DeleteAcls",
		32: "DescribeConfigs",
		33: "AlterConfigs",
		34: "AlterReplicaLogDirs",
		35: "DescribeLogDirs",
		36: "SaslAuthenticate",
		37: "CreatePartitions",
		38: "CreateDelegationToken",
		39: "RenewDelegationToken",
		40: "ExpireDelegationToken",
		41: "DescribeDelegationToken",
		42: "DeleteGroups",
		43: "ElectLeaders",
		44: "IncrementalAlterConfigs",
		45: "AlterPartitionReassignments",
		46: "ListPartitionReassignments",
		47: "OffsetDelete",
		48: "DescribeClientQuotas",
		49: "AlterClientQuotas",
		50: "DescribeUserScramCredentials",
		51: "AlterUserScramCredentials",
		56: "AlterPartition",
		57: "UpdateFeatures",
		60: "DescribeCluster",
		61: "DescribeProducers",
		65: "DescribeTransactions",
		66: "ListTransactions",
		67: "AllocateProducerIds",
	}
	if name, ok := names[key]; ok {
		return name
	}
	return fmt.Sprintf("APIKey(%d)", key)
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
	case APIKeyAddPartitions:
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
// Based on Kafka protocol error codes from franz-go/pkg/kerr
func kafkaErrorName(code int16) string {
	errors := map[int16]string{
		0:  "NONE",
		-1: "UNKNOWN_SERVER_ERROR",
		1:  "OFFSET_OUT_OF_RANGE",
		2:  "CORRUPT_MESSAGE",
		3:  "UNKNOWN_TOPIC_OR_PARTITION",
		4:  "INVALID_FETCH_SIZE",
		5:  "LEADER_NOT_AVAILABLE",
		6:  "NOT_LEADER_FOR_PARTITION",
		7:  "REQUEST_TIMED_OUT",
		8:  "BROKER_NOT_AVAILABLE",
		9:  "REPLICA_NOT_AVAILABLE",
		10: "MESSAGE_TOO_LARGE",
		11: "STALE_CONTROLLER_EPOCH",
		12: "OFFSET_METADATA_TOO_LARGE",
		13: "NETWORK_EXCEPTION",
		14: "COORDINATOR_LOAD_IN_PROGRESS",
		15: "COORDINATOR_NOT_AVAILABLE",
		16: "NOT_COORDINATOR",
		17: "INVALID_TOPIC_EXCEPTION",
		18: "RECORD_LIST_TOO_LARGE",
		19: "NOT_ENOUGH_REPLICAS",
		20: "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
		21: "INVALID_REQUIRED_ACKS",
		22: "ILLEGAL_GENERATION",
		23: "INCONSISTENT_GROUP_PROTOCOL",
		24: "INVALID_GROUP_ID",
		25: "UNKNOWN_MEMBER_ID",
		26: "INVALID_SESSION_TIMEOUT",
		27: "REBALANCE_IN_PROGRESS",
		28: "INVALID_COMMIT_OFFSET_SIZE",
		29: "TOPIC_AUTHORIZATION_FAILED",
		30: "GROUP_AUTHORIZATION_FAILED",
		31: "CLUSTER_AUTHORIZATION_FAILED",
		32: "INVALID_TIMESTAMP",
		33: "UNSUPPORTED_SASL_MECHANISM",
		34: "ILLEGAL_SASL_STATE",
		35: "UNSUPPORTED_VERSION",
		36: "TOPIC_ALREADY_EXISTS",
		37: "INVALID_PARTITIONS",
		38: "INVALID_REPLICATION_FACTOR",
		39: "INVALID_REPLICA_ASSIGNMENT",
		40: "INVALID_CONFIG",
		41: "NOT_CONTROLLER",
		42: "INVALID_REQUEST",
		43: "UNSUPPORTED_FOR_MESSAGE_FORMAT",
		44: "POLICY_VIOLATION",
		45: "OUT_OF_ORDER_SEQUENCE_NUMBER",
		46: "DUPLICATE_SEQUENCE_NUMBER",
		47: "INVALID_PRODUCER_EPOCH",
		48: "INVALID_TXN_STATE",
		49: "INVALID_PRODUCER_ID_MAPPING",
		50: "INVALID_TRANSACTION_TIMEOUT",
		51: "CONCURRENT_TRANSACTIONS",
		52: "TRANSACTION_COORDINATOR_FENCED",
		53: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
		54: "SECURITY_DISABLED",
		55: "OPERATION_NOT_ATTEMPTED",
		56: "KAFKA_STORAGE_ERROR",
		57: "LOG_DIR_NOT_FOUND",
		58: "SASL_AUTHENTICATION_FAILED",
		59: "UNKNOWN_PRODUCER_ID",
		60: "REASSIGNMENT_IN_PROGRESS",
		90: "PRODUCER_FENCED",
	}
	if name, ok := errors[code]; ok {
		return name
	}
	return fmt.Sprintf("ERROR_%d", code)
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
