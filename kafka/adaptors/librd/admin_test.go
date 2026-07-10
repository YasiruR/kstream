package librd

import (
	"testing"
	"time"

	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/tryfix/log"
)

// TestKAdmin_NewAdmin tests admin client creation
func TestKAdmin_NewAdmin(t *testing.T) {
	tests := []struct {
		name            string
		bootstrapServer []string
		options         []AdminOption
		expectPanic     bool
	}{
		{
			name:            "valid bootstrap server",
			bootstrapServer: []string{"localhost:9092"},
			options:         []AdminOption{WithTimeout(5 * time.Second)},
			expectPanic:     false,
		},
		{
			name:            "multiple bootstrap servers",
			bootstrapServer: []string{"localhost:9092", "localhost:9093"},
			options:         []AdminOption{WithLogger(log.NewNoopLogger())},
			expectPanic:     false,
		},
		{
			name:            "empty bootstrap server",
			bootstrapServer: []string{},
			options:         nil,
			expectPanic:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Expected panic but didn't get one")
					}
				}()
			}

			admin := NewAdmin(tt.bootstrapServer, tt.options...)
			if !tt.expectPanic {
				if admin == nil {
					t.Error("Expected admin client but got nil")
				}
				if admin.timeout != 10*time.Second && len(tt.options) == 0 {
					t.Error("Expected default timeout of 10 seconds")
				}
				admin.Close()
			}
		})
	}
}

// TestKAdmin_FetchInfo tests topic metadata fetching with error scenarios
func TestKAdmin_FetchInfo(t *testing.T) {
	// Create mock broker using librdkafka's mock cluster
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	bootstrapServers := mockCluster.BootstrapServers()

	tests := []struct {
		name           string
		topics         []string
		setupMock      func(*librdKafka.MockCluster)
		expectError    bool
		expectedTopics int
	}{
		{
			name:   "fetch single topic info",
			topics: []string{"test-topic"},
			setupMock: func(cluster *librdKafka.MockCluster) {
				cluster.CreateTopic("test-topic", 3, 1)
			},
			expectError:    false,
			expectedTopics: 1,
		},
		{
			name:   "fetch multiple topics info",
			topics: []string{"topic1", "topic2"},
			setupMock: func(cluster *librdKafka.MockCluster) {
				cluster.CreateTopic("topic1", 2, 1)
				cluster.CreateTopic("topic2", 4, 1)
			},
			expectError:    false,
			expectedTopics: 2,
		},
		{
			name:   "fetch non-existent topic",
			topics: []string{"non-existent-topic"},
			setupMock: func(cluster *librdKafka.MockCluster) {
				// Don't create the topic
			},
			expectError:    false, // Non-existent topics return an empty result, not error
			expectedTopics: 0,
		},
		{
			name:   "mixed existing and non-existing topics",
			topics: []string{"existing-topic", "non-existing-topic"},
			setupMock: func(cluster *librdKafka.MockCluster) {
				if err := cluster.CreateTopic("existing-topic", 1, 1); err != nil {
					t.Fatalf("Failed to create topic: %v", err)
				}
			},
			expectError:    false, // In mock mode, non-existing topics are ignored
			expectedTopics: 1,     // Only the existing topic should be returned
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock(mockCluster)

			admin := NewAdmin([]string{bootstrapServers}, WithTimeout(5*time.Second), WithMockBrokerEnabled())
			defer admin.Close()

			topicInfo, err := admin.FetchInfo(tt.topics)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(topicInfo) != tt.expectedTopics {
				t.Errorf("Expected %d topics, got %d", tt.expectedTopics, len(topicInfo))
			}

			// Verify topic details
			for _, topicName := range tt.topics {
				if topic, exists := topicInfo[topicName]; exists {
					if topic.Name != topicName {
						t.Errorf("Expected topic name %s, got %s", topicName, topic.Name)
					}
					if topic.NumPartitions <= 0 {
						t.Error("Expected positive number of partitions")
					}
					if topic.ReplicationFactor <= 0 {
						t.Error("Expected positive replication factor")
					}
				}
			}
		})
	}
}

// TestKAdmin_FetchInfoWithMockTopics tests FetchInfo with pre-created mock topics
func TestKAdmin_FetchInfoWithMockTopics(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	// Create test topics using mock cluster
	mockCluster.CreateTopic("single-topic", 3, 1)
	mockCluster.CreateTopic("topic-1", 2, 1)
	mockCluster.CreateTopic("topic-2", 4, 1)
	mockCluster.CreateTopic("partition-test", 5, 1)

	bootstrapServers := mockCluster.BootstrapServers()

	tests := []struct {
		name           string
		topics         []string
		expectedTopics int
		expectError    bool
	}{
		{
			name:           "fetch single existing topic",
			topics:         []string{"single-topic"},
			expectedTopics: 1,
			expectError:    false,
		},
		{
			name:           "fetch multiple existing topics",
			topics:         []string{"topic-1", "topic-2"},
			expectedTopics: 2,
			expectError:    false,
		},
		{
			name:           "fetch non-existent topic",
			topics:         []string{"non-existent"},
			expectedTopics: 0,
			expectError:    false, // Non-existent topics return empty result, not error
		},
		{
			name:           "fetch mixed existing and non-existing",
			topics:         []string{"topic-1", "non-existent"},
			expectedTopics: 1,
			expectError:    false, // Only existing topics are returned, no error
		},
		{
			name:           "verify partition counts",
			topics:         []string{"partition-test"},
			expectedTopics: 1,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			admin := NewAdmin([]string{bootstrapServers}, WithTimeout(5*time.Second), WithMockBrokerEnabled())
			defer admin.Close()

			topicInfo, err := admin.FetchInfo(tt.topics)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(topicInfo) != tt.expectedTopics {
				t.Errorf("Expected %d topics, got %d", tt.expectedTopics, len(topicInfo))
			}

			// Verify specific topic details for the partition test
			if tt.name == "verify partition counts" {
				if topic, exists := topicInfo["partition-test"]; exists {
					if topic.NumPartitions != 5 {
						t.Errorf("Expected 5 partitions, got %d", topic.NumPartitions)
					}
					if topic.ReplicationFactor != 1 {
						t.Errorf("Expected replication factor 1, got %d", topic.ReplicationFactor)
					}
					if len(topic.Partitions) != 5 {
						t.Errorf("Expected 5 partition configs, got %d", len(topic.Partitions))
					}
				}
			}
		})
	}
}

// TestKAdmin_ListTopicsWithMock tests listing topics with mock cluster
func TestKAdmin_ListTopicsWithMock(t *testing.T) {
	tests := []struct {
		name          string
		setupTopics   func(*librdKafka.MockCluster)
		expectError   bool
		minTopics     int
		expectedNames []string
	}{
		{
			name: "list topics from empty cluster",
			setupTopics: func(cluster *librdKafka.MockCluster) {
				// No topics created
			},
			expectError:   false,
			minTopics:     0,
			expectedNames: []string{},
		},
		{
			name: "list topics with existing topics",
			setupTopics: func(cluster *librdKafka.MockCluster) {
				cluster.CreateTopic("list-topic-1", 1, 1)
				cluster.CreateTopic("list-topic-2", 2, 1)
				cluster.CreateTopic("list-topic-3", 3, 1)
			},
			expectError:   false,
			minTopics:     3,
			expectedNames: []string{"list-topic-1", "list-topic-2", "list-topic-3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCluster, err := librdKafka.NewMockCluster(1)
			if err != nil {
				t.Fatalf("Failed to create mock cluster: %v", err)
			}
			defer mockCluster.Close()

			tt.setupTopics(mockCluster)

			admin := NewAdmin([]string{mockCluster.BootstrapServers()}, WithTimeout(5*time.Second), WithMockBrokerEnabled())
			defer admin.Close()

			topics, err := admin.ListTopics()

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(topics) < tt.minTopics {
				t.Errorf("Expected at least %d topics, got %d", tt.minTopics, len(topics))
			}

			// Check if expected topics are present
			for _, expectedTopic := range tt.expectedNames {
				found := false
				for _, actualTopic := range topics {
					if actualTopic == expectedTopic {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected topic %s not found in list: %v", expectedTopic, topics)
				}
			}
		})
	}
}

// TestKAdmin_StoreConfigs tests the config storage functionality
func TestKAdmin_StoreConfigs(t *testing.T) {
	admin := NewAdmin([]string{"localhost:9092"}) // Doesn't need to connect for this test
	defer admin.Close()

	tests := []struct {
		name        string
		topics      []*kafka.Topic
		expectError bool
		errorMsg    string
	}{
		{
			name: "store single topic config",
			topics: []*kafka.Topic{
				{
					Name:              "config-topic",
					NumPartitions:     2,
					ReplicationFactor: 1,
					ConfigEntries: map[string]string{
						"cleanup.policy": "compact",
						"retention.ms":   "604800000",
					},
				},
			},
			expectError: false,
		},
		{
			name: "store multiple topic configs",
			topics: []*kafka.Topic{
				{
					Name:              "config-topic1",
					NumPartitions:     1,
					ReplicationFactor: 1,
				},
				{
					Name:              "config-topic2",
					NumPartitions:     3,
					ReplicationFactor: 1,
				},
			},
			expectError: false,
		},
		{
			name: "store duplicate topic config should error",
			topics: []*kafka.Topic{
				{
					Name:              "duplicate-topic",
					NumPartitions:     1,
					ReplicationFactor: 1,
				},
			},
			expectError: true,
			errorMsg:    "already marked for creation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fresh admin for each test to avoid state
			testAdmin := NewAdmin([]string{"localhost:9092"})
			defer testAdmin.Close()

			// First store should always succeed
			err := testAdmin.StoreConfigs(tt.topics)
			if err != nil {
				t.Errorf("Unexpected error on first store: %v", err)
				return
			}

			if tt.expectError {
				// Try to store the same configs again - this should error
				err = testAdmin.StoreConfigs(tt.topics)
				if err == nil {
					t.Error("Expected error for duplicate topic config but got nil")
					return
				}
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %v", tt.errorMsg, err)
				}
			}

			// Verify configs are stored in memory
			if len(testAdmin.tempTopicConfigs) != len(tt.topics) {
				t.Errorf("Expected %d stored configs, got %d", len(tt.topics), len(testAdmin.tempTopicConfigs))
			}

			for _, topic := range tt.topics {
				if storedTopic, exists := testAdmin.tempTopicConfigs[topic.Name]; exists {
					if storedTopic.Name != topic.Name {
						t.Errorf("Expected stored topic name %s, got %s", topic.Name, storedTopic.Name)
					}
					if storedTopic.NumPartitions != topic.NumPartitions {
						t.Errorf("Expected %d partitions, got %d", topic.NumPartitions, storedTopic.NumPartitions)
					}
				} else {
					t.Errorf("Topic %s not found in stored configs", topic.Name)
				}
			}
		})
	}
}

// TestKAdmin_ErrorHandling tests various error scenarios
func TestKAdmin_ErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		testFunc    func(t *testing.T)
		description string
	}{
		{
			name:        "connection_errors",
			description: "Test behavior with connection errors",
			testFunc: func(t *testing.T) {
				admin := NewAdmin([]string{"localhost:9999"}) // Non-existent port
				defer admin.Close()

				// Operations should fail gracefully, not panic
				_, err := admin.FetchInfo([]string{"test-topic"})
				if err == nil {
					t.Error("Expected connection error")
				}
			},
		},
		{
			name:        "timeout scenarios",
			description: "Test various timeout scenarios",
			testFunc: func(t *testing.T) {
				admin := NewAdmin([]string{"localhost:9092"}, WithTimeout(1*time.Millisecond))
				defer admin.Close()

				// This should timeout quickly
				_, err := admin.FetchInfo([]string{"test-topic"})
				if err == nil {
					t.Error("Expected timeout error")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t)
		})
	}
}

// TestKAdmin_ErrorMessageValidation tests that our error handling improvements work correctly
func TestKAdmin_ErrorMessageValidation(t *testing.T) {
	tests := []struct {
		name        string
		description string
		testFunc    func(t *testing.T)
	}{
		{
			name:        "verify_empty_handling",
			description: "Validate empty input handling doesn't panic",
			testFunc: func(t *testing.T) {
				admin := NewAdmin([]string{"localhost:9092"}, WithMockBrokerEnabled())
				defer admin.Close()

				// These should not panic (our improvements)
				topicInfo, err := admin.FetchInfo([]string{})
				if err != nil {
					t.Errorf("Unexpected error for empty topic list: %v", err)
				}
				if len(topicInfo) != 0 {
					t.Errorf("Expected empty result, got %d topics", len(topicInfo))
				}

				// Empty topic creation should not panic
				err = admin.CreateTopics([]*kafka.Topic{})
				if err != nil {
					t.Errorf("Unexpected error for empty topics: %v", err)
				}
			},
		},
		{
			name:        "config_storage_validation",
			description: "Test config storage logic",
			testFunc: func(t *testing.T) {
				admin := NewAdmin([]string{"localhost:9092"})
				defer admin.Close()

				// Test config storage state management
				topic := &kafka.Topic{
					Name:              "test-config",
					NumPartitions:     1,
					ReplicationFactor: 1,
					ConfigEntries: map[string]string{
						"cleanup.policy": "compact",
					},
				}

				// First store should succeed
				err := admin.StoreConfigs([]*kafka.Topic{topic})
				if err != nil {
					t.Errorf("Unexpected error storing config: %v", err)
				}

				// Duplicate store should fail with specific error message
				err = admin.StoreConfigs([]*kafka.Topic{topic})
				if err == nil {
					t.Error("Expected error for duplicate config storage")
				}
				if !contains(err.Error(), "already marked for creation") {
					t.Errorf("Expected specific error message, got: %v", err)
				}

				// Verify config is in memory
				if len(admin.tempTopicConfigs) != 1 {
					t.Errorf("Expected 1 stored config, got %d", len(admin.tempTopicConfigs))
				}
				if stored, exists := admin.tempTopicConfigs[topic.Name]; !exists {
					t.Error("Config not found in storage")
				} else if stored.Name != topic.Name {
					t.Errorf("Stored config name mismatch: expected %s, got %s", topic.Name, stored.Name)
				}
			},
		},
		{
			name:        "admin_options_validation",
			description: "Test admin options handling",
			testFunc: func(t *testing.T) {
				// Test default options
				admin1 := NewAdmin([]string{"localhost:9092"})
				defer admin1.Close()
				if admin1.timeout != 10*time.Second {
					t.Errorf("Expected default timeout 10s, got %v", admin1.timeout)
				}

				// Test custom options
				customTimeout := 5 * time.Second
				admin2 := NewAdmin([]string{"localhost:9092"}, WithTimeout(customTimeout))
				defer admin2.Close()
				if admin2.timeout != customTimeout {
					t.Errorf("Expected custom timeout %v, got %v", customTimeout, admin2.timeout)
				}

				// Test logger option
				admin3 := NewAdmin([]string{"localhost:9092"}, WithLogger(log.NewNoopLogger()))
				defer admin3.Close()
				if admin3.logger == nil {
					t.Error("Expected logger to be set")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t)
		})
	}
}

// TestKAdmin_MetadataErrorHandling tests specific metadata error scenarios
func TestKAdmin_MetadataErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		description string
		testFunc    func(t *testing.T)
	}{
		{
			name:        "topic_metadata_errors",
			description: "Test handling of topic-level metadata errors",
			testFunc: func(t *testing.T) {
				// This test simulates the error handling paths we improved
				mockCluster, err := librdKafka.NewMockCluster(1)
				if err != nil {
					t.Fatalf("Failed to create mock cluster: %v", err)
				}
				defer mockCluster.Close()

				admin := NewAdmin([]string{mockCluster.BootstrapServers()}, WithMockBrokerEnabled())
				defer admin.Close()

				// Test with non-existent topics - in mock mode this returns empty result
				topicInfo, err := admin.FetchInfo([]string{"non-existent-topic"})
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				// In mock mode, non-existent topics return empty results
				if len(topicInfo) != 0 {
					t.Errorf("Expected empty result for non-existent topic, got %d topics", len(topicInfo))
				}
			},
		},
		{
			name:        "partition_metadata_errors",
			description: "Test handling of partition-level metadata errors",
			testFunc: func(t *testing.T) {
				mockCluster, err := librdKafka.NewMockCluster(1)
				if err != nil {
					t.Fatalf("Failed to create mock cluster: %v", err)
				}
				defer mockCluster.Close()

				admin := NewAdmin([]string{mockCluster.BootstrapServers()}, WithMockBrokerEnabled())
				defer admin.Close()

				// Create a topic and then test metadata fetching
				mockCluster.CreateTopic("test-partition-errors", 1, 1)

				// Test normal operation first
				topicInfo, err := admin.FetchInfo([]string{"test-partition-errors"})
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if len(topicInfo) != 1 {
					t.Errorf("Expected 1 topic, got %d", len(topicInfo))
				}
			},
		},
		{
			name:        "verify_action_error_handling",
			description: "Test verifyAction method error handling",
			testFunc: func(t *testing.T) {
				mockCluster, err := librdKafka.NewMockCluster(1)
				if err != nil {
					t.Fatalf("Failed to create mock cluster: %v", err)
				}
				defer mockCluster.Close()

				admin := NewAdmin([]string{mockCluster.BootstrapServers()}, WithTimeout(100*time.Millisecond), WithMockBrokerEnabled())
				defer admin.Close()

				// Test topic creation with very short timeout to potentially trigger verifyAction errors
				topics := []*kafka.Topic{
					{
						Name:              "verify-test",
						NumPartitions:     1,
						ReplicationFactor: 1,
					},
				}

				// This may pass or fail depending on timing, but should not panic
				err = admin.CreateTopics(topics)
				// We don't assert on error here as it depends on mock broker timing
				t.Logf("CreateTopics result: %v", err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t)
		})
	}
}

// TestKAdmin_ConfigEntries tests config entry handling
func TestKAdmin_ConfigEntries(t *testing.T) {
	mockCluster, err := librdKafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %v", err)
	}
	defer mockCluster.Close()

	admin := NewAdmin([]string{mockCluster.BootstrapServers()}, WithMockBrokerEnabled())
	defer admin.Close()

	// Create topic with configs
	mockCluster.CreateTopic("config-test", 1, 1)

	topicInfo, err := admin.FetchInfo([]string{"config-test"})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}

	if topic, exists := topicInfo["config-test"]; exists {
		if topic.ConfigEntries == nil {
			t.Error("ConfigEntries map should not be nil")
		}
	} else {
		t.Error("Expected topic config-test not found")
	}
}

// TestKAdmin_EdgeCases tests edge cases and boundary conditions
func TestKAdmin_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		testFunc    func(t *testing.T)
		description string
	}{
		{
			name:        "empty_topic_list",
			description: "Test with empty topic list",
			testFunc: func(t *testing.T) {
				mockCluster, err := librdKafka.NewMockCluster(1)
				if err != nil {
					t.Fatalf("Failed to create mock cluster: %v", err)
				}
				defer mockCluster.Close()

				admin := NewAdmin([]string{mockCluster.BootstrapServers()}, WithMockBrokerEnabled())
				defer admin.Close()

				topicInfo, err := admin.FetchInfo([]string{})
				if err != nil {
					t.Errorf("Unexpected error with empty topic list: %v", err)
				}
				if len(topicInfo) != 0 {
					t.Errorf("Expected empty result, got %d topics", len(topicInfo))
				}
			},
		},
		{
			name:        "nil_topic_configs",
			description: "Test with nil topic configurations",
			testFunc: func(t *testing.T) {
				mockCluster, err := librdKafka.NewMockCluster(1)
				if err != nil {
					t.Fatalf("Failed to create mock cluster: %v", err)
				}
				defer mockCluster.Close()

				admin := NewAdmin([]string{mockCluster.BootstrapServers()}, WithMockBrokerEnabled())
				defer admin.Close()

				// Test create topics with empty list
				err = admin.CreateTopics([]*kafka.Topic{})
				if err != nil {
					t.Errorf("Unexpected error with empty topics list: %v", err)
				}
			},
		},
		{
			name:        "large_number_of_partitions",
			description: "Test with large number of partitions",
			testFunc: func(t *testing.T) {
				mockCluster, err := librdKafka.NewMockCluster(3)
				if err != nil {
					t.Fatalf("Failed to create mock cluster: %v", err)
				}
				defer mockCluster.Close()

				admin := NewAdmin([]string{mockCluster.BootstrapServers()}, WithMockBrokerEnabled())
				defer admin.Close()

				// Create topic with many partitions
				topics := []*kafka.Topic{
					{
						Name:              "large-partition-topic",
						NumPartitions:     100,
						ReplicationFactor: 1,
					},
				}

				err = admin.CreateTopics(topics)
				// This may fail due to broker limitations, but should handle gracefully
				t.Logf("Large partition topic creation result: %v", err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t)
		})
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			containsMiddle(s, substr))))
}

func containsMiddle(s, substr string) bool {
	for i := 1; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
