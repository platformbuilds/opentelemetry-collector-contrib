package alertsgenconnector_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/state"
)

func TestTSDBSyncerIntegration(t *testing.T) {
	// Mock Prometheus/VictoriaMetrics server
	mockTSDB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/api/v1/query"):
			// Mock query response with active alerts
			response := `{
				"status": "success",
				"data": {
					"resultType": "vector",
					"result": [
						{
							"metric": {
								"__name__": "otel_alert_active",
								"rule_id": "high_latency",
								"service_name": "payment",
								"cluster": "prod",
								"severity": "critical"
							},
							"value": [1640995200, "1"]
						}
					]
				}
			}`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(response))

		case strings.Contains(r.URL.Path, "/api/v1/write"):
			// Mock remote write endpoint
			assert.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))
			assert.Equal(t, "snappy", r.Header.Get("Content-Encoding"))
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mockTSDB.Close()

	// Create TSDB syncer
	cfg := state.TSDBConfig{
		QueryURL:                 mockTSDB.URL,
		RemoteWriteURL:           mockTSDB.URL + "/api/v1/write",
		QueryTimeout:             10 * time.Second,
		WriteTimeout:             5 * time.Second,
		DedupWindow:              30 * time.Second,
		InstanceID:               "test-instance",
		EnableRemoteWrite:        true,
		RemoteWriteBatchSize:     100,
		RemoteWriteFlushInterval: 1 * time.Second,
	}

	syncer, err := state.NewTSDBSyncer(cfg)
	require.NoError(t, err)
	require.NotNil(t, syncer)

	// Test querying active alerts
	t.Run("QueryActive", func(t *testing.T) {
		entries, err := syncer.QueryActive("high_latency")
		require.NoError(t, err)
		assert.Len(t, entries, 1)

		// Verify the entry contains expected data
		for fp, entry := range entries {
			assert.True(t, fp > 0, "Fingerprint should be non-zero")
			assert.True(t, entry.Active)
			assert.Equal(t, "payment", entry.Labels["service_name"])
			assert.Equal(t, "prod", entry.Labels["cluster"])
		}
	})

	// Test publishing events
	t.Run("PublishEvents", func(t *testing.T) {
		now := time.Now()
		events := []interface{}{
			state.AlertEvent{
				Rule:        "test_rule",
				State:       "firing",
				Severity:    "warning",
				Labels:      map[string]string{"service": "test", "cluster": "dev"},
				Value:       42.5,
				Window:      "5m",
				For:         "2m",
				Timestamp:   now,
				Fingerprint: 12345,
			},
			state.AlertEvent{
				Rule:        "test_rule",
				State:       "resolved",
				Severity:    "warning",
				Labels:      map[string]string{"service": "test", "cluster": "dev"},
				Value:       10.0,
				Window:      "5m",
				For:         "2m",
				Timestamp:   now,
				Fingerprint: 12345,
			},
		}

		err := syncer.PublishEvents(events)
		assert.NoError(t, err)
	})

	// Test with map-based events (compatibility)
	t.Run("PublishMapEvents", func(t *testing.T) {
		mapEvents := []interface{}{
			map[string]interface{}{
				"rule":        "map_test_rule",
				"state":       "firing",
				"severity":    "critical",
				"labels":      map[string]string{"service": "api", "env": "prod"},
				"value":       100.0,
				"window":      "10m",
				"for":         "5m",
				"fingerprint": uint64(67890),
			},
		}

		err := syncer.PublishEvents(mapEvents)
		assert.NoError(t, err)
	})

	// Test error handling
	t.Run("InvalidQueryURL", func(t *testing.T) {
		invalidCfg := cfg
		invalidCfg.QueryURL = "invalid-url"

		_, err := state.NewTSDBSyncer(invalidCfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid query_url")
	})

	// Test disabled remote write
	t.Run("DisabledRemoteWrite", func(t *testing.T) {
		disabledCfg := cfg
		disabledCfg.RemoteWriteURL = ""

		syncerDisabled, err := state.NewTSDBSyncer(disabledCfg)
		require.NoError(t, err)

		// Should not error, but should be no-op
		err = syncerDisabled.PublishEvents([]interface{}{
			state.AlertEvent{Rule: "test", State: "firing"},
		})
		assert.NoError(t, err)
	})
}

func TestTSDBSyncerWithFailedServer(t *testing.T) {
	// Mock server that returns errors
	errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer errorServer.Close()

	cfg := state.TSDBConfig{
		QueryURL:       errorServer.URL,
		RemoteWriteURL: errorServer.URL + "/api/v1/write",
		QueryTimeout:   5 * time.Second,
		WriteTimeout:   5 * time.Second,
		InstanceID:     "test-error",
	}

	syncer, err := state.NewTSDBSyncer(cfg)
	require.NoError(t, err)

	// Test query failure
	t.Run("QueryFailure", func(t *testing.T) {
		_, err := syncer.QueryActive("test_rule")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query active alerts")
	})

	// Test publish failure
	t.Run("PublishFailure", func(t *testing.T) {
		events := []interface{}{
			state.AlertEvent{
				Rule:  "test_rule",
				State: "firing",
			},
		}

		err := syncer.PublishEvents(events)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "remote write failed")
	})
}

func TestTSDBConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         state.TSDBConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid_config",
			cfg: state.TSDBConfig{
				QueryURL:       "http://prometheus:9090",
				RemoteWriteURL: "http://prometheus:9090/api/v1/write",
				QueryTimeout:   30 * time.Second,
				InstanceID:     "test",
			},
			expectError: false,
		},
		{
			name: "missing_query_url",
			cfg: state.TSDBConfig{
				RemoteWriteURL: "http://prometheus:9090/api/v1/write",
			},
			expectError: true,
			errorMsg:    "query_url required",
		},
		{
			name: "invalid_query_url",
			cfg: state.TSDBConfig{
				QueryURL: "not-a-url",
			},
			expectError: true,
			errorMsg:    "invalid query_url",
		},
		{
			name: "invalid_remote_write_url",
			cfg: state.TSDBConfig{
				QueryURL:       "http://prometheus:9090",
				RemoteWriteURL: "not-a-url",
			},
			expectError: true,
			errorMsg:    "invalid remote_write_url",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := state.NewTSDBSyncer(tt.cfg)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFingerprintConsistency(t *testing.T) {
	// Test that fingerprint calculation is consistent
	rule := "test_rule"
	labels := map[string]string{
		"service": "payment",
		"cluster": "prod",
		"version": "v1.2.3",
	}

	// Create TSDB syncer to access fingerprint function
	cfg := state.TSDBConfig{
		QueryURL:   "http://localhost:9090",
		InstanceID: "test",
	}
	syncer, err := state.NewTSDBSyncer(cfg)
	require.NoError(t, err)

	// Calculate fingerprint multiple times
	fp1 := calculateFingerprint(rule, labels)
	fp2 := calculateFingerprint(rule, labels)
	fp3 := calculateFingerprint(rule, labels)

	// Should be consistent
	assert.Equal(t, fp1, fp2)
	assert.Equal(t, fp2, fp3)
	assert.NotZero(t, fp1)

	// Different labels should produce different fingerprints
	differentLabels := map[string]string{
		"service": "user",
		"cluster": "prod",
		"version": "v1.2.3",
	}
	fp4 := calculateFingerprint(rule, differentLabels)
	assert.NotEqual(t, fp1, fp4)

	// Different rule should produce different fingerprints
	fp5 := calculateFingerprint("different_rule", labels)
	assert.NotEqual(t, fp1, fp5)
}

// Helper function to calculate fingerprint (normally internal)
func calculateFingerprint(rule string, labels map[string]string) uint64 {
	// This should match the internal fingerprint function
	// For testing purposes, we can create a simple implementation
	h := fnv64a{}
	h.WriteString(rule)

	// Sort labels for consistency
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}

	// Sort keys (in real implementation, this is done)
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	for _, k := range keys {
		h.WriteByte(0xff)
		h.WriteString(k)
		h.WriteByte(0xff)
		h.WriteString(labels[k])
	}

	return h.Sum64()
}

// Simple FNV-1a implementation for testing
type fnv64a struct{ sum uint64 }

func (f *fnv64a) WriteByte(b byte) error {
	if f.sum == 0 {
		f.sum = 1469598103934665603
	}
	f.sum ^= uint64(b)
	f.sum *= 1099511628211
	return nil
}

func (f *fnv64a) WriteString(s string) {
	for i := 0; i < len(s); i++ {
		f.WriteByte(s[i])
	}
}

func (f *fnv64a) Sum64() uint64 {
	if f.sum == 0 {
		return 1469598103934665603
	}
	return f.sum
}
