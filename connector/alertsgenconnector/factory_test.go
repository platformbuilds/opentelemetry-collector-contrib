// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)

	assert.Equal(t, component.MustNewType("alertsgen"), factory.Type())

	// Check supported pipeline types
	assert.True(t, factory.TracesToLogsStability() == component.StabilityLevelAlpha)
	assert.True(t, factory.MetricsToLogsStability() == component.StabilityLevelAlpha)
	assert.True(t, factory.MetricsToMetricsStability() == component.StabilityLevelAlpha)
}

func TestCreateTracesToLogs(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	ctx := context.Background()
	set := connectortest.NewNopSettings()

	// Create logs consumer
	logsConsumer := consumertest.NewNop()

	// Create connector
	conn, err := factory.CreateTracesToLogs(ctx, set, cfg, logsConsumer)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Type assertion to check internal state
	alertConn, ok := conn.(*alertsConnector)
	require.True(t, ok)
	assert.Equal(t, logsConsumer, alertConn.nextLogs)
	assert.Nil(t, alertConn.nextMetrics)
	assert.Nil(t, alertConn.nextTraces)
}

func TestCreateMetricsToLogs(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	ctx := context.Background()
	set := connectortest.NewNopSettings()

	// Create logs consumer
	logsConsumer := consumertest.NewNop()

	// Create connector
	conn, err := factory.CreateMetricsToLogs(ctx, set, cfg, logsConsumer)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Type assertion to check internal state
	alertConn, ok := conn.(*alertsConnector)
	require.True(t, ok)
	assert.Equal(t, logsConsumer, alertConn.nextLogs)
	assert.Nil(t, alertConn.nextMetrics)
	assert.Nil(t, alertConn.nextTraces)
}

func TestCreateMetricsToMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	ctx := context.Background()
	set := connectortest.NewNopSettings()

	// Create metrics consumer
	metricsConsumer := consumertest.NewNop()

	// Create connector
	conn, err := factory.CreateMetricsToMetrics(ctx, set, cfg, metricsConsumer)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Type assertion to check internal state
	alertConn, ok := conn.(*alertsConnector)
	require.True(t, ok)
	assert.Equal(t, metricsConsumer, alertConn.nextMetrics)
	assert.Nil(t, alertConn.nextLogs)
	assert.Nil(t, alertConn.nextTraces)
}

func TestCreateWithInvalidConfig(t *testing.T) {
	factory := NewFactory()
	ctx := context.Background()
	set := connectortest.NewNopSettings()

	// Invalid config (missing TSDB)
	cfg := &Config{
		WindowSize: 5 * time.Second,
	}

	// Should fail for all pipeline types
	t.Run("traces_to_logs", func(t *testing.T) {
		logsConsumer := consumertest.NewNop()
		conn, err := factory.CreateTracesToLogs(ctx, set, cfg, logsConsumer)
		assert.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("metrics_to_logs", func(t *testing.T) {
		logsConsumer := consumertest.NewNop()
		conn, err := factory.CreateMetricsToLogs(ctx, set, cfg, logsConsumer)
		assert.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("metrics_to_metrics", func(t *testing.T) {
		metricsConsumer := consumertest.NewNop()
		conn, err := factory.CreateMetricsToMetrics(ctx, set, cfg, metricsConsumer)
		assert.Error(t, err)
		assert.Nil(t, conn)
	})
}

func TestFactoryType(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, component.MustNewType("alertsgen"), factory.Type())
}

func TestFactoryCapabilities(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	ctx := context.Background()
	set := connectortest.NewNopSettings()

	// Create a connector
	conn, err := factory.CreateTracesToLogs(ctx, set, cfg, consumertest.NewNop())
	require.NoError(t, err)

	// Check capabilities
	capabilities := conn.Capabilities()
	assert.False(t, capabilities.MutatesData)
}
