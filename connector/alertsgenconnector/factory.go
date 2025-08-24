// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const typeStr = "alertsgen"

// NewFactory wires up the connector to support the pipeline pairs exercised by
// generated_component_test: metrics->logs and traces->logs. We also keep
// metrics->metrics if your code already had it.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		component.MustNewType(typeStr),
		CreateDefaultConfig,
		connector.WithTracesToLogs(createTracesToLogs, component.StabilityLevelAlpha),
		connector.WithMetricsToLogs(createMetricsToLogs, component.StabilityLevelAlpha),
		connector.WithMetricsToMetrics(createMetricsToMetrics, component.StabilityLevelAlpha),
	)
}

// createTracesToLogs constructs the connector for a traces->logs pipeline.
func createTracesToLogs(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Logs,
) (connector.Traces, error) {
	ac, err := newAlertsConnector(ctx, set, cfg)
	if err != nil {
		return nil, err
	}
	// Validate required dependencies for this connector.
	if ac.tsdb == nil {
		return nil, fmt.Errorf("invalid config: TSDB is not configured")
	}
	// The alerts connector fans-in to logs; set the downstream logs consumer.
	ac.nextLogs = next
	return ac, nil
}

// createMetricsToLogs constructs the connector for a metrics->logs pipeline.
func createMetricsToLogs(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Logs,
) (connector.Metrics, error) {
	ac, err := newAlertsConnector(ctx, set, cfg)
	if err != nil {
		return nil, err
	}
	if ac.tsdb == nil {
		return nil, fmt.Errorf("invalid config: TSDB is not configured")
	}
	ac.nextLogs = next
	return ac, nil
}

// If you already had metrics->metrics support, keep it as-is. Leaving here so
// existing code paths keep working.
func createMetricsToMetrics(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (connector.Metrics, error) {
	ac, err := newAlertsConnector(ctx, set, cfg)
	if err != nil {
		return nil, err
	}
	if ac.tsdb == nil {
		return nil, fmt.Errorf("invalid config: TSDB is not configured")
	}
	ac.nextMetrics = next
	return ac, nil
}
