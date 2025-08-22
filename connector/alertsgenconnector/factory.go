// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const typeStr = "alertsgen"

func NewFactory() connector.Factory {
	return connector.NewFactory(
		component.MustNewType(typeStr),
		CreateDefaultConfig,
		connector.WithTracesToTraces(createTracesToTraces, component.StabilityLevelAlpha),
		connector.WithLogsToLogs(createLogsToLogs, component.StabilityLevelAlpha),
		connector.WithMetricsToMetrics(createMetricsToMetrics, component.StabilityLevelAlpha),
	)
}

func createTracesToTraces(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Traces,
) (connector.Traces, error) {
	ac, err := newAlertsConnector(ctx, set, cfg)
	if err != nil {
		return nil, err
	}
	ac.nextTraces = next
	return ac, nil
}

func createLogsToLogs(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Logs,
) (connector.Logs, error) {
	ac, err := newAlertsConnector(ctx, set, cfg)
	if err != nil {
		return nil, err
	}
	ac.nextLogs = next
	return ac, nil
}

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
	ac.nextMetrics = next
	return ac, nil
}
