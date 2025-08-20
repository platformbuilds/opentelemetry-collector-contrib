package alertsgenconnector

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
		createDefaultConfig,
		connector.WithTracesToTraces(createTracesToTraces, component.StabilityLevelAlpha),
		connector.WithLogsToLogs(createLogsToLogs, component.StabilityLevelAlpha),
		connector.WithMetricsToMetrics(createMetricsToMetrics, component.StabilityLevelAlpha),
	)
}

func createTracesToMetrics(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (connector.Traces, error) {
	return newConnector(set, cfg.(*Config), next)
}

func createLogsToMetrics(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (connector.Logs, error) {
	return newConnector(set, cfg.(*Config), next)
}

func createMetricsToMetrics(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (connector.Metrics, error) {
	return newConnector(set, cfg.(*Config), next)
}
