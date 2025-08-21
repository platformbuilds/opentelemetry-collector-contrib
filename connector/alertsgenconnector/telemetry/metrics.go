package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Metrics struct {
	// Core alerting metrics
	evalTotal     metric.Int64Counter
	evalDuration  metric.Float64Histogram
	eventsEmitted metric.Int64Counter
	notifyTotal   metric.Int64Counter
	activeGauge   metric.Int64UpDownCounter
	droppedTotal  metric.Int64Counter

	// Memory management metrics
	memoryUsageBytes   metric.Int64Gauge
	memoryUsagePercent metric.Float64Gauge
	droppedDataCounter metric.Int64Counter
	scaleEventsCounter metric.Int64Counter

	// Buffer utilization metrics
	bufferUtilization metric.Int64Gauge
}

func New(mp metric.MeterProvider) (*Metrics, error) {
	meter := mp.Meter("alertsgenconnector")

	evalTotal, err := meter.Int64Counter("otel_alert_evaluations_total",
		metric.WithDescription("Total number of rule evaluation passes"))
	if err != nil {
		return nil, err
	}

	evalDuration, err := meter.Float64Histogram("otel_alert_evaluation_duration_seconds",
		metric.WithDescription("Wall time of a rule evaluation pass in seconds"))
	if err != nil {
		return nil, err
	}

	eventsEmitted, err := meter.Int64Counter("otel_alert_events_emitted_total",
		metric.WithDescription("Number of alert events emitted"))
	if err != nil {
		return nil, err
	}

	notifyTotal, err := meter.Int64Counter("otel_alert_notifications_total",
		metric.WithDescription("Number of notification batches sent"))
	if err != nil {
		return nil, err
	}

	activeGauge, err := meter.Int64UpDownCounter("otel_alert_active_total",
		metric.WithDescription("Active firing alerts (up/down)"))
	if err != nil {
		return nil, err
	}

	droppedTotal, err := meter.Int64Counter("otel_alert_dropped_total",
		metric.WithDescription("Alerts dropped by limiter/dedup"))
	if err != nil {
		return nil, err
	}

	// Memory management metrics
	memoryUsageBytes, err := meter.Int64Gauge("otel_alert_memory_usage_bytes",
		metric.WithDescription("Current memory usage in bytes"))
	if err != nil {
		return nil, err
	}

	memoryUsagePercent, err := meter.Float64Gauge("otel_alert_memory_usage_percent",
		metric.WithDescription("Current memory usage as percentage of limit"))
	if err != nil {
		return nil, err
	}

	droppedDataCounter, err := meter.Int64Counter("otel_alert_data_dropped_total",
		metric.WithDescription("Data dropped due to memory pressure or limits"))
	if err != nil {
		return nil, err
	}

	scaleEventsCounter, err := meter.Int64Counter("otel_alert_scale_events_total",
		metric.WithDescription("Buffer scaling events"))
	if err != nil {
		return nil, err
	}

	bufferUtilization, err := meter.Int64Gauge("otel_alert_buffer_utilization",
		metric.WithDescription("Buffer utilization by signal type"))
	if err != nil {
		return nil, err
	}

	return &Metrics{
		evalTotal:          evalTotal,
		evalDuration:       evalDuration,
		eventsEmitted:      eventsEmitted,
		notifyTotal:        notifyTotal,
		activeGauge:        activeGauge,
		droppedTotal:       droppedTotal,
		memoryUsageBytes:   memoryUsageBytes,
		memoryUsagePercent: memoryUsagePercent,
		droppedDataCounter: droppedDataCounter,
		scaleEventsCounter: scaleEventsCounter,
		bufferUtilization:  bufferUtilization,
	}, nil
}

// Core alerting methods
func (m *Metrics) RecordEvaluation(ctx context.Context, _ string, _ string, dur time.Duration) {
	if m == nil {
		return
	}
	m.evalTotal.Add(ctx, 1)
	m.evalDuration.Record(ctx, dur.Seconds())
}

func (m *Metrics) RecordEvents(ctx context.Context, n int, _ string, _ string) {
	if m == nil || n <= 0 {
		return
	}
	m.eventsEmitted.Add(ctx, int64(n))
}

func (m *Metrics) AddActive(ctx context.Context, delta int, _ string, _ string) {
	if m == nil || delta == 0 {
		return
	}
	m.activeGauge.Add(ctx, int64(delta))
}

func (m *Metrics) RecordNotify(ctx context.Context, _ bool) {
	if m == nil {
		return
	}
	m.notifyTotal.Add(ctx, 1)
}

func (m *Metrics) RecordDropped(ctx context.Context, n int, _ string) {
	if m == nil || n <= 0 {
		return
	}
	m.droppedTotal.Add(ctx, int64(n))
}

// Memory management methods
func (m *Metrics) RecordMemoryUsage(ctx context.Context, current, max int64, percent float64) {
	if m == nil {
		return
	}
	m.memoryUsageBytes.Record(ctx, current)
	m.memoryUsagePercent.Record(ctx, percent)
}

func (m *Metrics) RecordBufferSizes(ctx context.Context, traces, logs, metrics int) {
	if m == nil {
		return
	}

	// Record buffer utilization for each signal type
	m.bufferUtilization.Record(ctx, int64(traces), metric.WithAttributeSet(attribute.NewSet(
		attribute.String("signal_type", "traces"),
	)))

	m.bufferUtilization.Record(ctx, int64(logs), metric.WithAttributeSet(attribute.NewSet(
		attribute.String("signal_type", "logs"),
	)))

	m.bufferUtilization.Record(ctx, int64(metrics), metric.WithAttributeSet(attribute.NewSet(
		attribute.String("signal_type", "metrics"),
	)))
}

func (m *Metrics) RecordDroppedData(ctx context.Context, droppedTraces, droppedLogs, droppedMetrics int64) {
	if m == nil {
		return
	}

	if droppedTraces > 0 {
		m.droppedDataCounter.Add(ctx, droppedTraces, metric.WithAttributeSet(attribute.NewSet(
			attribute.String("signal_type", "traces"),
			attribute.String("reason", "memory_pressure"),
		)))
	}

	if droppedLogs > 0 {
		m.droppedDataCounter.Add(ctx, droppedLogs, metric.WithAttributeSet(attribute.NewSet(
			attribute.String("signal_type", "logs"),
			attribute.String("reason", "memory_pressure"),
		)))
	}

	if droppedMetrics > 0 {
		m.droppedDataCounter.Add(ctx, droppedMetrics, metric.WithAttributeSet(attribute.NewSet(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "memory_pressure"),
		)))
	}
}

func (m *Metrics) RecordScaleEvent(ctx context.Context, eventType string, scaleFactor float64) {
	if m == nil {
		return
	}

	m.scaleEventsCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("event_type", eventType), // "scale_up", "scale_down", "memory_pressure"
		attribute.Float64("scale_factor", scaleFactor),
	)))
}

func New(mp metric.MeterProvider) (*Metrics, error) {
	meter := mp.Meter("alertsgenconnector")

	evalTotal, err := meter.Int64Counter("otel_alert_evaluations_total",
		metric.WithDescription("Total number of rule evaluation passes"))
	if err != nil {
		return nil, err
	}

	evalDuration, err := meter.Float64Histogram("otel_alert_evaluation_duration_seconds",
		metric.WithDescription("Wall time of a rule evaluation pass in seconds"))
	if err != nil {
		return nil, err
	}

	eventsEmitted, err := meter.Int64Counter("otel_alert_events_emitted_total",
		metric.WithDescription("Number of alert events emitted"))
	if err != nil {
		return nil, err
	}

	notifyTotal, err := meter.Int64Counter("otel_alert_notifications_total",
		metric.WithDescription("Number of notification batches sent"))
	if err != nil {
		return nil, err
	}

	activeGauge, err := meter.Int64UpDownCounter("otel_alert_active_total",
		metric.WithDescription("Active firing alerts (up/down)"))
	if err != nil {
		return nil, err
	}

	droppedTotal, err := meter.Int64Counter("otel_alert_dropped_total",
		metric.WithDescription("Alerts dropped by limiter/dedup"))
	if err != nil {
		return nil, err
	}

	// Memory management metrics
	memoryUsageBytes, err := meter.Int64UpDownCounter("otel_alert_memory_usage_bytes",
		metric.WithDescription("Current memory usage in bytes"))
	if err != nil {
		return nil, err
	}

	memoryUsagePercent, err := meter.Float64Gauge("otel_alert_memory_usage_percent",
		metric.WithDescription("Current memory usage as percentage of limit"))
	if err != nil {
		return nil, err
	}

	bufferSizesGauge, err := meter.Int64UpDownCounter("otel_alert_buffer_size",
		metric.WithDescription("Current buffer sizes by signal type"))
	if err != nil {
		return nil, err
	}

	droppedDataCounter, err := meter.Int64Counter("otel_alert_data_dropped_total",
		metric.WithDescription("Data dropped due to memory pressure or limits"))
	if err != nil {
		return nil, err
	}

	scaleEventsCounter, err := meter.Int64Counter("otel_alert_scale_events_total",
		metric.WithDescription("Buffer scaling events"))
	if err != nil {
		return nil, err
	}

	tracesBuffered, err := meter.Int64UpDownCounter("otel_alert_traces_buffered",
		metric.WithDescription("Number of traces currently buffered"))
	if err != nil {
		return nil, err
	}

	logsBuffered, err := meter.Int64UpDownCounter("otel_alert_logs_buffered",
		metric.WithDescription("Number of logs currently buffered"))
	if err != nil {
		return nil, err
	}

	metricsBuffered, err := meter.Int64UpDownCounter("otel_alert_metrics_buffered",
		metric.WithDescription("Number of metrics currently buffered"))
	if err != nil {
		return nil, err
	}

	return &Metrics{
		evalTotal:          evalTotal,
		evalDuration:       evalDuration,
		eventsEmitted:      eventsEmitted,
		notifyTotal:        notifyTotal,
		activeGauge:        activeGauge,
		droppedTotal:       droppedTotal,
		memoryUsageBytes:   memoryUsageBytes,
		memoryUsagePercent: memoryUsagePercent,
		bufferSizesGauge:   bufferSizesGauge,
		droppedDataCounter: droppedDataCounter,
		scaleEventsCounter: scaleEventsCounter,
		tracesBuffered:     tracesBuffered,
		logsBuffered:       logsBuffered,
		metricsBuffered:    metricsBuffered,
	}, nil
}

func (m *Metrics) RecordBufferUtilization(ctx context.Context, signalType string, current, capacity int, utilizationPercent float64) {
	if m == nil {
		return
	}

	m.bufferSizesGauge.Add(ctx, int64(current), metric.WithAttributes(
		metric.String("signal_type", signalType),
		metric.String("metric_type", "current"),
	))

	m.bufferSizesGauge.Add(ctx, int64(capacity), metric.WithAttributes(
		metric.String("signal_type", signalType),
		metric.String("metric_type", "capacity"),
	))
}

// Helper methods to track current values (in practice, these would be stored as state)
func (m *Metrics) getCurrentMemoryUsage() int64 {
	// This would be stored as internal state in a real implementation
	return 0
}

func (m *Metrics) getCurrentTracesBuffered() int {
	// This would be stored as internal state in a real implementation
	return 0
}

func (m *Metrics) getCurrentLogsBuffered() int {
	// This would be stored as internal state in a real implementation
	return 0
}

func (m *Metrics) getCurrentMetricsBuffered() int {
	// This would be stored as internal state in a real implementation
	return 0
}
