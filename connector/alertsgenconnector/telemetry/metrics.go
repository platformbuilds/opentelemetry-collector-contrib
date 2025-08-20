package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
)

type Metrics struct {
	evalTotal     metric.Int64Counter
	evalDuration  metric.Float64Histogram
	eventsEmitted metric.Int64Counter
	notifyTotal   metric.Int64Counter
	activeGauge   metric.Int64UpDownCounter
	droppedTotal  metric.Int64Counter
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

	return &Metrics{
		evalTotal:     evalTotal,
		evalDuration:  evalDuration,
		eventsEmitted: eventsEmitted,
		notifyTotal:   notifyTotal,
		activeGauge:   activeGauge,
		droppedTotal:  droppedTotal,
	}, nil
}

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
