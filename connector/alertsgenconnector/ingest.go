package alertsgenconnector

import (
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type ingester struct {
	mu      sync.Mutex
	cfg     *Config
	traces  []traceRow
	logs    []logRow
	metrics []metricRow
}

type traceRow struct {
	ts         time.Time
	attrs      map[string]string
	durationNs float64
	statusCode string
}

type logRow struct {
	ts       time.Time
	attrs    map[string]string
	body     string
	severity string
}

type metricRow struct {
	ts    time.Time
	attrs map[string]string
	name  string
	value float64
}

func newIngester(cfg *Config) *ingester {
	return &ingester{
		cfg:     cfg,
		traces:  make([]traceRow, 0, 10000),
		logs:    make([]logRow, 0, 10000),
		metrics: make([]metricRow, 0, 10000),
	}
}

func (i *ingester) drain() ([]traceRow, []logRow, []metricRow) {
	i.mu.Lock()
	defer i.mu.Unlock()

	tr := i.traces
	lg := i.logs
	mt := i.metrics

	// Reset slices
	i.traces = make([]traceRow, 0, 10000)
	i.logs = make([]logRow, 0, 10000)
	i.metrics = make([]metricRow, 0, 10000)

	return tr, lg, mt
}

func (i *ingester) consumeTraces(td ptrace.Traces) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	for r := 0; r < td.ResourceSpans().Len(); r++ {
		rs := td.ResourceSpans().At(r)
		rAttrs := extractAttrs(rs.Resource().Attributes())

		for s := 0; s < rs.ScopeSpans().Len(); s++ {
			spans := rs.ScopeSpans().At(s).Spans()
			for j := 0; j < spans.Len(); j++ {
				span := spans.At(j)
				attrs := mergeAttrs(rAttrs, extractAttrs(span.Attributes()))

				i.traces = append(i.traces, traceRow{
					ts:         span.EndTimestamp().AsTime(),
					attrs:      attrs,
					durationNs: float64(span.EndTimestamp() - span.StartTimestamp()),
					statusCode: span.Status().Code().String(),
				})
			}
		}
	}

	// Trim to window size if needed
	if len(i.traces) > 100000 {
		i.traces = i.traces[len(i.traces)-100000:]
	}

	return nil
}

func (i *ingester) consumeLogs(ld plog.Logs) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	for r := 0; r < ld.ResourceLogs().Len(); r++ {
		rl := ld.ResourceLogs().At(r)
		rAttrs := extractAttrs(rl.Resource().Attributes())

		for s := 0; s < rl.ScopeLogs().Len(); s++ {
			logs := rl.ScopeLogs().At(s).LogRecords()
			for j := 0; j < logs.Len(); j++ {
				lr := logs.At(j)
				attrs := mergeAttrs(rAttrs, extractAttrs(lr.Attributes()))

				i.logs = append(i.logs, logRow{
					ts:       lr.Timestamp().AsTime(),
					attrs:    attrs,
					body:     lr.Body().AsString(),
					severity: lr.SeverityText(),
				})
			}
		}
	}

	if len(i.logs) > 100000 {
		i.logs = i.logs[len(i.logs)-100000:]
	}

	return nil
}

func (i *ingester) consumeMetrics(md pmetric.Metrics) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	for r := 0; r < md.ResourceMetrics().Len(); r++ {
		rm := md.ResourceMetrics().At(r)
		rAttrs := extractAttrs(rm.Resource().Attributes())

		for s := 0; s < rm.ScopeMetrics().Len(); s++ {
			metrics := rm.ScopeMetrics().At(s).Metrics()
			for j := 0; j < metrics.Len(); j++ {
				metric := metrics.At(j)

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()
					for k := 0; k < dps.Len(); k++ {
						dp := dps.At(k)
						attrs := mergeAttrs(rAttrs, extractAttrs(dp.Attributes()))

						i.metrics = append(i.metrics, metricRow{
							ts:    dp.Timestamp().AsTime(),
							attrs: attrs,
							name:  metric.Name(),
							value: dp.DoubleValue(),
						})
					}

				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for k := 0; k < dps.Len(); k++ {
						dp := dps.At(k)
						attrs := mergeAttrs(rAttrs, extractAttrs(dp.Attributes()))

						i.metrics = append(i.metrics, metricRow{
							ts:    dp.Timestamp().AsTime(),
							attrs: attrs,
							name:  metric.Name(),
							value: dp.DoubleValue(),
						})
					}
				}
			}
		}
	}

	if len(i.metrics) > 100000 {
		i.metrics = i.metrics[len(i.metrics)-100000:]
	}

	return nil
}

func extractAttrs(attrs pcommon.Map) map[string]string {
	m := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}

func mergeAttrs(a, b map[string]string) map[string]string {
	result := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}
