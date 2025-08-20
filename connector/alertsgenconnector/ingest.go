package alertsgenconnector

import (
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

/***************
 * Row models  *
 ***************/

type traceRow struct {
	ts         time.Time
	durationNs float64
	attrs      map[string]string
}

type logRow struct {
	ts    time.Time
	attrs map[string]string
}

type metricRow struct {
	ts    time.Time
	value float64
	attrs map[string]string
}

/******************
 * Helper utils   *
 ******************/

// addAllAttrs copies all entries from a pcommon.Map into dst (stringified).
func addAllAttrs(dst map[string]string, src pcommon.Map) {
	src.Range(func(k string, v pcommon.Value) bool {
		// Stringify consistently; adjust if you need richer typing.
		dst[k] = v.AsString()
		return true
	})
}

func cloneAttrs(base map[string]string) map[string]string {
	out := make(map[string]string, len(base))
	for k, v := range base {
		out[k] = v
	}
	return out
}

/******************
 * Ingest buffer  *
 ******************/

type ingester struct {
	mu sync.Mutex

	traces  []traceRow
	logs    []logRow
	metrics []metricRow
}

func newIngester() *ingester { return &ingester{} }

// drain returns current buffers and clears them.
func (i *ingester) drain() ([]traceRow, []logRow, []metricRow) {
	i.mu.Lock()
	defer i.mu.Unlock()
	tr := i.traces
	lg := i.logs
	mt := i.metrics
	i.traces = nil
	i.logs = nil
	i.metrics = nil
	return tr, lg, mt
}

/**********************
 * Traces ingestion   *
 **********************/

func (i *ingester) addTraces(td ptrace.Traces) {
	resSpans := td.ResourceSpans()
	for rsi := 0; rsi < resSpans.Len(); rsi++ {
		rs := resSpans.At(rsi)
		// Base labels from resource + scope
		base := make(map[string]string, 16)
		addAllAttrs(base, rs.Resource().Attributes())

		scopeSpans := rs.ScopeSpans()
		for ssi := 0; ssi < scopeSpans.Len(); ssi++ {
			ss := scopeSpans.At(ssi)
			scope := ss.Scope()
			if scope.Name() != "" {
				base["otel.scope.name"] = scope.Name()
			}
			if scope.Version() != "" {
				base["otel.scope.version"] = scope.Version()
			}

			spans := ss.Spans()
			for si := 0; si < spans.Len(); si++ {
				sp := spans.At(si)

				lbls := cloneAttrs(base)
				addAllAttrs(lbls, sp.Attributes())

				start := sp.StartTimestamp().AsTime()
				end := sp.EndTimestamp().AsTime()
				dur := end.Sub(start)
				if dur < 0 {
					dur = 0
				}

				i.mu.Lock()
				i.traces = append(i.traces, traceRow{
					ts:         end, // use span end as event time
					durationNs: float64(dur.Nanoseconds()),
					attrs:      lbls,
				})
				i.mu.Unlock()
			}
		}
	}
}

/*******************
 * Logs ingestion  *
 *******************/

func (i *ingester) addLogs(ld plog.Logs) {
	resLogs := ld.ResourceLogs()
	for rli := 0; rli < resLogs.Len(); rli++ {
		rl := resLogs.At(rli)
		base := make(map[string]string, 16)
		addAllAttrs(base, rl.Resource().Attributes())

		scopeLogs := rl.ScopeLogs()
		for sli := 0; sli < scopeLogs.Len(); sli++ {
			sl := scopeLogs.At(sli)
			scope := sl.Scope()
			if scope.Name() != "" {
				base["otel.scope.name"] = scope.Name()
			}
			if scope.Version() != "" {
				base["otel.scope.version"] = scope.Version()
			}

			recs := sl.LogRecords()
			for ri := 0; ri < recs.Len(); ri++ {
				rec := recs.At(ri)
				lbls := cloneAttrs(base)
				addAllAttrs(lbls, rec.Attributes())

				ts := rec.Timestamp().AsTime()
				if ts.IsZero() {
					ts = time.Now()
				}

				i.mu.Lock()
				i.logs = append(i.logs, logRow{
					ts:    ts,
					attrs: lbls,
				})
				i.mu.Unlock()
			}
		}
	}
}

/**********************
 * Metrics ingestion  *
 **********************/

func (i *ingester) addMetrics(md pmetric.Metrics) {
	resMetrics := md.ResourceMetrics()
	for rmi := 0; rmi < resMetrics.Len(); rmi++ {
		rm := resMetrics.At(rmi)
		base := make(map[string]string, 16)
		addAllAttrs(base, rm.Resource().Attributes())

		scopeMetrics := rm.ScopeMetrics()
		for smi := 0; smi < scopeMetrics.Len(); smi++ {
			sm := scopeMetrics.At(smi)
			scope := sm.Scope()
			if scope.Name() != "" {
				base["otel.scope.name"] = scope.Name()
			}
			if scope.Version() != "" {
				base["otel.scope.version"] = scope.Version()
			}

			ms := sm.Metrics()
			for mi := 0; mi < ms.Len(); mi++ {
				m := ms.At(mi)

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for di := 0; di < dps.Len(); di++ {
						dp := dps.At(di)
						lbls := cloneAttrs(base)
						addAllAttrs(lbls, dp.Attributes())
						ts := dp.Timestamp().AsTime()
						val := dp.DoubleValue()
						if val == 0 && dp.IntValue() != 0 {
							val = float64(dp.IntValue())
						}
						i.mu.Lock()
						i.metrics = append(i.metrics, metricRow{
							ts:    ts,
							value: val,
							attrs: lbls,
						})
						i.mu.Unlock()
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for di := 0; di < dps.Len(); di++ {
						dp := dps.At(di)
						lbls := cloneAttrs(base)
						addAllAttrs(lbls, dp.Attributes())
						ts := dp.Timestamp().AsTime()
						val := dp.DoubleValue()
						if val == 0 && dp.IntValue() != 0 {
							val = float64(dp.IntValue())
						}
						i.mu.Lock()
						i.metrics = append(i.metrics, metricRow{
							ts:    ts,
							value: val,
							attrs: lbls,
						})
						i.mu.Unlock()
					}
				// You can extend here to Histogram/Summary if needed
				default:
					// ignore unsupported for now
				}
			}
		}
	}
}
