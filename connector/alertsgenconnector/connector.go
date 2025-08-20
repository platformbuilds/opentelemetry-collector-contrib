package alertsgenconnector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/state"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/telemetry"
)

// alertsConnector evaluates streaming telemetry against rules and forwards the
// original data to the next consumer (Traces->Traces, Logs->Logs, Metrics->Metrics).
type alertsConnector struct {
	cfg  *Config
	rs   *ruleSet
	ing  *ingester
	mx   *telemetry.Metrics
	tsdb *state.TSDBSyncer // optional; nil if HA/TSDB is not configured

	// downstreams (only one of these will be set depending on factory used)
	nextTraces  consumer.Traces
	nextLogs    consumer.Logs
	nextMetrics consumer.Metrics

	evalMu   sync.Mutex
	evalStop chan struct{}
}

// ---- factory helpers --------------------------------------------------------

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

func newAlertsConnector(_ context.Context, set connector.Settings, cfg component.Config) (*alertsConnector, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type %T", cfg)
	}

	// compile rules
	rs, err := compileRules(c)
	if err != nil {
		return nil, err
	}

	// self telemetry
	mx, err := telemetry.New(set.MeterProvider)
	if err != nil {
		return nil, err
	}
	rs.mx = mx

	// ingester holds sliding windows / buffers for the last N seconds
	ing := newIngester(c) // assumes you have newIngester(*Config) in your package

	// optional TSDB syncer for HA
	var ts *state.TSDBSyncer
	if c.TSDB != nil {
		ts = state.NewTSDBSyncer(c.TSDB)
		_ = rs.restoreFromTSDB(ts) // best-effort
	}

	return &alertsConnector{
		cfg:      c,
		rs:       rs,
		ing:      ing,
		mx:       mx,
		tsdb:     ts,
		evalStop: make(chan struct{}),
	}, nil
}

// ---- lifecycle --------------------------------------------------------------

func (e *alertsConnector) Start(ctx context.Context, _ component.Host) error {
	// periodic evaluation loop
	interval := e.cfg.Step
	if interval <= 0 {
		interval = e.cfg.WindowSize
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}

	t := time.NewTicker(interval)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				e.evaluateOnce(time.Now())
			case <-e.evalStop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (e *alertsConnector) Shutdown(context.Context) error {
	close(e.evalStop)
	return nil
}

func (e *alertsConnector) Capabilities() consumer.Capabilities {
	// We pass through original data to the next consumer.
	return consumer.Capabilities{MutatesData: false}
}

// ---- evaluation -------------------------------------------------------------

func (e *alertsConnector) evaluateOnce(now time.Time) {
	e.evalMu.Lock()
	defer e.evalMu.Unlock()

	events, _ := e.rs.evaluate(now, e.ing)

	// Persist alert state to TSDB for HA dedup/continuity (best-effort).
	if e.tsdb != nil && len(events) > 0 {
		_ = e.tsdb.Publish(events)
	}
	// Self-telemetry: number of events produced in this tick.
	if e.mx != nil && len(events) > 0 {
		e.mx.RecordEvents(context.Background(), len(events), "all", "n/a")
	}
}

// ---- Consume (Traces, Logs, Metrics) ---------------------------------------

func (e *alertsConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// Ingest into sliding window
	if err := ingestTraces(ctx, td, e.ing); err != nil {
		return err
	}
	// Forward downstream unchanged
	if e.nextTraces != nil && !td.ResourceSpans().IsEmpty() {
		return e.nextTraces.ConsumeTraces(ctx, td)
	}
	return nil
}

func (e *alertsConnector) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if err := ingestLogs(ctx, ld, e.ing); err != nil {
		return err
	}
	if e.nextLogs != nil && !ld.ResourceLogs().IsEmpty() {
		return e.nextLogs.ConsumeLogs(ctx, ld)
	}
	return nil
}

func (e *alertsConnector) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if err := ingestMetrics(ctx, md, e.ing); err != nil {
		return err
	}
	if e.nextMetrics != nil && !md.ResourceMetrics().IsEmpty() {
		return e.nextMetrics.ConsumeMetrics(ctx, md)
	}
	return nil
}
