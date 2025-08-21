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
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/state"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/telemetry"
)

// alertsConnector evaluates streaming telemetry against rules and forwards the
// original data to the next consumer (Traces->Traces, Logs->Logs, Metrics->Metrics).
type alertsConnector struct {
	cfg    *Config
	logger *zap.Logger
	rs     *ruleSet
	ing    *ingester
	mx     *telemetry.Metrics
	tsdb   *state.TSDBSyncer // optional; nil if HA/TSDB is not configured

	// downstreams (only one of these will be set depending on factory used)
	nextTraces  consumer.Traces
	nextLogs    consumer.Logs
	nextMetrics consumer.Metrics

	evalMu   sync.Mutex
	evalStop chan struct{}
	wg       sync.WaitGroup
}

func newAlertsConnector(ctx context.Context, set connector.Settings, cfg component.Config) (*alertsConnector, error) {
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
		set.Logger.Warn("Failed to create telemetry", zap.Error(err))
	}
	rs.mx = mx

	// ingester holds sliding windows / buffers for the last N seconds
	ing := newIngester(c)

	// optional TSDB syncer for HA
	var ts *state.TSDBSyncer
	if c.HA != nil && c.HA.TSDBReadURL != "" {
		ts, err = state.NewTSDBSyncer(c.HA.TSDBReadURL, c.HA.QueryTimeout, c.HA.DedupWindow)
		if err != nil {
			set.Logger.Warn("Failed to create TSDB syncer", zap.Error(err))
		} else {
			_ = rs.restoreFromTSDB(ts)
		}
	}

	return &alertsConnector{
		cfg:      c,
		logger:   set.Logger,
		rs:       rs,
		ing:      ing,
		mx:       mx,
		tsdb:     ts,
		evalStop: make(chan struct{}),
	}, nil
}

// ---- lifecycle --------------------------------------------------------------

func (e *alertsConnector) Start(ctx context.Context, _ component.Host) error {
	e.logger.Info("Starting alerts connector")

	interval := e.cfg.Step
	if interval <= 0 {
		interval = e.cfg.WindowSize
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
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

func (e *alertsConnector) Shutdown(ctx context.Context) error {
	e.logger.Info("Shutting down alerts connector")
	close(e.evalStop)
	e.wg.Wait()
	return nil
}

func (e *alertsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ---- evaluation -------------------------------------------------------------

func (e *alertsConnector) evaluateOnce(now time.Time) {
	e.evalMu.Lock()
	defer e.evalMu.Unlock()

	events, _ := e.rs.evaluate(now, e.ing)

	// Persist alert state to TSDB for HA dedup/continuity
	if e.tsdb != nil && len(events) > 0 {
		// Direct conversion using append with spread operator
		interfaceEvents := make([]interface{}, 0, len(events))
		for _, event := range events {
			interfaceEvents = append(interfaceEvents, event)
		}
		if err := e.tsdb.PublishEvents(interfaceEvents); err != nil {
			e.logger.Warn("Failed to publish events to TSDB", zap.Error(err))
		}
	}

	// Self-telemetry
	if e.mx != nil && len(events) > 0 {
		e.mx.RecordEvents(context.Background(), len(events), "all", "n/a")
	}
}

// ---- Consume methods ---------------------------------------

func (e *alertsConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// Ingest into sliding window
	if err := e.ing.consumeTraces(td); err != nil {
		return err
	}
	// Forward downstream unchanged
	if e.nextTraces != nil {
		return e.nextTraces.ConsumeTraces(ctx, td)
	}
	return nil
}

func (e *alertsConnector) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if err := e.ing.consumeLogs(ld); err != nil {
		return err
	}
	if e.nextLogs != nil {
		return e.nextLogs.ConsumeLogs(ctx, ld)
	}
	return nil
}

func (e *alertsConnector) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if err := e.ing.consumeMetrics(md); err != nil {
		return err
	}
	if e.nextMetrics != nil {
		return e.nextMetrics.ConsumeMetrics(ctx, md)
	}
	return nil
}
