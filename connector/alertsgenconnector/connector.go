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

	// Batching for remote write
	eventBatch   []state.AlertEvent
	eventBatchMu sync.Mutex
	batchTicker  *time.Ticker
	flushChan    chan struct{}

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
	if c.TSDB != nil && c.TSDB.QueryURL != "" {
		tsdbCfg := state.TSDBConfig{
			QueryURL:       c.TSDB.QueryURL,
			RemoteWriteURL: c.TSDB.RemoteWriteURL,
			QueryTimeout:   c.TSDB.QueryTimeout,
			WriteTimeout:   c.TSDB.WriteTimeout,
			DedupWindow:    c.TSDB.DedupWindow,
			InstanceID:     c.InstanceID,
		}

		ts, err = state.NewTSDBSyncer(tsdbCfg)
		if err != nil {
			set.Logger.Warn("Failed to create TSDB syncer", zap.Error(err))
		} else {
			// Restore state from TSDB on startup
			if err := rs.restoreFromTSDB(ts); err != nil {
				set.Logger.Warn("Failed to restore state from TSDB", zap.Error(err))
			} else {
				set.Logger.Info("Successfully restored alert state from TSDB")
			}
		}
	}

	connector := &alertsConnector{
		cfg:        c,
		logger:     set.Logger,
		rs:         rs,
		ing:        ing,
		mx:         mx,
		tsdb:       ts,
		eventBatch: make([]state.AlertEvent, 0, c.TSDB.RemoteWriteBatchSize),
		flushChan:  make(chan struct{}, 1),
		evalStop:   make(chan struct{}),
	}

	// Start batch flushing if remote write is enabled
	if ts != nil && c.TSDB.EnableRemoteWrite {
		connector.batchTicker = time.NewTicker(c.TSDB.RemoteWriteFlushInterval)
	}

	return connector, nil
}

// ---- lifecycle --------------------------------------------------------------

func (e *alertsConnector) Start(ctx context.Context, _ component.Host) error {
	e.logger.Info("Starting alerts connector",
		zap.String("instance_id", e.cfg.InstanceID),
		zap.Duration("window_size", e.cfg.WindowSize),
		zap.Int("num_rules", len(e.cfg.Rules)),
	)

	interval := e.cfg.Step
	if interval <= 0 {
		interval = e.cfg.WindowSize
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}

	// Start evaluation goroutine
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

	// Start batch flushing goroutine if enabled
	if e.batchTicker != nil {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			defer e.batchTicker.Stop()

			for {
				select {
				case <-e.batchTicker.C:
					e.flushEventBatch()
				case <-e.flushChan:
					e.flushEventBatch()
				case <-e.evalStop:
					// Final flush before shutdown
					e.flushEventBatch()
					return
				case <-ctx.Done():
					e.flushEventBatch()
					return
				}
			}
		}()
	}

	return nil
}

func (e *alertsConnector) Shutdown(ctx context.Context) error {
	e.logger.Info("Shutting down alerts connector")
	close(e.evalStop)

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.logger.Info("Alerts connector shutdown complete")
	case <-ctx.Done():
		e.logger.Warn("Alerts connector shutdown timed out")
	}

	return nil
}

func (e *alertsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ---- evaluation -------------------------------------------------------------

func (e *alertsConnector) evaluateOnce(now time.Time) {
	e.evalMu.Lock()
	defer e.evalMu.Unlock()

	start := time.Now()
	events, metrics := e.rs.evaluate(now, e.ing)
	evalDuration := time.Since(start)

	if len(events) > 0 {
		e.logger.Debug("Generated alert events",
			zap.Int("count", len(events)),
			zap.Duration("eval_duration", evalDuration),
		)

		// Convert to TSDB events and add to batch
		tsdbEvents := e.convertToTSDBEvents(events, now)
		e.addEventsToBatch(tsdbEvents)

		// Self-telemetry
		if e.mx != nil {
			e.mx.RecordEvents(context.Background(), len(events), "all", "n/a")
		}
	}

	// Update active alert metrics
	if e.mx != nil && len(metrics) > 0 {
		for _, metric := range metrics {
			if metric.Active > 0 {
				e.mx.AddActive(context.Background(), 1, metric.Rule, metric.Severity)
			}
		}
	}

	// Record evaluation metrics
	if e.mx != nil {
		e.mx.RecordEvaluation(context.Background(), "all", "success", evalDuration)
	}
}

// convertToTSDBEvents converts internal alertEvent to state.AlertEvent
func (e *alertsConnector) convertToTSDBEvents(events []alertEvent, timestamp time.Time) []state.AlertEvent {
	tsdbEvents := make([]state.AlertEvent, 0, len(events))

	for _, event := range events {
		// Calculate fingerprint
		fp := fingerprint(event.Rule, event.Labels)

		tsdbEvent := state.AlertEvent{
			Rule:        event.Rule,
			State:       event.State,
			Severity:    event.Severity,
			Labels:      event.Labels,
			Value:       event.Value,
			Window:      event.Window,
			For:         event.For,
			Timestamp:   timestamp,
			Fingerprint: fp,
		}

		tsdbEvents = append(tsdbEvents, tsdbEvent)
	}

	return tsdbEvents
}

// addEventsToBatch adds events to the batch and triggers flush if needed
func (e *alertsConnector) addEventsToBatch(events []state.AlertEvent) {
	if e.tsdb == nil || !e.cfg.TSDB.EnableRemoteWrite {
		return
	}

	e.eventBatchMu.Lock()
	defer e.eventBatchMu.Unlock()

	e.eventBatch = append(e.eventBatch, events...)

	// Trigger immediate flush if batch is full
	if len(e.eventBatch) >= e.cfg.TSDB.RemoteWriteBatchSize {
		select {
		case e.flushChan <- struct{}{}:
		default:
			// Channel is full, flush will happen on next timer
		}
	}
}

// flushEventBatch sends accumulated events to TSDB
func (e *alertsConnector) flushEventBatch() {
	if e.tsdb == nil || !e.cfg.TSDB.EnableRemoteWrite {
		return
	}

	e.eventBatchMu.Lock()
	if len(e.eventBatch) == 0 {
		e.eventBatchMu.Unlock()
		return
	}

	// Copy and reset batch
	eventsToFlush := make([]state.AlertEvent, len(e.eventBatch))
	copy(eventsToFlush, e.eventBatch)
	e.eventBatch = e.eventBatch[:0] // Reset slice but keep capacity
	e.eventBatchMu.Unlock()

	// Convert to interface{} slice for PublishEvents
	interfaceEvents := make([]interface{}, len(eventsToFlush))
	for i, event := range eventsToFlush {
		interfaceEvents[i] = event
	}

	// Publish to TSDB
	if err := e.tsdb.PublishEvents(interfaceEvents); err != nil {
		e.logger.Warn("Failed to publish events to TSDB",
			zap.Error(err),
			zap.Int("event_count", len(eventsToFlush)),
		)
		// Record failed events for telemetry
		if e.mx != nil {
			e.mx.RecordDropped(context.Background(), len(eventsToFlush), "tsdb_publish_failed")
		}
	} else {
		e.logger.Debug("Successfully published events to TSDB",
			zap.Int("event_count", len(eventsToFlush)),
		)
	}
}

// ---- Consume methods ---------------------------------------

func (e *alertsConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// Ingest into sliding window
	if err := e.ing.consumeTraces(td); err != nil {
		e.logger.Error("Failed to ingest traces", zap.Error(err))
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
		e.logger.Error("Failed to ingest logs", zap.Error(err))
		return err
	}
	if e.nextLogs != nil {
		return e.nextLogs.ConsumeLogs(ctx, ld)
	}
	return nil
}

func (e *alertsConnector) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if err := e.ing.consumeMetrics(md); err != nil {
		e.logger.Error("Failed to ingest metrics", zap.Error(err))
		return err
	}
	if e.nextMetrics != nil {
		return e.nextMetrics.ConsumeMetrics(ctx, md)
	}
	return nil
}
