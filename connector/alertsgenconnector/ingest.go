// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector"

import (
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// ingester holds sliding windows / buffers with adaptive memory management
type ingester struct {
	mu     sync.RWMutex
	cfg    *Config
	logger *zap.Logger
	memMgr *MemoryManager

	// Data storage
	traces  DataBuffer
	logs    DataBuffer
	metrics DataBuffer

	// Statistics
	traceCount  int64
	logCount    int64
	metricCount int64

	// Memory usage tracking
	estimatedMemoryUsage int64
	lastMemoryCheck      time.Time
}

// MemoryManager handles adaptive memory management
type MemoryManager struct {
	cfg MemoryConfig
	mu  sync.RWMutex

	// Current buffer limits (can be adjusted dynamically)
	currentTraceLimit  int64
	currentLogLimit    int64
	currentMetricLimit int64

	// Memory usage tracking
	currentMemoryUsage int64
	maxMemoryLimit     int64

	// Adaptive scaling state
	lastScaleCheck      time.Time
	scaleHistory        []scaleEvent
	underMemoryPressure bool

	// Statistics for decision making
	stats MemoryStats
}

type scaleEvent struct {
	timestamp time.Time
	factor    float64
	reason    string
}

type MemoryStats struct {
	TotalAllocations     int64
	CurrentAllocations   int64
	DroppedTraces        int64
	DroppedLogs          int64
	DroppedMetrics       int64
	ScaleUpEvents        int64
	ScaleDownEvents      int64
	MemoryPressureEvents int64
}

// DataBuffer interface allows for different buffer implementations
type DataBuffer interface {
	Add(item interface{}) bool
	Drain() []interface{}
	Len() int
	Cap() int
	EstimateMemoryUsage() int64
	Resize(newSize int64)
}

// SliceBuffer implements DataBuffer using a slice
type SliceBuffer struct {
	mu       sync.RWMutex
	data     []interface{}
	maxSize  int64
	itemSize int64 // estimated size per item
}

// RingBuffer implements DataBuffer using a ring buffer
type RingBuffer struct {
	mu        sync.RWMutex
	data      []interface{}
	head      int
	tail      int
	size      int
	maxSize   int64
	itemSize  int64
	overwrite bool
}

// NewSliceBuffer creates a new slice-based buffer
func NewSliceBuffer(maxSize, itemSize int64) *SliceBuffer {
	return &SliceBuffer{
		data:     make([]interface{}, 0, maxSize),
		maxSize:  maxSize,
		itemSize: itemSize,
	}
}

func (sb *SliceBuffer) Add(item interface{}) bool {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if int64(len(sb.data)) >= sb.maxSize {
		return false // Buffer full
	}

	sb.data = append(sb.data, item)
	return true
}

func (sb *SliceBuffer) Drain() []interface{} {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	result := make([]interface{}, len(sb.data))
	copy(result, sb.data)
	sb.data = sb.data[:0] // Reset but keep capacity
	return result
}

func (sb *SliceBuffer) Len() int {
	sb.mu.RLock()
	defer sb.mu.RUnlock()
	return len(sb.data)
}

func (sb *SliceBuffer) Cap() int {
	return int(sb.maxSize)
}

func (sb *SliceBuffer) EstimateMemoryUsage() int64 {
	sb.mu.RLock()
	defer sb.mu.RUnlock()
	return int64(len(sb.data)) * sb.itemSize
}

func (sb *SliceBuffer) Resize(newSize int64) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.maxSize = newSize
	// If current data exceeds new size, trim it
	if int64(len(sb.data)) > newSize {
		sb.data = sb.data[:newSize]
	}
}

// NewRingBuffer creates a new ring buffer
func NewRingBuffer(maxSize, itemSize int64, overwrite bool) *RingBuffer {
	return &RingBuffer{
		data:      make([]interface{}, maxSize),
		maxSize:   maxSize,
		itemSize:  itemSize,
		overwrite: overwrite,
	}
}

func (rb *RingBuffer) Add(item interface{}) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size >= int(rb.maxSize) && !rb.overwrite {
		return false // Buffer full and no overwrite
	}

	rb.data[rb.tail] = item
	rb.tail = (rb.tail + 1) % int(rb.maxSize)

	if rb.size < int(rb.maxSize) {
		rb.size++
	} else if rb.overwrite {
		// Move head forward when overwriting
		rb.head = (rb.head + 1) % int(rb.maxSize)
	}

	return true
}

func (rb *RingBuffer) Drain() []interface{} {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 {
		return nil
	}

	result := make([]interface{}, 0, rb.size)

	for i := 0; i < rb.size; i++ {
		idx := (rb.head + i) % int(rb.maxSize)
		if rb.data[idx] != nil {
			result = append(result, rb.data[idx])
			rb.data[idx] = nil // Clear reference
		}
	}

	rb.head = 0
	rb.tail = 0
	rb.size = 0

	return result
}

func (rb *RingBuffer) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

func (rb *RingBuffer) Cap() int {
	return int(rb.maxSize)
}

func (rb *RingBuffer) EstimateMemoryUsage() int64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return int64(rb.size) * rb.itemSize
}

func (rb *RingBuffer) Resize(newSize int64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if newSize == rb.maxSize {
		return
	}

	// Create new buffer
	newData := make([]interface{}, newSize)

	// Copy existing data
	copySize := rb.size
	if copySize > int(newSize) {
		copySize = int(newSize)
	}

	for i := 0; i < copySize; i++ {
		idx := (rb.head + i) % int(rb.maxSize)
		newData[i] = rb.data[idx]
	}

	rb.data = newData
	rb.maxSize = newSize
	rb.head = 0
	rb.tail = copySize % int(newSize)
	rb.size = copySize
}

// NewMemoryManager creates a new adaptive memory manager
func NewMemoryManager(cfg MemoryConfig) *MemoryManager {
	mm := &MemoryManager{
		cfg:            cfg,
		lastScaleCheck: time.Now(),
		scaleHistory:   make([]scaleEvent, 0, 100), // Keep last 100 events
	}

	mm.initializeLimits()
	return mm
}

// initializeLimits sets up initial buffer limits based on configuration
func (mm *MemoryManager) initializeLimits() {
	// Calculate maximum memory limit
	if mm.cfg.MaxMemoryBytes > 0 {
		mm.maxMemoryLimit = mm.cfg.MaxMemoryBytes
	} else {
		// Auto-detect based on system memory
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		mm.maxMemoryLimit = int64(float64(memStats.Sys) * mm.cfg.MaxMemoryPercent)
	}

	// Set initial buffer limits
	if mm.cfg.MaxTraceEntries > 0 {
		mm.currentTraceLimit = int64(mm.cfg.MaxTraceEntries)
	} else {
		// Auto-calculate based on memory budget (assume 1KB per trace entry)
		mm.currentTraceLimit = mm.maxMemoryLimit / (3 * 1024) // Divide by 3 signals, 1KB each
	}

	if mm.cfg.MaxLogEntries > 0 {
		mm.currentLogLimit = int64(mm.cfg.MaxLogEntries)
	} else {
		mm.currentLogLimit = mm.maxMemoryLimit / (3 * 512) // 512 bytes per log entry
	}

	if mm.cfg.MaxMetricEntries > 0 {
		mm.currentMetricLimit = int64(mm.cfg.MaxMetricEntries)
	} else {
		mm.currentMetricLimit = mm.maxMemoryLimit / (3 * 256) // 256 bytes per metric
	}

	// Ensure minimum limits
	if mm.currentTraceLimit < 1000 {
		mm.currentTraceLimit = 1000
	}
	if mm.currentLogLimit < 1000 {
		mm.currentLogLimit = 1000
	}
	if mm.currentMetricLimit < 1000 {
		mm.currentMetricLimit = 1000
	}
}

// GetCurrentLimits returns the current buffer limits
func (mm *MemoryManager) GetCurrentLimits() (traces, logs, metrics int64) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.currentTraceLimit, mm.currentLogLimit, mm.currentMetricLimit
}

// ShouldAcceptTrace determines if a new trace should be accepted
func (mm *MemoryManager) ShouldAcceptTrace(currentCount int64) bool {
	if !mm.cfg.EnableMemoryPressureHandling {
		return currentCount < mm.currentTraceLimit
	}

	if mm.isUnderMemoryPressure() {
		// Apply sampling under memory pressure
		return mm.shouldSample() && currentCount < mm.currentTraceLimit
	}

	return currentCount < mm.currentTraceLimit
}

// ShouldAcceptLog determines if a new log should be accepted
func (mm *MemoryManager) ShouldAcceptLog(currentCount int64) bool {
	if !mm.cfg.EnableMemoryPressureHandling {
		return currentCount < mm.currentLogLimit
	}

	if mm.isUnderMemoryPressure() {
		return mm.shouldSample() && currentCount < mm.currentLogLimit
	}

	return currentCount < mm.currentLogLimit
}

// ShouldAcceptMetric determines if a new metric should be accepted
func (mm *MemoryManager) ShouldAcceptMetric(currentCount int64) bool {
	if !mm.cfg.EnableMemoryPressureHandling {
		return currentCount < mm.currentMetricLimit
	}

	if mm.isUnderMemoryPressure() {
		return mm.shouldSample() && currentCount < mm.currentMetricLimit
	}

	return currentCount < mm.currentMetricLimit
}

// UpdateMemoryUsage updates the current memory usage and triggers scaling if needed
func (mm *MemoryManager) UpdateMemoryUsage(usage int64) {
	atomic.StoreInt64(&mm.currentMemoryUsage, usage)

	if mm.cfg.EnableAdaptiveScaling {
		mm.checkAndScale()
	}
}

// checkAndScale performs adaptive scaling based on current usage
func (mm *MemoryManager) checkAndScale() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	now := time.Now()
	if now.Sub(mm.lastScaleCheck) < mm.cfg.ScaleCheckInterval {
		return
	}
	mm.lastScaleCheck = now

	currentUsage := atomic.LoadInt64(&mm.currentMemoryUsage)
	usageRatio := float64(currentUsage) / float64(mm.maxMemoryLimit)

	// Check for memory pressure
	if usageRatio > mm.cfg.MemoryPressureThreshold {
		if !mm.underMemoryPressure {
			mm.underMemoryPressure = true
			atomic.AddInt64(&mm.stats.MemoryPressureEvents, 1)
			mm.addScaleEvent(now, 1.0, "memory_pressure_start")
		}
	} else if usageRatio < mm.cfg.MemoryPressureThreshold*0.9 { // Hysteresis
		mm.underMemoryPressure = false
	}
}

// scaleBuffers adjusts buffer sizes by the given factor
func (mm *MemoryManager) scaleBuffers(factor float64, reason string) {
	newTraceLimit := int64(float64(mm.currentTraceLimit) * factor)
	newLogLimit := int64(float64(mm.currentLogLimit) * factor)
	newMetricLimit := int64(float64(mm.currentMetricLimit) * factor)

	// Apply maximum scale factor limits
	maxTraceLimit := int64(float64(mm.cfg.MaxTraceEntries) * mm.cfg.MaxScaleFactor)
	maxLogLimit := int64(float64(mm.cfg.MaxLogEntries) * mm.cfg.MaxScaleFactor)
	maxMetricLimit := int64(float64(mm.cfg.MaxMetricEntries) * mm.cfg.MaxScaleFactor)

	if maxTraceLimit > 0 && newTraceLimit > maxTraceLimit {
		newTraceLimit = maxTraceLimit
	}
	if maxLogLimit > 0 && newLogLimit > maxLogLimit {
		newLogLimit = maxLogLimit
	}
	if maxMetricLimit > 0 && newMetricLimit > maxMetricLimit {
		newMetricLimit = maxMetricLimit
	}

	// Apply minimum limits
	if newTraceLimit < 1000 {
		newTraceLimit = 1000
	}
	if newLogLimit < 1000 {
		newLogLimit = 1000
	}
	if newMetricLimit < 1000 {
		newMetricLimit = 1000
	}

	mm.currentTraceLimit = newTraceLimit
	mm.currentLogLimit = newLogLimit
	mm.currentMetricLimit = newMetricLimit

	mm.addScaleEvent(time.Now(), factor, reason)
}

// Helper methods
func (mm *MemoryManager) isUnderMemoryPressure() bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.underMemoryPressure
}

func (mm *MemoryManager) shouldSample() bool {
	// Simple random sampling based on configured rate
	return rand.Float64() < mm.cfg.SamplingRateUnderPressure
}

func (mm *MemoryManager) addScaleEvent(timestamp time.Time, factor float64, reason string) {
	event := scaleEvent{
		timestamp: timestamp,
		factor:    factor,
		reason:    reason,
	}

	mm.scaleHistory = append(mm.scaleHistory, event)

	// Keep only last 100 events
	if len(mm.scaleHistory) > 100 {
		mm.scaleHistory = mm.scaleHistory[1:]
	}
}

// GetStats returns current memory management statistics
func (mm *MemoryManager) GetStats() MemoryStats {
	return MemoryStats{
		TotalAllocations:     atomic.LoadInt64(&mm.stats.TotalAllocations),
		CurrentAllocations:   atomic.LoadInt64(&mm.stats.CurrentAllocations),
		DroppedTraces:        atomic.LoadInt64(&mm.stats.DroppedTraces),
		DroppedLogs:          atomic.LoadInt64(&mm.stats.DroppedLogs),
		DroppedMetrics:       atomic.LoadInt64(&mm.stats.DroppedMetrics),
		ScaleUpEvents:        atomic.LoadInt64(&mm.stats.ScaleUpEvents),
		ScaleDownEvents:      atomic.LoadInt64(&mm.stats.ScaleDownEvents),
		MemoryPressureEvents: atomic.LoadInt64(&mm.stats.MemoryPressureEvents),
	}
}

// GetMemoryUsage returns current memory usage information
func (mm *MemoryManager) GetMemoryUsage() (current, max int64, usagePercent float64) {
	current = atomic.LoadInt64(&mm.currentMemoryUsage)
	mm.mu.RLock()
	max = mm.maxMemoryLimit
	mm.mu.RUnlock()

	usagePercent = float64(current) / float64(max) * 100
	return current, max, usagePercent
}

func newIngester(cfg *Config) *ingester {
	return newIngesterWithLogger(cfg, nil)
}

func newIngesterWithLogger(cfg *Config, logger *zap.Logger) *ingester {
	// Initialize memory manager
	memMgr := NewMemoryManager(cfg.Memory)
	traceLimit, logLimit, metricLimit := memMgr.GetCurrentLimits()

	ing := &ingester{
		cfg:             cfg,
		logger:          logger,
		memMgr:          memMgr,
		lastMemoryCheck: time.Now(),
	}

	// Initialize buffers based on configuration
	if cfg.Memory.UseRingBuffers {
		ing.traces = NewRingBuffer(traceLimit, 1024, cfg.Memory.RingBufferOverwrite)  // ~1KB per trace
		ing.logs = NewRingBuffer(logLimit, 512, cfg.Memory.RingBufferOverwrite)       // ~512B per log
		ing.metrics = NewRingBuffer(metricLimit, 256, cfg.Memory.RingBufferOverwrite) // ~256B per metric
	} else {
		ing.traces = NewSliceBuffer(traceLimit, 1024)
		ing.logs = NewSliceBuffer(logLimit, 512)
		ing.metrics = NewSliceBuffer(metricLimit, 256)
	}

	// Start memory monitoring goroutine
	go ing.monitorMemoryUsage()

	return ing
}

// monitorMemoryUsage continuously monitors and updates memory usage
func (i *ingester) monitorMemoryUsage() {
	ticker := time.NewTicker(10 * time.Second) // Update every 10 seconds
	defer ticker.Stop()

	for range ticker.C {
		i.updateMemoryUsage()
	}
}

func (i *ingester) updateMemoryUsage() {
	totalUsage := i.traces.EstimateMemoryUsage() +
		i.logs.EstimateMemoryUsage() +
		i.metrics.EstimateMemoryUsage()

	atomic.StoreInt64(&i.estimatedMemoryUsage, totalUsage)
	i.memMgr.UpdateMemoryUsage(totalUsage)

	// Update buffer sizes if they changed
	traceLimit, logLimit, metricLimit := i.memMgr.GetCurrentLimits()
	i.traces.Resize(traceLimit)
	i.logs.Resize(logLimit)
	i.metrics.Resize(metricLimit)

	// Log memory statistics periodically (every 5 minutes)
	now := time.Now()
	if i.logger != nil && now.Sub(i.lastMemoryCheck) > 5*time.Minute {
		i.lastMemoryCheck = now
		current, max, percent := i.memMgr.GetMemoryUsage()
		stats := i.memMgr.GetStats()

		i.logger.Info("Memory usage stats",
			zap.Int64("current_bytes", current),
			zap.Int64("max_bytes", max),
			zap.Float64("usage_percent", percent),
			zap.Int("trace_count", i.traces.Len()),
			zap.Int("log_count", i.logs.Len()),
			zap.Int("metric_count", i.metrics.Len()),
			zap.Int64("trace_limit", traceLimit),
			zap.Int64("log_limit", logLimit),
			zap.Int64("metric_limit", metricLimit),
			zap.Int64("dropped_traces", stats.DroppedTraces),
			zap.Int64("dropped_logs", stats.DroppedLogs),
			zap.Int64("dropped_metrics", stats.DroppedMetrics),
			zap.Int64("scale_up_events", stats.ScaleUpEvents),
			zap.Int64("scale_down_events", stats.ScaleDownEvents),
		)
	}
}

func (i *ingester) drain() ([]traceRow, []logRow, []metricRow) {
	// Drain all buffers
	traceData := i.traces.Drain()
	logData := i.logs.Drain()
	metricData := i.metrics.Drain()

	// Convert back to typed slices
	traces := make([]traceRow, 0, len(traceData))
	for _, t := range traceData {
		if tr, ok := t.(traceRow); ok {
			traces = append(traces, tr)
		}
	}

	logs := make([]logRow, 0, len(logData))
	for _, l := range logData {
		if lr, ok := l.(logRow); ok {
			logs = append(logs, lr)
		}
	}

	metrics := make([]metricRow, 0, len(metricData))
	for _, m := range metricData {
		if mr, ok := m.(metricRow); ok {
			metrics = append(metrics, mr)
		}
	}

	return traces, logs, metrics
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

func (i *ingester) consumeTraces(td ptrace.Traces) error {
	for r := 0; r < td.ResourceSpans().Len(); r++ {
		rs := td.ResourceSpans().At(r)
		rAttrs := extractAttrs(rs.Resource().Attributes())

		for s := 0; s < rs.ScopeSpans().Len(); s++ {
			spans := rs.ScopeSpans().At(s).Spans()
			for j := 0; j < spans.Len(); j++ {
				span := spans.At(j)
				attrs := mergeAttrs(rAttrs, extractAttrs(span.Attributes()))

				traceData := traceRow{
					ts:         span.EndTimestamp().AsTime(),
					attrs:      attrs,
					durationNs: float64(span.EndTimestamp() - span.StartTimestamp()),
					statusCode: span.Status().Code().String(),
				}

				// Check if we should accept this trace
				currentCount := atomic.LoadInt64(&i.traceCount)
				if i.memMgr.ShouldAcceptTrace(currentCount) {
					if i.traces.Add(traceData) {
						atomic.AddInt64(&i.traceCount, 1)
					} else {
						// Buffer full, increment dropped counter
						atomic.AddInt64(&i.memMgr.stats.DroppedTraces, 1)
					}
				} else {
					// Dropped due to memory pressure or limits
					atomic.AddInt64(&i.memMgr.stats.DroppedTraces, 1)
				}
			}
		}
	}

	return nil
}

func (i *ingester) consumeLogs(ld plog.Logs) error {
	for r := 0; r < ld.ResourceLogs().Len(); r++ {
		rl := ld.ResourceLogs().At(r)
		rAttrs := extractAttrs(rl.Resource().Attributes())

		for s := 0; s < rl.ScopeLogs().Len(); s++ {
			logs := rl.ScopeLogs().At(s).LogRecords()
			for j := 0; j < logs.Len(); j++ {
				lr := logs.At(j)
				attrs := mergeAttrs(rAttrs, extractAttrs(lr.Attributes()))

				logData := logRow{
					ts:       lr.Timestamp().AsTime(),
					attrs:    attrs,
					body:     lr.Body().AsString(),
					severity: lr.SeverityText(),
				}

				// Check if we should accept this log
				currentCount := atomic.LoadInt64(&i.logCount)
				if i.memMgr.ShouldAcceptLog(currentCount) {
					if i.logs.Add(logData) {
						atomic.AddInt64(&i.logCount, 1)
					} else {
						atomic.AddInt64(&i.memMgr.stats.DroppedLogs, 1)
					}
				} else {
					atomic.AddInt64(&i.memMgr.stats.DroppedLogs, 1)
				}
			}
		}
	}

	return nil
}

func (i *ingester) consumeMetrics(md pmetric.Metrics) error {
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

						metricData := metricRow{
							ts:    dp.Timestamp().AsTime(),
							attrs: attrs,
							name:  metric.Name(),
							value: dp.DoubleValue(),
						}

						currentCount := atomic.LoadInt64(&i.metricCount)
						if i.memMgr.ShouldAcceptMetric(currentCount) {
							if i.metrics.Add(metricData) {
								atomic.AddInt64(&i.metricCount, 1)
							} else {
								atomic.AddInt64(&i.memMgr.stats.DroppedMetrics, 1)
							}
						} else {
							atomic.AddInt64(&i.memMgr.stats.DroppedMetrics, 1)
						}
					}

				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for k := 0; k < dps.Len(); k++ {
						dp := dps.At(k)
						attrs := mergeAttrs(rAttrs, extractAttrs(dp.Attributes()))

						metricData := metricRow{
							ts:    dp.Timestamp().AsTime(),
							attrs: attrs,
							name:  metric.Name(),
							value: dp.DoubleValue(),
						}

						currentCount := atomic.LoadInt64(&i.metricCount)
						if i.memMgr.ShouldAcceptMetric(currentCount) {
							if i.metrics.Add(metricData) {
								atomic.AddInt64(&i.metricCount, 1)
							} else {
								atomic.AddInt64(&i.memMgr.stats.DroppedMetrics, 1)
							}
						} else {
							atomic.AddInt64(&i.memMgr.stats.DroppedMetrics, 1)
						}
					}
				}
			}
		}
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
