package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Entry is a snapshot of an alert's state stored in TSDB.
type Entry struct {
	Labels      map[string]string
	Active      bool
	ForDuration time.Duration
}

// TSDBSyncer does remote-read against Prometheus/VictoriaMetrics-compatible
// /api/v1/query endpoints to recover active alert state on startup.
type TSDBSyncer struct {
	base        *url.URL
	client      *http.Client
	dedupWindow time.Duration
}

// NewTSDBSyncer builds a syncer for the given baseURL (e.g. http://vm:8428).
func NewTSDBSyncer(baseURL string, queryTimeout, dedupWindow time.Duration) (*TSDBSyncer, error) {
	if baseURL == "" {
		return nil, errors.New("baseURL required")
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	cli := &http.Client{
		Timeout: queryTimeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return &TSDBSyncer{
		base:        u,
		client:      cli,
		dedupWindow: dedupWindow,
	}, nil
}

// QueryActive returns currently firing entries for a given rule_id by issuing
// an instant query: otel_alert_active_total{rule_id="<rule>"} != 0
func (s *TSDBSyncer) QueryActive(ruleID string) (map[uint64]Entry, error) {
	if s == nil || s.base == nil {
		return map[uint64]Entry{}, nil
	}
	q := fmt.Sprintf(`otel_alert_active_total{rule_id=%q}`, ruleID)
	res, err := s.instantQuery(context.Background(), q)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]Entry, len(res))
	for _, r := range res {
		lbls := map[string]string{}
		for k, v := range r.Metric {
			if k == "__name__" || k == "rule_id" || k == "severity" {
				continue
			}
			lbls[k] = v
		}
		active := r.Value.Num() != "0"
		fp := fingerprint(ruleID, lbls)
		out[fp] = Entry{
			Labels:      lbls,
			Active:      active,
			ForDuration: 0,
		}
	}
	return out, nil
}

// ---- HTTP & response parsing ----

func (s *TSDBSyncer) instantQuery(ctx context.Context, query string) ([]vmResult, error) {
	u := *s.base
	u.Path = strings.TrimRight(u.Path, "/") + "/api/v1/query"
	v := url.Values{}
	v.Set("query", query)
	u.RawQuery = v.Encode()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out vmResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Status != "success" {
		return nil, fmt.Errorf("tsdb query failed: %s", out.Error)
	}
	return out.Data.Result, nil
}

type vmResp struct {
	Status string      `json:"status"`
	Data   vmData      `json:"data"`
	Error  string      `json:"error,omitempty"`
	Warn   interface{} `json:"warnings,omitempty"`
}
type vmData struct {
	ResultType string     `json:"resultType"`
	Result     []vmResult `json:"result"`
}
type vmResult struct {
	Metric map[string]string `json:"metric"`
	Value  vmValue           `json:"value"`
}
type vmValue [2]any

func (v vmValue) Num() string {
	if len(v) != 2 {
		return "0"
	}
	if s, ok := v[1].(string); ok {
		return s
	}
	return "0"
}

// fingerprint matches the engine logic (FNV-1a).
func fingerprint(rule string, labels map[string]string) uint64 {
	h := fnv64a{}
	h.WriteString(rule)
	ks := make([]string, 0, len(labels))
	for k := range labels {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		_ = h.WriteByte(0xff)
		h.WriteString(k)
		_ = h.WriteByte(0xff)
		h.WriteString(labels[k])
	}
	return h.Sum64()
}

// lightweight FNV-1a 64 implementation.
type fnv64a struct{ sum uint64 }

func (f *fnv64a) WriteByte(b byte) error {
	if f.sum == 0 {
		f.sum = 1469598103934665603
	}
	f.sum ^= uint64(b)
	f.sum *= 1099511628211
	return nil
}
func (f *fnv64a) WriteString(s string) {
	for i := 0; i < len(s); i++ {
		_ = f.WriteByte(s[i])
	}
}
func (f *fnv64a) Sum64() uint64 {
	if f.sum == 0 {
		return 1469598103934665603
	}
	return f.sum
}

// Add to the existing TSDBSyncer struct methods:

// PublishEvents sends alert events to TSDB (stub for now)
func (s *TSDBSyncer) PublishEvents(events []interface{}) error {
	// TODO: Implement actual publishing to TSDB
	// This would typically write alert state metrics via remote write API
	return nil
}
