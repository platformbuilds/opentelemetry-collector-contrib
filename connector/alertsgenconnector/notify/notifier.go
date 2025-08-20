package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Event is the minimal event shape the connector emits.
// Keep in sync with alertsgenconnector.alertEvent.
type Event struct {
	Rule     string            `json:"rule"`
	State    string            `json:"state"` // firing|resolved|no_data
	Severity string            `json:"severity"`
	Labels   map[string]string `json:"labels"`
	Value    float64           `json:"value"`
	Window   string            `json:"window"`
	For      string            `json:"for"`
}

// Notifier is a pluggable sink. Implementations should be non-blocking
// and handle retries/backoff internally.
type Notifier interface {
	Send(ctx context.Context, events []Event) error
}

// Nop is a no-op notifier.
type Nop struct{}

func NewNop() *Nop                                 { return &Nop{} }
func (n *Nop) Send(context.Context, []Event) error { return nil }

// Alertmanager implements batch POST to /api/v2/alerts.
type Alertmanager struct {
	Client  *http.Client
	BaseURL string
}

func NewAlertmanager(url string, timeout time.Duration) *Alertmanager {
	return &Alertmanager{
		Client:  &http.Client{Timeout: timeout},
		BaseURL: url,
	}
}

func (a *Alertmanager) Send(ctx context.Context, events []Event) error {
	if a == nil || a.BaseURL == "" || len(events) == 0 {
		return nil
	}
	payload := make([]map[string]any, 0, len(events))
	for _, ev := range events {
		labels := map[string]string{
			"alertname": ev.Rule,
			"severity":  ev.Severity,
			"state":     ev.State,
		}
		for k, v := range ev.Labels {
			labels[k] = v
		}
		annotations := map[string]string{
			"value":  fmt.Sprintf("%g", ev.Value),
			"window": ev.Window,
			"for":    ev.For,
		}
		payload = append(payload, map[string]any{
			"labels":      labels,
			"annotations": annotations,
		})
	}
	b, _ := json.Marshal(payload)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+"/api/v2/alerts", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("alertmanager responded %s", resp.Status)
	}
	return nil
}
