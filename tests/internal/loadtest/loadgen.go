package loadtest

import (
	"net/http"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

type LoadConfig struct {
	URL      string
	Rps      int
	Duration time.Duration
}

func GenerateLoad(cfg *LoadConfig) *vegeta.Metrics {
	rate := vegeta.Rate{Freq: cfg.Rps, Per: time.Second}
	targeter := vegeta.NewStaticTargeter(vegeta.Target{
		Method: "POST",
		URL:    cfg.URL,
		Body:   []byte(`{"messages": [{"value": {"string": "hello1"}}]}`),
		Header: http.Header{"Content-Type": []string{"application/json"}},
	})
	attacker := vegeta.NewAttacker()

	var metrics vegeta.Metrics
	for res := range attacker.Attack(targeter, rate, cfg.Duration, "main") {
		metrics.Add(res)
	}
	metrics.Close()
	return &metrics
}
