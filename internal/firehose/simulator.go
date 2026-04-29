package firehose

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

const (
	DefaultBurstMultiplier      = 3.0
	DefaultBurstDuration        = 10
	DefaultSimulatorConcurrency = 1
	DefaultEventsPerSecond      = 1000
	DefaultBurstProbability     = 0.3
)

// Simulator generates realistic-looking AT protocol events locally.
// It mimics the bursty, high-volume nature of the real Bluesky firehose.
type Simulator struct {
	// EventsPerSecond controlsThe target throughput.
	EventsPerSecond int
	// SimulatorConcurrency the number of goroutines to run concurrently.
	SimulatorConcurrency int
	// BurstMultiplier briefly spikes throughput to simulate viral moments.
	BurstMultiplier float64
	// BurstDuration the duration of the burst.
	BurstDuration int
	// BurstProbability the probability of a burst occurring.
	BurstProbability float64
}

func NewSimulator(opts ...SimulatorOption) *Simulator {
	s := &Simulator{
		EventsPerSecond:      DefaultEventsPerSecond,
		SimulatorConcurrency: DefaultSimulatorConcurrency,
		BurstMultiplier:      DefaultBurstMultiplier,
		BurstDuration:        DefaultBurstDuration,
		BurstProbability:     DefaultBurstProbability,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

type SimulatorOption func(*Simulator)

func WithConcurrency(concurrency int) SimulatorOption {
	return func(s *Simulator) {
		s.SimulatorConcurrency = concurrency
	}
}

func WithBurstMultiplier(multiplier float64) SimulatorOption {
	return func(s *Simulator) {
		s.BurstMultiplier = multiplier
	}
}

func WithBurstDuration(duration int) SimulatorOption {
	return func(s *Simulator) {
		s.BurstDuration = duration
	}
}

func WithEventsPerSecond(eventsPerSecond int) SimulatorOption {
	return func(s *Simulator) {
		s.EventsPerSecond = eventsPerSecond
	}
}

func WithBurstProbability(probability float64) SimulatorOption {
	return func(s *Simulator) {
		s.BurstProbability = probability
	}
}

func (s *Simulator) Name() string { return "local-simulator" }

func (s *Simulator) Subscribe(ctx context.Context) (<-chan models.FirehoseEvent, error) {
	l := slog.Default()
	ch := make(chan models.FirehoseEvent, s.EventsPerSecond) // buffer = 1 second of events

	l.Info("subscribing to firehose", "events_per_second", s.EventsPerSecond, "burst_duration", s.BurstDuration, "burst_multiplier", s.BurstMultiplier, "simulator_concurrency", s.SimulatorConcurrency)
	burstDuration := time.Duration(s.BurstDuration) * time.Second

	go func() {
		defer close(ch)

		ticker := time.NewTicker(time.Second / time.Duration(s.EventsPerSecond))
		defer ticker.Stop()

		burstTicker := time.NewTicker(burstDuration)
		defer burstTicker.Stop()

		inBurst := false
		burstEnd := time.Time{}

		for {
			select {
			case <-ctx.Done():
				return

			case <-burstTicker.C:
				// randomly trigger a burst ~30% of the time
				if rand.Float64() < 0.3 {
					l.Info("triggering burst")
					inBurst = true
					burstEnd = time.Now().Add(2 * time.Second)
				}

			case t := <-ticker.C:
				if inBurst && time.Now().After(burstEnd) {
					l.Info("burst ended")
					inBurst = false
				}

				count := 1
				if inBurst {
					count = int(s.BurstMultiplier)
				}

				for range count {
					evt := generateEvent(t)
					select {
					case ch <- evt:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ch, nil
}

// synthetic data pools

var (
	eventKinds = []models.EventKind{
		models.EventPost, // included three times to weight heavier
		models.EventPost,
		models.EventPost,
		models.EventRepost,
		models.EventFollow,
		models.EventProfile,
	}

	sampleTexts = []string{
		"Just saw the most amazing sunset 🌅",
		"Click here for free crypto!! t.co/fakespam",
		"This is a test post from a bot account",
		"Really enjoying the AT Protocol ecosystem",
		"BUY NOW LIMITED OFFER CLICK LINK IN BIO",
		"Excited about the new Bluesky features",
		"[SPAM] Earn money fast, DM me",
		"Good morning everyone on the fediverse!",
		"Replying to myself repeatedly for engagement",
		"Genuine human post about my cat 🐈",
	}

	sampleLinks = []string{
		"https://bsky.app",
		"https://legit-site.com/article",
		"https://spam-link.xyz/free-money",
		"https://news.ycombinator.com",
		"https://phishing.example.com/login",
	}
)

var counter int64

func generateEvent(t time.Time) models.FirehoseEvent {
	counter++
	kind := eventKinds[rand.IntN(len(eventKinds))]

	evt := models.FirehoseEvent{
		ID:         fmt.Sprintf("evt-%d", counter),
		DID:        fmt.Sprintf("did:plc:%016x", rand.Int64N(16)),
		Kind:       kind,
		CreatedAt:  t,
		ReceivedAt: time.Now(),
	}

	if kind == models.EventPost {
		evt.Text = sampleTexts[rand.IntN(len(sampleTexts))]
		// include a link 20% of the time
		if rand.Float64() < 0.2 {
			evt.Links = []string{sampleLinks[rand.IntN(len(sampleLinks))]}
		}
	}

	return evt
}
