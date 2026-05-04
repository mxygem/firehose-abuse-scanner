package firehose

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

const (
	DefaultBurstMultiplier      = 3.0
	DefaultBurstDuration        = 10
	DefaultSimulatorConcurrency = 1
	DefaultEventsPerSecond      = 1000
	DefaultBurstProbability     = 0.3
	MaxChannelBuffer            = 10000
)

// Simulator generates realistic-looking AT protocol events locally.
// It mimics the bursty, high-volume nature of the real Bluesky firehose.
type Simulator struct {
	// EventsPerSecond controls the target throughput.
	EventsPerSecond int
	// SimulatorConcurrency is the number of goroutines to run concurrently.
	SimulatorConcurrency int
	// BurstMultiplier briefly spikes throughput to simulate viral moments.
	BurstMultiplier float64
	// BurstDuration is the duration of the burst.
	BurstDuration int
	// BurstProbability is the probability of a burst occurring.
	BurstProbability float64
	// Duration limits how long the simulator runs. Zero means run until ctx is canceled.
	Duration time.Duration
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

func WithDuration(d time.Duration) SimulatorOption {
	return func(s *Simulator) {
		s.Duration = d
	}
}

func (s *Simulator) Name() string { return "local-simulator" }

func (s *Simulator) Subscribe(ctx context.Context) (<-chan models.FirehoseEvent, error) {
	l := slog.Default()
	ch := make(chan models.FirehoseEvent, min(s.EventsPerSecond, MaxChannelBuffer))

	var cancel context.CancelFunc
	if s.Duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.Duration)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	l.Info("subscribing to firehose", "events_per_second", s.EventsPerSecond, "burst_duration", s.BurstDuration, "burst_multiplier", s.BurstMultiplier, "simulator_concurrency", s.SimulatorConcurrency)

	var wg sync.WaitGroup
	for i := 0; i < s.SimulatorConcurrency; i++ {
		wg.Add(1)
		go func(runnerID int) {
			defer wg.Done()
			ticker := time.NewTicker(time.Second / time.Duration(s.EventsPerSecond))
			defer ticker.Stop()

			burstDuration := time.Duration(s.BurstDuration) * time.Second
			burstTicker := time.NewTicker(burstDuration)
			defer burstTicker.Stop()

			inBurst := false
			burstEnd := time.Time{}

			for {
				select {
				case <-ctx.Done():
					return
				case <-burstTicker.C:
					// randomly trigger a burst
					if rand.Float64() < s.BurstProbability {
						l.Info("triggering burst", "runner", runnerID)
						inBurst = true
						burstEnd = time.Now().Add(burstDuration)
					}

				case t := <-ticker.C:
					if inBurst && time.Now().After(burstEnd) {
						l.Info("burst ended", "runner", runnerID)
						inBurst = false
					}

					count := 3
					if inBurst {
						count = int(s.BurstMultiplier)
						if count < 1 {
							count = 1
						}
					}

					for j := 0; j < count; j++ {
						evt := generateEvent(t)
						select {
						case ch <- evt:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		cancel()
		close(ch)
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

var counter atomic.Int64

func generateEvent(t time.Time) models.FirehoseEvent {
	id := counter.Add(1)
	kind := eventKinds[rand.IntN(len(eventKinds))]

	evt := models.FirehoseEvent{
		ID:         fmt.Sprintf("evt-%d", id),
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
