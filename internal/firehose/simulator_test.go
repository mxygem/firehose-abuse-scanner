package firehose

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewSimulator(t *testing.T) {
	testCases := []struct {
		desc     string
		opts     []SimulatorOption
		expected *Simulator
	}{
		{
			desc: "default no options",
			opts: []SimulatorOption{},
			expected: &Simulator{
				EventsPerSecond:      DefaultEventsPerSecond,
				SimulatorConcurrency: DefaultSimulatorConcurrency,
				BurstMultiplier:      DefaultBurstMultiplier,
				BurstDuration:        DefaultBurstDuration,
				BurstProbability:     DefaultBurstProbability,
			},
		},
		{
			desc: "with events per second",
			opts: []SimulatorOption{WithEventsPerSecond(1000)},
			expected: &Simulator{
				EventsPerSecond:      1000,
				SimulatorConcurrency: DefaultSimulatorConcurrency,
				BurstMultiplier:      DefaultBurstMultiplier,
				BurstDuration:        DefaultBurstDuration,
				BurstProbability:     DefaultBurstProbability,
			},
		},
		{
			desc: "with burst multiplier",
			opts: []SimulatorOption{WithBurstMultiplier(5)},
			expected: &Simulator{
				EventsPerSecond:      DefaultEventsPerSecond,
				SimulatorConcurrency: DefaultSimulatorConcurrency,
				BurstMultiplier:      5,
				BurstDuration:        DefaultBurstDuration,
				BurstProbability:     DefaultBurstProbability,
			},
		},
		{
			desc: "with burst duration",
			opts: []SimulatorOption{WithBurstDuration(10)},
			expected: &Simulator{
				EventsPerSecond:      DefaultEventsPerSecond,
				SimulatorConcurrency: DefaultSimulatorConcurrency,
				BurstMultiplier:      DefaultBurstMultiplier,
				BurstDuration:        10,
				BurstProbability:     DefaultBurstProbability,
			},
		},
		{
			desc: "with concurrency",
			opts: []SimulatorOption{WithConcurrency(7)},
			expected: &Simulator{
				EventsPerSecond:      DefaultEventsPerSecond,
				SimulatorConcurrency: 7,
				BurstMultiplier:      DefaultBurstMultiplier,
				BurstDuration:        DefaultBurstDuration,
				BurstProbability:     DefaultBurstProbability,
			},
		},
		{
			desc: "with events per second and concurrency",
			opts: []SimulatorOption{WithEventsPerSecond(1000), WithConcurrency(12)},
			expected: &Simulator{
				EventsPerSecond:      1000,
				SimulatorConcurrency: 12,
				BurstMultiplier:      DefaultBurstMultiplier,
				BurstDuration:        DefaultBurstDuration,
				BurstProbability:     DefaultBurstProbability,
			},
		},
		{
			desc: "with all options overridden",
			opts: []SimulatorOption{
				WithEventsPerSecond(9999),
				WithBurstMultiplier(4),
				WithBurstDuration(25),
				WithConcurrency(3),
			},
			expected: &Simulator{
				EventsPerSecond:      9999,
				SimulatorConcurrency: 3,
				BurstMultiplier:      4,
				BurstDuration:        25,
				BurstProbability:     DefaultBurstProbability,
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			s := NewSimulator(tC.opts...)
			assert.Equal(t, tC.expected, s)
		})
	}
}

func TestSimulator_Name(t *testing.T) {
	s := NewSimulator()
	assert.Equal(t, "local-simulator", s.Name())
}

func TestSimulator_Subscribe_basic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewSimulator(
		WithEventsPerSecond(10),
		WithConcurrency(1),
		WithBurstMultiplier(1),
		WithBurstDuration(2),
	)

	ch, err := s.Subscribe(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, ch)

	// Receive a few events and ensure they are reasonable.
	evt, ok := <-ch
	assert.True(t, ok, "channel should produce event before context cancel")
	assert.NotEmpty(t, evt.ID)
	assert.NotZero(t, evt.CreatedAt)
}

func TestSimulator_Subscribe_context_cancel(t *testing.T) {
	s := NewSimulator(WithEventsPerSecond(5))

	ctx, cancel := context.WithCancel(context.Background())

	ch, err := s.Subscribe(ctx)
	assert.NoError(t, err)

	// Cancel context while reading events
	cancel()

	// Channel should be closed soon; read until closed or timeout.
	select {
	case _, ok := <-ch:
		// Either we get an event or channel closes
		if ok {
			// consume possible spurious value, but subsequent read must close
			_, more := <-ch
			assert.False(t, more, "channel should close after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel to close after cancel")
	}
}
