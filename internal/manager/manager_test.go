package manager

import (
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/knadh/listmonk/models"
	"github.com/paulbellamy/ratecounter"
)

// mockStore is a no-op Store implementation for tests.
type mockStore struct{}

func (s *mockStore) NextCampaigns(currentIDs []int64, sentCounts []int64) ([]*models.Campaign, error) {
	return nil, nil
}

func (s *mockStore) NextSubscribers(campID, limit int) ([]models.Subscriber, error) {
	return nil, nil
}

func (s *mockStore) GetCampaign(campID int) (*models.Campaign, error) {
	return &models.Campaign{}, nil
}

func (s *mockStore) GetAttachment(mediaID int) (models.Attachment, error) {
	return models.Attachment{}, nil
}

func (s *mockStore) UpdateCampaignStatus(campID int, status string) error { return nil }

func (s *mockStore) UpdateCampaignCounts(campID int, toSend int, sent int, lastSubID int) error {
	return nil
}

func (s *mockStore) CreateLink(url string) (string, error) { return "0000", nil }
func (s *mockStore) BlocklistSubscriber(id int64) error    { return nil }
func (s *mockStore) DeleteSubscriber(id int64) error       { return nil }

// mockMessenger records pushed messages.
type mockMessenger struct {
	mu   sync.Mutex
	sent []models.Message
}

func (m *mockMessenger) Name() string { return "email" }

func (m *mockMessenger) Push(msg models.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, msg)
	return nil
}

func (m *mockMessenger) Flush() error { return nil }
func (m *mockMessenger) Close() error { return nil }

func (m *mockMessenger) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sent)
}

func newTestManager(t *testing.T, cfg Config) (*Manager, *mockMessenger) {
	t.Helper()

	if cfg.MessageRate == 0 {
		cfg.MessageRate = 10000
	}

	m := New(cfg, &mockStore{}, nil, log.New(io.Discard, "", 0))
	msgr := &mockMessenger{}
	if err := m.AddMessenger(msgr); err != nil {
		t.Fatalf("error adding messenger: %v", err)
	}

	return m, msgr
}

func newTestPipe(m *Manager, id int) *pipe {
	camp := &models.Campaign{Name: "test-campaign", Messenger: "email"}
	camp.ID = id

	return &pipe{
		camp: camp,
		rate: ratecounter.NewRateCounter(time.Minute),
		wg:   &sync.WaitGroup{},
		m:    m,
	}
}

// TestWorkerSkipsMessageForCleanedUpPipe verifies that a worker doesn't
// process (or panic on) a queued message whose pipe has already been
// cleaned up and removed from the active pipes map.
func TestWorkerSkipsMessageForCleanedUpPipe(t *testing.T) {
	m, msgr := newTestManager(t, Config{Concurrency: 1})
	go m.worker()

	// The pipe is deliberately NOT registered in m.pipes, simulating a pipe
	// that cleanup() has already removed while its message was still queued.
	p := newTestPipe(m, 1)
	m.campMsgQ <- CampaignMessage{
		Campaign:   p.camp,
		Subscriber: models.Subscriber{Email: "a@b.c"},
		to:         "a@b.c",
		pipe:       p,
	}

	// The message must be skipped without a panic and without a send.
	time.Sleep(300 * time.Millisecond)
	if n := msgr.count(); n != 0 {
		t.Fatalf("expected 0 messages sent for a cleaned-up pipe, got %d", n)
	}
}

// TestWorkerProcessesMessageForActivePipe verifies that a message whose pipe
// is still active is processed normally.
func TestWorkerProcessesMessageForActivePipe(t *testing.T) {
	m, msgr := newTestManager(t, Config{Concurrency: 1})
	go m.worker()

	p := newTestPipe(m, 1)
	m.pipesMut.Lock()
	m.pipes[1] = p
	m.pipesMut.Unlock()

	p.wg.Add(1)
	m.campMsgQ <- CampaignMessage{
		Campaign:   p.camp,
		Subscriber: models.Subscriber{Email: "a@b.c"},
		to:         "a@b.c",
		pipe:       p,
	}

	// The worker calls wg.Done() once the message is processed.
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the message to be processed")
	}

	if n := msgr.count(); n != 1 {
		t.Fatalf("expected 1 message sent for an active pipe, got %d", n)
	}
}

// TestRequeuePipeWhenQueueFull verifies that a pipe is not stopped/dropped
// when the nextPipes queue is full, and that it's eventually requeued so
// that its already queued messages are not lost.
func TestRequeuePipeWhenQueueFull(t *testing.T) {
	m, _ := newTestManager(t, Config{Concurrency: 1})

	// Fill the pipe queue to capacity.
	for i := 0; i < cap(m.nextPipes); i++ {
		m.nextPipes <- &pipe{}
	}

	p := newTestPipe(m, 42)
	m.requeuePipe(p)

	// The pipe must not be stopped (which would discard its queued messages).
	if p.stopped.Load() {
		t.Fatal("pipe was stopped instead of being requeued")
	}

	// Free up a slot. The pipe should be enqueued asynchronously.
	<-m.nextPipes

	timeout := time.After(5 * time.Second)
	for {
		select {
		case got := <-m.nextPipes:
			if got == p {
				return
			}
		case <-timeout:
			t.Fatal("pipe was not requeued after the queue drained")
		}
	}
}

// TestRequeuePipeImmediate verifies the fast path where the queue has room.
func TestRequeuePipeImmediate(t *testing.T) {
	m, _ := newTestManager(t, Config{Concurrency: 1})

	p := newTestPipe(m, 1)
	m.requeuePipe(p)

	select {
	case got := <-m.nextPipes:
		if got != p {
			t.Fatal("dequeued a different pipe than the one requeued")
		}
	default:
		t.Fatal("pipe was not queued synchronously")
	}

	if p.stopped.Load() {
		t.Fatal("pipe was unexpectedly stopped")
	}
}

// TestSlidingWindowConcurrency hammers the sliding window limiter from
// multiple goroutines. Run with -race to detect unsynchronized access to
// the shared counter/window state, and verify that the configured rate
// limit is actually enforced (no lost increments).
func TestSlidingWindowConcurrency(t *testing.T) {
	m, _ := newTestManager(t, Config{})
	m.cfg.SlidingWindow = true
	m.cfg.SlidingWindowRate = 100
	m.cfg.SlidingWindowDuration = 100 * time.Millisecond

	const (
		goroutines = 8
		perWorker  = 50
		total      = goroutines * perWorker
	)

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				m.throttleSlidingWindow()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// 400 messages at a limit of 100 per 100ms window must span at least
	// ~3 windows of waiting. If increments are lost to the data race, the
	// limit is hit less often and the run finishes suspiciously fast.
	if elapsed < 250*time.Millisecond {
		t.Fatalf("sliding window limit not enforced: %d messages throttled in %v", total, elapsed)
	}
}
