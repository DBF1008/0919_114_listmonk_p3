package manager

import (
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/knadh/listmonk/models"
)

// mockStore implements the Store interface for tests.
type mockStore struct {
	mu             sync.Mutex
	subs           []models.Subscriber
	sentCounts     []int
	statuses       []string
	campaignStatus string
}

func (s *mockStore) NextCampaigns(currentIDs []int64, sentCounts []int64) ([]*models.Campaign, error) {
	return nil, nil
}

func (s *mockStore) NextSubscribers(campID, limit int) ([]models.Subscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.subs) == 0 {
		return nil, nil
	}
	n := limit
	if n > len(s.subs) {
		n = len(s.subs)
	}
	out := s.subs[:n]
	s.subs = s.subs[n:]
	return out, nil
}

func (s *mockStore) GetCampaign(campID int) (*models.Campaign, error) {
	return &models.Campaign{Status: s.campaignStatus}, nil
}

func (s *mockStore) GetAttachment(mediaID int) (models.Attachment, error) {
	return models.Attachment{}, nil
}

func (s *mockStore) UpdateCampaignStatus(campID int, status string) error {
	s.mu.Lock()
	s.statuses = append(s.statuses, status)
	s.mu.Unlock()
	return nil
}

func (s *mockStore) UpdateCampaignCounts(campID int, toSend int, sent int, lastSubID int) error {
	s.mu.Lock()
	s.sentCounts = append(s.sentCounts, sent)
	s.mu.Unlock()
	return nil
}

func (s *mockStore) CreateLink(url string) (string, error) { return "", nil }
func (s *mockStore) BlocklistSubscriber(id int64) error    { return nil }
func (s *mockStore) DeleteSubscriber(id int64) error       { return nil }

// mockMessenger implements the Messenger interface for tests.
type mockMessenger struct {
	mu     sync.Mutex
	pushed int
}

func (m *mockMessenger) Name() string { return "email" }
func (m *mockMessenger) Push(models.Message) error {
	m.mu.Lock()
	m.pushed++
	m.mu.Unlock()
	return nil
}
func (m *mockMessenger) Flush() error { return nil }
func (m *mockMessenger) Close() error { return nil }

func newTestManager(cfg Config, store Store) *Manager {
	m := New(cfg, store, nil, log.New(io.Discard, "", 0))
	m.fnNotify = func(subject string, data any) error { return nil }
	_ = m.AddMessenger(&mockMessenger{})
	return m
}

func newTestCampaign(id int) *models.Campaign {
	c := &models.Campaign{
		UUID:        "test-uuid",
		Name:        "test-campaign",
		Subject:     "test subject",
		Body:        "test body",
		ContentType: models.CampaignContentTypePlain,
		Messenger:   "email",
		Status:      models.CampaignStatusRunning,
	}
	c.ID = id
	return c
}

// TestWorkerUpdatesPipeBeforeCleanup verifies that a worker finishes updating
// all pipe state (sent count, last ID, rate) before marking a message as done
// on the pipe's waitgroup. cleanup() is triggered by the waitgroup and deletes
// the pipe from the manager; if a worker is still touching the pipe at that
// point, updates race with (and are lost to) cleanup.
//
// Run with -race to catch the regression.
func TestWorkerUpdatesPipeBeforeCleanup(t *testing.T) {
	store := &mockStore{campaignStatus: models.CampaignStatusRunning}
	m := newTestManager(Config{
		Concurrency: 1,
		MessageRate: 100000,
		BatchSize:   10,
	}, store)

	p, err := m.newPipe(newTestCampaign(1))
	if err != nil {
		t.Fatalf("error creating pipe: %v", err)
	}

	const numMsgs = 50
	for i := 0; i < numMsgs; i++ {
		sub := models.Subscriber{
			UUID:  fmt.Sprintf("uuid-%d", i),
			Email: fmt.Sprintf("sub%d@test.com", i),
		}
		sub.ID = i + 1

		msg, err := p.newMessage(sub)
		if err != nil {
			t.Fatalf("error creating message: %v", err)
		}
		m.campMsgQ <- msg
	}

	// Start a worker and release the pipe's initial waitgroup count,
	// simulating subscriber exhaustion in Run().
	go m.worker()
	p.wg.Done()

	// Wait for cleanup() to record the campaign's counts in the store.
	deadline := time.Now().Add(5 * time.Second)
	for {
		store.mu.Lock()
		n := len(store.sentCounts)
		store.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for cleanup()")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Every message's sent increment must be visible to cleanup(). If a
	// worker marks the waitgroup done before incrementing, cleanup() can
	// read the counter before the last increments land.
	store.mu.Lock()
	got := store.sentCounts[0]
	store.mu.Unlock()
	if got != numMsgs {
		t.Fatalf("cleanup() recorded sent=%d, expected %d", got, numMsgs)
	}

	// The pipe should be removed from the manager after cleanup().
	deadline = time.Now().Add(2 * time.Second)
	for {
		m.pipesMut.RLock()
		_, ok := m.pipes[1]
		m.pipesMut.RUnlock()
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pipe was not removed from the manager after cleanup()")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRequeuePipeWhenQueueFull verifies that when the nextPipes queue is full,
// requeuePipe retries until a slot frees up instead of dropping the pipe
// (which would stop it and make workers discard its already queued messages,
// losing them after the campaign's subscriber checkpoint has advanced).
func TestRequeuePipeWhenQueueFull(t *testing.T) {
	m := newTestManager(Config{Concurrency: 1, MessageRate: 10}, &mockStore{})

	oldRetries, oldInterval := pipeRequeueRetries, pipeRequeueInterval
	pipeRequeueRetries = 50
	pipeRequeueInterval = 5 * time.Millisecond
	defer func() { pipeRequeueRetries, pipeRequeueInterval = oldRetries, oldInterval }()

	// Use a queue of capacity 1 and fill it.
	m.nextPipes = make(chan *pipe, 1)

	p, err := m.newPipe(newTestCampaign(1))
	if err != nil {
		t.Fatalf("error creating pipe: %v", err)
	}
	m.nextPipes <- p

	done := make(chan bool, 1)
	go func() { done <- m.requeuePipe(p) }()

	// While the queue is full, requeuePipe must keep retrying.
	select {
	case <-done:
		t.Fatal("requeuePipe returned while the queue was still full")
	case <-time.After(50 * time.Millisecond):
	}

	// Free a slot. The requeue must succeed and the pipe must not be stopped.
	<-m.nextPipes
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("requeuePipe failed after a slot freed up")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("requeuePipe did not return after a slot freed up")
	}

	if p.stopped.Load() {
		t.Fatal("pipe was stopped while requeueing")
	}
}

// TestRequeuePipeGivesUp verifies that requeuePipe eventually gives up on a
// persistently full queue so that the caller can release the pipe instead of
// blocking forever.
func TestRequeuePipeGivesUp(t *testing.T) {
	m := newTestManager(Config{Concurrency: 1, MessageRate: 10}, &mockStore{})

	oldRetries, oldInterval := pipeRequeueRetries, pipeRequeueInterval
	pipeRequeueRetries = 3
	pipeRequeueInterval = 5 * time.Millisecond
	defer func() { pipeRequeueRetries, pipeRequeueInterval = oldRetries, oldInterval }()

	m.nextPipes = make(chan *pipe, 1)

	p, err := m.newPipe(newTestCampaign(1))
	if err != nil {
		t.Fatalf("error creating pipe: %v", err)
	}
	m.nextPipes <- p

	if m.requeuePipe(p) {
		t.Fatal("requeuePipe succeeded on a persistently full queue")
	}
}

// TestSlidingWindowConcurrent verifies that the sliding window counter is
// safe under concurrent use and never exceeds the configured rate.
//
// Run with -race to catch the regression.
func TestSlidingWindowConcurrent(t *testing.T) {
	const (
		rate       = 5
		goroutines = 2
		perWorker  = 5
	)
	m := newTestManager(Config{
		Concurrency:           1,
		MessageRate:           10,
		SlidingWindow:         true,
		SlidingWindowRate:     rate,
		SlidingWindowDuration: 1100 * time.Millisecond,
	}, &mockStore{})

	// Sample the counter while the workers run and track the maximum.
	var maxObserved atomic.Int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				m.slidingMut.Lock()
				c := m.slidingCount
				m.slidingMut.Unlock()
				if int64(c) > maxObserved.Load() {
					maxObserved.Store(int64(c))
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				m.slidingWindowWait()
			}
		}()
	}
	wg.Wait()
	close(stop)

	// 10 messages at a rate of 5 per 1.1s window must trigger at least one
	// full window sleep.
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("sliding window did not throttle: %d messages at rate %d finished in %v",
			goroutines*perWorker, rate, elapsed)
	}

	if got := maxObserved.Load(); got > rate {
		t.Fatalf("sliding count exceeded the rate limit: observed %d, limit %d", got, rate)
	}
}
