package ant

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// defaultOriginIdleTimeout is the default duration after which an
// idle origin is garbage-collected along with its scheduling state.
const defaultOriginIdleTimeout = 5 * time.Minute

// OriginStats is a point-in-time snapshot of the scheduling state
// of a single origin.
//
// The counters are safe to read while the engine is running, they
// represent a best-effort snapshot and are not synchronized with
// each other.
type OriginStats struct {
	// Origin is the origin key, built from the scheme, hostname
	// and effective port, e.g. "https://example.com" or
	// "http://example.com:8080".
	Origin string

	// Queued is the amount of URLs currently queued for the
	// origin, waiting to be dispatched.
	Queued int64

	// InFlight is the amount of URLs currently being processed.
	InFlight int64

	// Success is the total amount of URLs that were processed
	// successfully during the current Engine.Run.
	Success int64

	// Failed is the total amount of URLs whose processing failed
	// during the current Engine.Run.
	Failed int64
}

// origin holds the scheduling state of a single origin, identified
// by its scheme, hostname and effective port.
//
// Every origin owns its own concurrency window and download delay,
// so two different origins never share rate-limiting state while
// URLs of the same origin always go through the same window.
type origin struct {
	// key is the origin key, see originKey.
	key string

	// mu guards the fields below and serializes reservation of
	// the download-delay slots so that same-origin dispatches are
	// spaced out.
	mu sync.Mutex

	// sema limits the amount of concurrently processed URLs
	// for the origin, nil means no per-origin limit.
	sema *semaphore.Weighted

	// delay is the minimum duration between two dispatches
	// for the origin, zero means no delay.
	delay time.Duration

	// nextDispatch is the earliest time the next URL may be
	// dispatched, zero means it may go immediately.
	nextDispatch time.Time

	// lastSeen tracks the last activity of the origin, used for
	// idle garbage-collection.
	lastSeen time.Time

	// queued is the amount of URLs currently waiting to be
	// dispatched to the origin.
	queued int64

	// inFlight is the amount of URLs currently being processed.
	inFlight int64

	// success is the total amount of successfully processed URLs
	// during the current run.
	success int64

	// failed is the total amount of failed URLs during the
	// current run.
	failed int64
}

// snapshot returns a stable counter snapshot.
func (o *origin) snapshot() OriginStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return OriginStats{
		Origin:   o.key,
		Queued:   o.queued,
		InFlight: o.inFlight,
		Success:  o.success,
		Failed:   o.failed,
	}
}

// scheduler dispatches URLs to per-origin scheduling state.
//
// The scheduler guarantees that URLs of the same origin respect
// the configured concurrency window and download delay, while URLs
// of different origins may be acquired and processed in parallel.
//
// Retries and redirects remain attributed to the originating
// origin because Acquire is called before fetching, using the URL
// that was dequeued; any HTTP redirect and any fetcher retry
// happens underneath that same Acquire call.
type scheduler struct {
	mu      sync.Mutex
	origins map[string]*origin

	// maxConcurrent is the per-origin concurrency window,
	// zero means no limit.
	maxConcurrent int64

	// downloadDelay is the minimum duration between the
	// dispatch of two URLs of the same origin.
	downloadDelay time.Duration

	// idleTimeout is the duration after which an origin with
	// no queued and no in-flight URLs is garbage-collected.
	idleTimeout time.Duration

	// globalSema optionally limits the total amount of
	// concurrently processed URLs across all origins, nil
	// means no global limit.
	globalSema *semaphore.Weighted

	// stopC and stopOnce terminate the idle reaper goroutine.
	stopC    chan struct{}
	stopOnce sync.Once

	// wg tracks the idle reaper goroutine.
	wg sync.WaitGroup
}

// newScheduler returns a new scheduler and starts its idle reaper.
func newScheduler(maxConcurrent int, downloadDelay, idleTimeout time.Duration, globalSema *semaphore.Weighted) *scheduler {
	if idleTimeout <= 0 {
		idleTimeout = defaultOriginIdleTimeout
	}
	s := &scheduler{
		origins:       make(map[string]*origin),
		maxConcurrent: int64(maxConcurrent),
		downloadDelay: downloadDelay,
		idleTimeout:   idleTimeout,
		globalSema:    globalSema,
		stopC:         make(chan struct{}),
	}
	s.startReaper()
	return s
}

// startReaper launches the idle reaper goroutine.
func (s *scheduler) startReaper() {
	s.wg.Add(1)
	go s.reaper()
}

// reset clears all per-origin state for a new Engine.Run and
// restarts the idle reaper if Close was called previously.
//
// The method must not be called while a previous run is still
// active; origins only carry run-scoped counters.
func (s *scheduler) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.origins = make(map[string]*origin)

	select {
	case <-s.stopC:
		// The previous reaper was stopped, start a fresh one so
		// the engine remains usable after Close.
		s.stopC = make(chan struct{})
		s.stopOnce = sync.Once{}
		s.startReaper()
	default:
	}
}

// stop terminates the idle reaper. In-flight URLs are tracked by
// the engine through Release/Done, not by the scheduler.
func (s *scheduler) stop() {
	s.stopOnce.Do(func() {
		close(s.stopC)
	})
	s.wg.Wait()
}

// enqueue accounts for the given URLs in the per-origin queued
// counters.
//
// The URLs must have been de-duplicated already, every URL is
// counted exactly once.
func (s *scheduler) enqueue(urls URLs) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, u := range urls {
		o := s.lookupLocked(u)
		o.mu.Lock()
		o.queued++
		o.lastSeen = now
		o.mu.Unlock()
	}
}

// Acquire blocks until the URL's origin concurrency window, the
// download delay and the optional global concurrency allow the URL
// to be processed.
//
// The method returns immediately with the context's error when the
// context is canceled, in which case Release must not be called and
// the URL is treated as not dispatched.
func (s *scheduler) Acquire(ctx context.Context, u *URL) error {
	o := s.lookup(u)

	// Global concurrency window first, so a canceled context
	// never reserves an origin slot.
	if s.globalSema != nil {
		if err := s.globalSema.Acquire(ctx, 1); err != nil {
			return err
		}
	}

	// Per-origin concurrency window.
	if o.sema != nil {
		if err := o.sema.Acquire(ctx, 1); err != nil {
			if s.globalSema != nil {
				s.globalSema.Release(1)
			}
			return err
		}
	}

	// Per-origin download delay. The origin lock serializes the
	// wait so that same-origin dispatches are spaced by `delay`
	// even when multiple workers are contending.
	if err := s.waitDelay(ctx, o); err != nil {
		if o.sema != nil {
			o.sema.Release(1)
		}
		if s.globalSema != nil {
			s.globalSema.Release(1)
		}
		return err
	}

	o.mu.Lock()
	o.queued--
	o.inFlight++
	o.lastSeen = time.Now()
	o.mu.Unlock()

	return nil
}

// waitDelay enforces the minimum spacing between two dispatches of
// the same origin.
//
// The next allowed dispatch time is computed and pushed forward
// while holding the origin lock, so contending workers of the same
// origin line up: each one reserves a slot `delay` after the
// previously reserved one, guaranteeing two same-origin URLs are
// never dispatched closer together than the delay. The actual sleep
// happens without holding the lock and is interruptible by context
// cancellation.
func (s *scheduler) waitDelay(ctx context.Context, o *origin) error {
	if o.delay <= 0 {
		return nil
	}

	o.mu.Lock()
	now := time.Now()
	next := o.nextDispatch
	if next.Before(now) {
		next = now
	}
	wait := next.Sub(now)
	o.nextDispatch = next.Add(o.delay)
	o.mu.Unlock()

	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees the concurrency windows acquired for the URL.
//
// The method must be called once for every successful Acquire.
func (s *scheduler) Release(u *URL) {
	o, ok := s.lookupIfExists(u)
	if !ok {
		// The origin was reaped while the URL was in flight;
		// release the global window but there is nothing else.
		if s.globalSema != nil {
			s.globalSema.Release(1)
		}
		return
	}

	if o.sema != nil {
		o.sema.Release(1)
	}
	if s.globalSema != nil {
		s.globalSema.Release(1)
	}

	o.mu.Lock()
	o.inFlight--
	o.lastSeen = time.Now()
	o.mu.Unlock()
}

// Discard settles a URL that was accounted for but never
// dispatched, either because it was dropped by a cancellation-driven
// queue Close or because a worker's Acquire failed on a canceled
// context.
//
// It only decrements the origin's queued counter: a URL that never
// reached a fetcher is neither a success nor a failure.
func (s *scheduler) Discard(u *URL) {
	o, ok := s.lookupIfExists(u)
	if !ok {
		return
	}

	o.mu.Lock()
	o.queued--
	o.lastSeen = time.Now()
	o.mu.Unlock()
}

// Done records the outcome of a fully processed URL, incrementing
// the origin's success or failure counter.
//
// A URL reaches Done only after a successful Acquire, so its
// queued counter was already decremented when it was dispatched.
func (s *scheduler) Done(u *URL, err error) {
	o, ok := s.lookupIfExists(u)
	if !ok {
		return
	}

	o.mu.Lock()
	if err != nil {
		o.failed++
	} else {
		o.success++
	}
	o.lastSeen = time.Now()
	o.mu.Unlock()
}

// lookup returns the origin for the URL, creating it on first use.
func (s *scheduler) lookup(u *URL) *origin {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookupLocked(u)
}

// lookupIfExists returns the origin only if it is still tracked.
func (s *scheduler) lookupIfExists(u *URL) (*origin, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.origins[originKey(u)]
	return o, ok
}

// lookupLocked returns the origin for the URL, creating it.
func (s *scheduler) lookupLocked(u *URL) *origin {
	key := originKey(u)
	o, ok := s.origins[key]
	if !ok {
		o = &origin{
			key:      key,
			delay:    s.downloadDelay,
			lastSeen: time.Now(),
		}
		if s.maxConcurrent > 0 {
			o.sema = semaphore.NewWeighted(s.maxConcurrent)
		}
		s.origins[key] = o
	}
	return o
}

// reaper garbage-collects idle origins periodically.
func (s *scheduler) reaper() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.idleTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopC:
			return
		case now := <-ticker.C:
			s.reap(now)
		}
	}
}

// reap removes origins that have no queued and no in-flight URLs
// and have been idle for longer than idleTimeout.
func (s *scheduler) reap(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, o := range s.origins {
		o.mu.Lock()
		idle := o.queued == 0 && o.inFlight == 0 &&
			now.Sub(o.lastSeen) >= s.idleTimeout
		o.mu.Unlock()
		if idle {
			delete(s.origins, key)
		}
	}
}

// Stats returns a snapshot of the per-origin counters, keyed by
// origin key.
func (s *scheduler) Stats() map[string]OriginStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make(map[string]OriginStats, len(s.origins))
	for key, o := range s.origins {
		ret[key] = o.snapshot()
	}
	return ret
}

// OriginStats returns the stats of the given origin key, the
// boolean is false if the origin is not (or no longer) known.
func (s *scheduler) OriginStats(key string) (OriginStats, bool) {
	s.mu.Lock()
	o, ok := s.origins[key]
	s.mu.Unlock()
	if !ok {
		return OriginStats{}, false
	}
	return o.snapshot(), true
}

// Origins returns the origin keys currently known to the scheduler.
func (s *scheduler) Origins() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make([]string, 0, len(s.origins))
	for key := range s.origins {
		ret = append(ret, key)
	}
	return ret
}

// originKey builds a stable origin key from the URL's scheme,
// hostname and effective port.
//
// The effective port defaults to the scheme's well-known port when
// the URL does not specify one, so "http://example.com" and
// "http://example.com:80" share an origin while a URL with an
// explicit non-default port gets its own origin. This avoids the
// net.SplitHostPort pitfall where a host without a port yields an
// empty hostname and collapses unrelated sites onto one spider.
func originKey(u *url.URL) string {
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		switch u.Scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		default:
			port = "0"
		}
	}
	return strings.ToLower(u.Scheme) + "://" + host + ":" + port
}
