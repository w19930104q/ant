package ant

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recordingScraper counts scrape calls and blocks until release is
// closed, returning no follow-up URLs.
type recordingScraper struct {
	mu       sync.Mutex
	paths    []string
	inFlight int64
	maxSeen  int64
	started  chan struct{}
	release  chan struct{}
}

func newRecordingScraper() *recordingScraper {
	return &recordingScraper{
		started: make(chan struct{}, 128),
		release: make(chan struct{}),
	}
}

func (s *recordingScraper) Scrape(ctx context.Context, p *Page) (URLs, error) {
	n := atomic.AddInt64(&s.inFlight, 1)
	for {
		seen := atomic.LoadInt64(&s.maxSeen)
		if n <= seen || atomic.CompareAndSwapInt64(&s.maxSeen, seen, n) {
			break
		}
	}
	defer atomic.AddInt64(&s.inFlight, -1)

	s.mu.Lock()
	s.paths = append(s.paths, p.URL.Path)
	s.mu.Unlock()

	select {
	case s.started <- struct{}{}:
	default:
	}

	// Block until released or the crawl is canceled; on cancel
	// the in-flight scrape still returns cleanly so the engine
	// can wait for it to finish.
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return nil, nil
}

func (s *recordingScraper) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.paths)
}

// waitUntilStarted blocks until n scrapes have started.
func (s *recordingScraper) waitUntilStarted(t testing.TB, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-s.started:
		case <-deadline:
			t.Fatalf("timed out waiting for scrape %d to start", i+1)
		}
	}
}

// nonBlockingScraper returns immediately instead of waiting for
// release, used for tests that measure timing rather than blocking.
type nonBlockingScraper struct {
	recordingScraper
}

func (s *nonBlockingScraper) Scrape(ctx context.Context, p *Page) (URLs, error) {
	atomic.AddInt64(&s.inFlight, 1)
	defer atomic.AddInt64(&s.inFlight, -1)

	s.mu.Lock()
	s.paths = append(s.paths, p.URL.Path)
	s.mu.Unlock()
	return nil, nil
}

func newNonBlockingScraper() *nonBlockingScraper {
	return &nonBlockingScraper{
		recordingScraper: recordingScraper{
			started: make(chan struct{}, 128),
			release: make(chan struct{}),
		},
	}
}

// newTestServer returns an httptest server serving n standalone
// pages (no cross links, so de-duplication never drops start URLs).
func newTestServer(t testing.TB, n int) (*httptest.Server, *atomic.Int64) {
	t.Helper()

	var hits atomic.Int64
	mux := http.NewServeMux()

	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	for i := 0; i < n; i++ {
		path := fmt.Sprintf("/p%d", i)
		mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			fmt.Fprint(w, `<html><body>ok</body></html>`)
		})
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &hits
}

func newTestEngine(t testing.TB, scraper Scraper, cfg EngineConfig) *Engine {
	t.Helper()
	cfg.Scraper = scraper
	cfg.Impolite = true
	eng, err := NewEngine(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func TestOriginKey(t *testing.T) {
	var cases = []struct {
		name string
		raw  string
		want string
	}{
		{"http default port implied", "http://example.com/a", "http://example.com:80"},
		{"http explicit default port", "http://example.com:80/a", "http://example.com:80"},
		{"https default port implied", "https://example.com/a", "https://example.com:443"},
		{"https explicit default port", "https://example.com:443/a", "https://example.com:443"},
		{"http explicit non-default port", "http://example.com:8080/a", "http://example.com:8080"},
		{"https explicit non-default port", "https://example.com:8443/a", "https://example.com:8443"},
		{"different host", "http://other.com/a", "http://other.com:80"},
		{"different scheme same host", "https://example.com/a", "https://example.com:443"},
		{"uppercase host normalized", "http://EXAMPLE.com/a", "http://example.com:80"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert := require.New(t)

			got, err := OriginKey(c.raw)
			assert.NoError(err)
			assert.Equal(c.want, got)

			// The same origin must be produced through url.URL directly.
			u, err := url.Parse(c.raw)
			assert.NoError(err)
			assert.Equal(c.want, originKey(u))
		})
	}
}

func TestOriginKeyDefaultAndExplicitPortShareOrigin(t *testing.T) {
	assert := require.New(t)

	implicit, err := OriginKey("http://example.com/path")
	assert.NoError(err)
	explicit, err := OriginKey("http://example.com:80/path")
	assert.NoError(err)
	assert.Equal(implicit, explicit)

	httpsImplicit, err := OriginKey("https://example.com/path")
	assert.NoError(err)
	httpsExplicit, err := OriginKey("https://example.com:443/path")
	assert.NoError(err)
	assert.Equal(httpsImplicit, httpsExplicit)

	// But a non-default port must not collapse onto the default one.
	custom, err := OriginKey("http://example.com:8080/path")
	assert.NoError(err)
	assert.NotEqual(implicit, custom)
}

func TestEnginePerOriginConcurrency(t *testing.T) {
	t.Run("same origin respects the window", func(t *testing.T) {
		assert := require.New(t)
		srv, _ := newTestServer(t, 6)
		scraper := newRecordingScraper()
		eng := newTestEngine(t, scraper, EngineConfig{
			Workers:                      8,
			MaxConcurrentRequestsPerSite: 2,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		runErr := make(chan error, 1)
		go func() {
			runErr <- eng.Run(ctx,
				srv.URL+"/p0", srv.URL+"/p1", srv.URL+"/p2",
				srv.URL+"/p3", srv.URL+"/p4", srv.URL+"/p5",
			)
		}()

		// The window is 2: exactly two scrapes start and the rest
		// must wait behind the origin's semaphore.
		scraper.waitUntilStarted(t, 2)
		assert.Eventually(func() bool {
			return atomic.LoadInt64(&scraper.inFlight) == 2
		}, time.Second, 5*time.Millisecond)
		assert.Equal(int64(2), atomic.LoadInt64(&scraper.maxSeen))

		close(scraper.release)
		assert.NoError(<-runErr)

		assert.Equal(6, scraper.count())
		assert.Equal(int64(2), atomic.LoadInt64(&scraper.maxSeen))

		st, ok := eng.OriginStats(mustOriginKey(t, srv.URL+"/p0"))
		assert.True(ok)
		assert.Equal(int64(6), st.Success)
		assert.Equal(int64(0), st.Failed)
		assert.Equal(int64(0), st.Queued)
		assert.Equal(int64(0), st.InFlight)
	})

	t.Run("different origins run in parallel", func(t *testing.T) {
		assert := require.New(t)
		srvA, _ := newTestServer(t, 3)
		srvB, _ := newTestServer(t, 3)
		scraper := newRecordingScraper()
		eng := newTestEngine(t, scraper, EngineConfig{
			Workers:                      8,
			MaxConcurrentRequestsPerSite: 1,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		runErr := make(chan error, 1)
		go func() {
			runErr <- eng.Run(ctx,
				srvA.URL+"/p0", srvA.URL+"/p1", srvA.URL+"/p2",
				srvB.URL+"/p0", srvB.URL+"/p1", srvB.URL+"/p2",
			)
		}()

		// Each origin is capped at 1, but the two origins together
		// must reach 2 concurrent scrapes.
		scraper.waitUntilStarted(t, 2)
		assert.Eventually(func() bool {
			return atomic.LoadInt64(&scraper.inFlight) == 2
		}, time.Second, 5*time.Millisecond)
		assert.GreaterOrEqual(atomic.LoadInt64(&scraper.maxSeen), int64(2))

		close(scraper.release)
		assert.NoError(<-runErr)

		assert.Equal(6, scraper.count())

		keys := eng.Origins()
		assert.Len(keys, 2)
		for _, key := range keys {
			st, ok := eng.OriginStats(key)
			assert.True(ok)
			assert.Equal(int64(3), st.Success, key)
			assert.Equal(int64(0), st.Failed, key)
		}
	})

	t.Run("explicit and default port are separate origins", func(t *testing.T) {
		assert := require.New(t)
		srv, _ := newTestServer(t, 2)

		u, _ := url.Parse(srv.URL)
		// The loopback IP and "localhost" with the same effective
		// port are distinct hostnames and therefore distinct
		// origins; asserting the keys directly plus the engine's
		// origin set covers the port normalization rule.
		viaIP := "http://127.0.0.1:" + u.Port() + "/p0"
		viaLocalhost := "http://localhost:" + u.Port() + "/p1"

		scraper := newRecordingScraper()
		eng := newTestEngine(t, scraper, EngineConfig{Workers: 4})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		close(scraper.release)
		assert.NoError(eng.Run(ctx, viaIP, viaLocalhost))

		assert.Len(eng.Origins(), 2)
		assert.NotEqual(
			mustOriginKey(t, viaIP),
			mustOriginKey(t, viaLocalhost),
		)

		// Default and explicit well-known ports still collapse.
		implicit, _ := OriginKey("http://example.com/p")
		explicit, _ := OriginKey("http://example.com:80/p")
		assert.Equal(implicit, explicit)
	})
}

func TestEngineDownloadDelay(t *testing.T) {
	t.Run("same origin spacing", func(t *testing.T) {
		assert := require.New(t)
		srv, _ := newTestServer(t, 4)
		scraper := newNonBlockingScraper()
		delay := 120 * time.Millisecond
		eng := newTestEngine(t, scraper, EngineConfig{
			Workers:       4,
			DownloadDelay: delay,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		start := time.Now()
		assert.NoError(eng.Run(ctx,
			srv.URL+"/p0", srv.URL+"/p1", srv.URL+"/p2", srv.URL+"/p3",
		))
		elapsed := time.Since(start)

		assert.Equal(4, scraper.count())
		// Four same-origin dispatches must take at least 3 delays.
		assert.GreaterOrEqual(elapsed, 3*delay)

		st, ok := eng.OriginStats(mustOriginKey(t, srv.URL+"/p0"))
		assert.True(ok)
		assert.Equal(int64(4), st.Success)
	})

	t.Run("delay does not serialize different origins", func(t *testing.T) {
		assert := require.New(t)
		srvA, _ := newTestServer(t, 3)
		srvB, _ := newTestServer(t, 3)
		scraper := newNonBlockingScraper()
		delay := 150 * time.Millisecond
		eng := newTestEngine(t, scraper, EngineConfig{
			Workers:       8,
			DownloadDelay: delay,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		start := time.Now()
		assert.NoError(eng.Run(ctx,
			srvA.URL+"/p0", srvA.URL+"/p1", srvA.URL+"/p2",
			srvB.URL+"/p0", srvB.URL+"/p1", srvB.URL+"/p2",
		))
		elapsed := time.Since(start)

		assert.Equal(6, scraper.count())
		// Each origin needs at least 2 delays; origins run in
		// parallel so the total must stay well under the 5 delays
		// a single shared limiter would impose.
		assert.Less(elapsed, 4*delay)
	})

	t.Run("window and delay combine per origin", func(t *testing.T) {
		assert := require.New(t)
		srvA, _ := newTestServer(t, 3)
		srvB, _ := newTestServer(t, 3)
		scraper := newNonBlockingScraper()
		delay := 100 * time.Millisecond
		eng := newTestEngine(t, scraper, EngineConfig{
			Workers:                      8,
			DownloadDelay:                delay,
			MaxConcurrentRequestsPerSite: 2,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		start := time.Now()
		assert.NoError(eng.Run(ctx,
			srvA.URL+"/p0", srvA.URL+"/p1", srvA.URL+"/p2",
			srvB.URL+"/p0", srvB.URL+"/p1", srvB.URL+"/p2",
		))
		elapsed := time.Since(start)

		assert.Equal(6, scraper.count())
		// Per origin: two slots start together, the third waits one
		// delay, so each origin takes ~1 delay; the two run in
		// parallel. A shared limiter would impose ~5 delays.
		assert.GreaterOrEqual(elapsed, delay)
		assert.Less(elapsed, 3*delay)
	})
}

func TestEngineCancelStopsDispatchAndWaitsInFlight(t *testing.T) {
	assert := require.New(t)
	srv, _ := newTestServer(t, 8)
	scraper := newRecordingScraper()
	eng := newTestEngine(t, scraper, EngineConfig{
		Workers:                      4,
		MaxConcurrentRequestsPerSite: 2,
	})

	ctx, cancel := context.WithCancel(context.Background())

	runErr := make(chan error, 1)
	go func() {
		runErr <- eng.Run(ctx,
			srv.URL+"/p0", srv.URL+"/p1", srv.URL+"/p2", srv.URL+"/p3",
			srv.URL+"/p4", srv.URL+"/p5", srv.URL+"/p6", srv.URL+"/p7",
		)
	}()

	// Wait until the origin window is full of in-flight scrapes.
	scraper.waitUntilStarted(t, 2)
	assert.Eventually(func() bool {
		return atomic.LoadInt64(&scraper.inFlight) == 2
	}, time.Second, 5*time.Millisecond)

	cancel()

	select {
	case err := <-runErr:
		assert.Error(err)
		assert.True(
			errors.Is(err, context.Canceled),
			"expected context.Canceled, got %v", err,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// Cancellation stops dispatching: only the two in-flight
	// scrapes ran. The remaining URLs were either blocked on the
	// origin window or still queued, both are discarded and never
	// reach the scraper.
	assert.Equal(2, scraper.count())

	// In-flight scrapes were allowed to finish, nothing is left
	// running and no counter goes negative.
	assert.Eventually(func() bool {
		return atomic.LoadInt64(&scraper.inFlight) == 0
	}, 2*time.Second, 10*time.Millisecond)

	st, ok := eng.OriginStats(mustOriginKey(t, srv.URL+"/p0"))
	assert.True(ok)
	assert.Equal(int64(0), st.InFlight)
	assert.Equal(int64(0), st.Queued)
	// Only the two dispatched scrapes are counted, the six
	// discarded URLs are neither success nor failure.
	assert.Equal(int64(2), st.Success)
	assert.Equal(int64(0), st.Failed)
}

func TestEngineCancelWhileBlockedOnOriginWindow(t *testing.T) {
	// More workers than URLs and a window of one: every URL is
	// dequeued immediately and the surplus blocks inside Acquire,
	// so cancellation must unwind blocked workers instead of
	// waiting on an already-empty queue.
	assert := require.New(t)
	srv, _ := newTestServer(t, 4)
	scraper := newRecordingScraper()
	eng := newTestEngine(t, scraper, EngineConfig{
		Workers:                      8,
		MaxConcurrentRequestsPerSite: 1,
	})

	ctx, cancel := context.WithCancel(context.Background())

	runErr := make(chan error, 1)
	go func() {
		runErr <- eng.Run(ctx,
			srv.URL+"/p0", srv.URL+"/p1", srv.URL+"/p2", srv.URL+"/p3",
		)
	}()

	scraper.waitUntilStarted(t, 1)
	cancel()

	select {
	case err := <-runErr:
		assert.ErrorIs(err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run hung while workers were blocked on the origin window")
	}

	assert.Eventually(func() bool {
		return atomic.LoadInt64(&scraper.inFlight) == 0
	}, 2*time.Second, 10*time.Millisecond)

	st, ok := eng.OriginStats(mustOriginKey(t, srv.URL+"/p0"))
	assert.True(ok)
	assert.Equal(int64(0), st.Queued)
	assert.Equal(int64(0), st.InFlight)
	assert.Equal(int64(1), st.Success+st.Failed)
}

func TestEngineRetryAndRedirectStayOnOrigin(t *testing.T) {
	t.Run("retry is attributed to the dispatching origin", func(t *testing.T) {
		assert := require.New(t)

		var attempts atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/robots.txt" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if attempts.Add(1) < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			fmt.Fprint(w, `<html><body>ok</body></html>`)
		}))
		t.Cleanup(srv.Close)

		scraper := newNonBlockingScraper()
		eng := newTestEngine(t, scraper, EngineConfig{
			Workers: 2,
			Fetcher: &Fetcher{
				MaxAttempts: 5,
				MinBackoff:  time.Millisecond,
				MaxBackoff:  2 * time.Millisecond,
			},
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		assert.NoError(eng.Run(ctx, srv.URL+"/retry"))
		assert.Equal(1, scraper.count())
		assert.Equal(int64(3), attempts.Load())

		st, ok := eng.OriginStats(mustOriginKey(t, srv.URL+"/retry"))
		assert.True(ok)
		assert.Equal(int64(1), st.Success)
		assert.Equal(int64(0), st.Failed)
	})

	t.Run("cross-origin redirect is charged to the originating origin", func(t *testing.T) {
		assert := require.New(t)

		target, targetHits := newTestServer(t, 1)

		var redirects atomic.Int64
		entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/robots.txt" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			redirects.Add(1)
			http.Redirect(w, r, target.URL+"/p0", http.StatusFound)
		}))
		t.Cleanup(entry.Close)

		scraper := newNonBlockingScraper()
		eng := newTestEngine(t, scraper, EngineConfig{Workers: 2})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		assert.NoError(eng.Run(ctx, entry.URL+"/start"))
		assert.Equal(int64(1), redirects.Load())
		assert.Equal(int64(1), targetHits.Load())

		// The originating origin owns the dispatch and the success.
		st, ok := eng.OriginStats(mustOriginKey(t, entry.URL+"/start"))
		assert.True(ok)
		assert.Equal(int64(1), st.Success)
		assert.Equal(int64(0), st.Failed)
	})
}

func TestEngineConsecutiveRuns(t *testing.T) {
	assert := require.New(t)
	srv, _ := newTestServer(t, 3)
	scraper := newNonBlockingScraper()
	eng := newTestEngine(t, scraper, EngineConfig{
		Workers: 4,
		// A fresh deduper per run below keeps the three URLs
		// visible on every Run; the default queue is reset by the
		// engine itself.
		Deduper: DedupeMap(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for run := 0; run < 3; run++ {
		// Replace the deduper with an empty one for each run to
		// simulate independent crawls against the same engine.
		eng.deduper = DedupeMap()
		assert.NoError(eng.Run(ctx, srv.URL+"/p0", srv.URL+"/p1", srv.URL+"/p2"))

		st, ok := eng.OriginStats(mustOriginKey(t, srv.URL+"/p0"))
		assert.True(ok)
		assert.Equal(int64(3), st.Success, "run %d", run)
		assert.Equal(int64(0), st.Queued, "run %d", run)
		assert.Equal(int64(0), st.InFlight, "run %d", run)
	}

	// Stats are scoped per run: the scheduler resets origins.
	st, _ := eng.OriginStats(mustOriginKey(t, srv.URL+"/p0"))
	assert.Equal(int64(3), st.Success)
}

func TestEngineCloseAndReuse(t *testing.T) {
	assert := require.New(t)
	srv, _ := newTestServer(t, 2)
	scraper := newNonBlockingScraper()
	eng := newTestEngine(t, scraper, EngineConfig{Workers: 4})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Close is idempotent.
	assert.NoError(eng.Close())
	assert.NoError(eng.Close())

	// The engine remains usable after Close: Run restarts the
	// idle reaper and a fresh crawl still completes and records
	// per-origin counters. An empty deduper keeps both URLs new.
	eng.deduper = DedupeMap()
	assert.NoError(eng.Run(ctx, srv.URL+"/p0", srv.URL+"/p1"))

	st, ok := eng.OriginStats(mustOriginKey(t, srv.URL+"/p0"))
	assert.True(ok)
	assert.Equal(int64(2), st.Success)
	assert.Equal(int64(0), st.Queued)
	assert.Equal(int64(0), st.InFlight)
}

func TestSchedulerIdleReap(t *testing.T) {
	t.Run("idle origin is garbage-collected deterministically", func(t *testing.T) {
		assert := require.New(t)

		sched := newScheduler(1, 0, time.Minute, nil)
		defer sched.stop()

		u, err := url.Parse("http://example.com/p")
		assert.NoError(err)
		sched.enqueue(URLs{u})

		sched.mu.Lock()
		assert.Len(sched.origins, 1)
		o := sched.origins[originKey(u)]
		// Settle its counters and age it beyond the idle timeout.
		o.mu.Lock()
		o.queued = 0
		o.inFlight = 0
		o.lastSeen = time.Now().Add(-2 * time.Minute)
		o.mu.Unlock()
		sched.mu.Unlock()

		sched.reap(time.Now())

		sched.mu.Lock()
		assert.Len(sched.origins, 0)
		sched.mu.Unlock()

		// A later enqueue recreates the origin cleanly.
		sched.enqueue(URLs{u})
		st, ok := sched.OriginStats(originKey(u))
		assert.True(ok)
		assert.Equal(int64(1), st.Queued)
	})

	t.Run("busy origins are not reaped", func(t *testing.T) {
		assert := require.New(t)

		sched := newScheduler(1, 0, time.Minute, nil)
		defer sched.stop()

		u, _ := url.Parse("http://example.com/p")
		sched.enqueue(URLs{u})

		sched.mu.Lock()
		o := sched.origins[originKey(u)]
		o.mu.Lock()
		o.lastSeen = time.Now().Add(-2 * time.Minute)
		// Still in flight: it must survive reaping.
		o.inFlight = 1
		o.mu.Unlock()
		sched.mu.Unlock()

		sched.reap(time.Now())

		_, ok := sched.OriginStats(originKey(u))
		assert.True(ok)
	})

	t.Run("background reaper eventually collects idle origins", func(t *testing.T) {
		assert := require.New(t)

		sched := newScheduler(1, 0, 20*time.Millisecond, nil)
		defer sched.stop()

		u, _ := url.Parse("http://example.com/p")
		sched.enqueue(URLs{u})
		sched.mu.Lock()
		o := sched.origins[originKey(u)]
		o.mu.Lock()
		o.queued = 0
		o.mu.Unlock()
		sched.mu.Unlock()

		assert.Eventually(func() bool {
			sched.mu.Lock()
			defer sched.mu.Unlock()
			return len(sched.origins) == 0
		}, 2*time.Second, 10*time.Millisecond)
	})
}

func TestSchedulerQueuedSettlesOnCancel(t *testing.T) {
	assert := require.New(t)

	sched := newScheduler(1, 0, time.Minute, nil)
	defer sched.stop()

	u, err := url.Parse("http://example.com/p")
	assert.NoError(err)
	sched.enqueue(URLs{u})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assert.NoError(sched.Acquire(ctx, u))
	sched.Release(u)
	sched.Done(u, nil)

	st, ok := sched.OriginStats(originKey(u))
	assert.True(ok)
	assert.Equal(int64(0), st.Queued)
	assert.Equal(int64(0), st.InFlight)
	assert.Equal(int64(1), st.Success)

	// A canceled Acquire must be settled through Discard: it never
	// ran, so it is neither a success nor a failure. Use a distinct
	// origin so its counters are isolated from the first URL.
	u2, _ := url.Parse("http://other.example/q")
	sched.enqueue(URLs{u2})
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	err = sched.Acquire(cctx, u2)
	assert.ErrorIs(err, context.Canceled)
	sched.Discard(u2)

	st, _ = sched.OriginStats(originKey(u2))
	assert.Equal(int64(0), st.Queued)
	assert.Equal(int64(0), st.InFlight)
	assert.Equal(int64(0), st.Failed)
	assert.Equal(int64(0), st.Success)
}

func mustOriginKey(t testing.TB, rawurl string) string {
	t.Helper()
	key, err := OriginKey(rawurl)
	require.NoError(t, err)
	return key
}
