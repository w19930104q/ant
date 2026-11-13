package ant

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/yields/ant/internal/normalize"
	"github.com/yields/ant/internal/robots"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// EngineConfig configures the engine.
type EngineConfig struct {
	// Scraper is the scraper to use.
	//
	// If nil, NewEngine returns an error.
	Scraper Scraper

	// Deduper is the URL de-duplicator to use.
	//
	// If nil, DedupeMap is used.
	Deduper Deduper

	// Fetcher is the page fetcher to use.
	//
	// If nil, the default HTTP fetcher is used.
	Fetcher *Fetcher

	// Queue is the URL queue to use.
	//
	// If nil, the default in-memory queue is used. The default
	// queue is reset between consecutive calls to Run, a custom
	// Queue implementation is used as-is and must support being
	// reused if the engine is run more than once.
	Queue Queue

	// Limiter is the rate limiter to use.
	//
	// The limiter is called with each URL before
	// it is fetched.
	//
	// If nil, no limits are used.
	Limiter Limiter

	// Matcher is the URL matcher to use.
	//
	// The matcher is called with a URL before it is queued
	// if it returns false the URL is discarded.
	//
	// If nil, all URLs are queued.
	Matcher Matcher

	// Impolite skips any robots.txt checking.
	//
	// Note that it does not affect any configured
	// ratelimiters or matchers.
	//
	// By default the engine checks robots.txt, it uses
	// the default ant.UserAgent.
	Impolite bool

	// Workers specifies the amount of workers to use.
	//
	// Every worker the engine start consumes URLs from the queue
	// and starts a goroutine for each URL.
	//
	// If <= 0, defaults to 1.
	Workers int

	// Concurrency is the maximum amount of URLs to process
	// at any given time.
	//
	// The engine uses a global semaphore to limit the amount
	// of goroutines started by the workers, across all origins.
	//
	// If <= 0, there's no global limit.
	Concurrency int

	// MaxConcurrentRequestsPerSite limits the amount of URLs
	// processed concurrently for any single origin.
	//
	// An origin is identified by its scheme, hostname and
	// effective port, so "http://example.com" and
	// "http://example.com:8080" are scheduled independently,
	// while URLs of the same origin share the window. URLs of
	// different origins may still be processed in parallel.
	//
	// If <= 0, there is no per-origin concurrency limit.
	MaxConcurrentRequestsPerSite int

	// DownloadDelay is the minimum duration between the dispatch
	// of two URLs of the same origin.
	//
	// The delay is scoped per origin, it does not slow down
	// unrelated origins. Fetcher retries and HTTP redirects of
	// a URL are attributed to the origin that dispatched it.
	//
	// If zero, no delay is applied.
	DownloadDelay time.Duration

	// OriginIdleTimeout is the duration after which an origin
	// with no queued and no in-flight URLs has its scheduling
	// state garbage-collected.
	//
	// If <= 0, a default of 5 minutes is used.
	OriginIdleTimeout time.Duration
}

// Engine implements web crawler engine.
type Engine struct {
	deduper Deduper
	scraper Scraper
	fetcher *Fetcher
	queue   Queue
	matcher Matcher
	limiter Limiter
	robots  *robots.Cache

	impolite bool
	workers  int
	sema     *semaphore.Weighted
	sched    *scheduler
}

// resettableQueue is implemented by queues that can be prepared
// for a new Engine.Run.
type resettableQueue interface {
	reset()
}

// discardHookQueue is implemented by queues that report URLs
// discarded by a Close happening before the URL was dequeued.
type discardHookQueue interface {
	setDiscardHook(func(*URL))
}

// NewEngine returns a new engine.
func NewEngine(c EngineConfig) (*Engine, error) {
	if c.Scraper == nil {
		return nil, errors.New("ant: scraper is required")
	}

	if c.Deduper == nil {
		c.Deduper = DedupeMap()
	}

	if c.Fetcher == nil {
		c.Fetcher = &Fetcher{}
	}

	if c.Workers <= 0 {
		c.Workers = 1
	}

	if c.Queue == nil {
		c.Queue = MemoryQueue(c.Workers)
	}

	var sema *semaphore.Weighted
	if n := int64(c.Concurrency); n > 0 {
		sema = semaphore.NewWeighted(n)
	}

	eng := &Engine{
		scraper:  c.Scraper,
		deduper:  c.Deduper,
		fetcher:  c.Fetcher,
		queue:    c.Queue,
		matcher:  c.Matcher,
		limiter:  c.Limiter,
		robots:   robots.NewCache(DefaultClient, 1000),
		impolite: c.Impolite,
		workers:  c.Workers,
		sema:     sema,
		sched: newScheduler(
			c.MaxConcurrentRequestsPerSite,
			c.DownloadDelay,
			c.OriginIdleTimeout,
			sema,
		),
	}

	// URLs discarded by a cancellation-driven Close never reach
	// Acquire, settle their per-origin counters through Discard.
	if hq, ok := eng.queue.(discardHookQueue); ok {
		hq.setDiscardHook(func(u *URL) {
			eng.sched.Discard(u)
		})
	}

	return eng, nil
}

// Run runs the engine with the given start urls.
func (eng *Engine) Run(ctx context.Context, urls ...string) error {
	// Prepare the engine for a fresh run. The scheduler drops the
	// previous run's origins and the engine-owned default queue is
	// reset so the same engine can be reused.
	eng.reset()

	var eg, subctx = errgroup.WithContext(ctx)

	// Enqueue initial URLs.
	if err := eng.Enqueue(ctx, urls...); err != nil {
		// Nothing was dispatched, close the reset queue so any
		// worker that might observe it shuts down immediately.
		_ = eng.queue.Close(ctx)
		return fmt.Errorf("ant: enqueue - %w", err)
	}

	// Spawn workers.
	for i := 0; i < eng.workers; i++ {
		eg.Go(func() error {
			return eng.run(subctx)
		})
	}

	// Wait until all URLs are handled, or until the context is
	// canceled. On cancellation the queue is closed: URLs that
	// were never dispatched are discarded and their queue
	// counters settled, so Wait unblocks without waiting for work
	// that must never run. In-flight URLs are still awaited below
	// through the error group.
	waitDone := make(chan struct{})
	go func() {
		eng.queue.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-ctx.Done():
		if err := eng.queue.Close(ctx); err != nil {
			return err
		}
		<-waitDone
	}

	if err := eng.queue.Close(ctx); err != nil {
		return err
	}

	// Wait until all workers shutdown, including in-flight URLs.
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("ant: run - %w", err)
	}

	// A canceled or timed-out root context always surfaces, even
	// when the in-flight URLs happened to finish successfully.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("ant: run - %w", err)
	}

	return nil
}

// Close releases the engine's background resources, notably the
// goroutine that garbage-collects idle origins.
//
// It does not close the configured Queue, which is user-owned when
// supplied through EngineConfig. It is safe to call Close more than
// once and safe to reuse the engine with Run afterwards.
func (eng *Engine) Close() error {
	eng.sched.stop()
	return nil
}

// reset prepares the engine for a new run.
//
// Only the scheduler's origin state and the engine-owned default
// queue are reset; a user-provided Deduper or Queue keeps its
// state and semantics across runs.
func (eng *Engine) reset() {
	eng.sched.reset()
	if rq, ok := eng.queue.(resettableQueue); ok {
		rq.reset()
	}
}

// Enqueue enqueues the given set of URLs.
//
// The method blocks until all URLs are queued
// or the given context is canceled.
//
// The method will also de-duplicate the URLs, ensuring
// that URLs will not be visited more than once.
func (eng *Engine) Enqueue(ctx context.Context, rawurls ...string) error {
	var batch = make(URLs, 0, len(rawurls))

	for _, rawurl := range rawurls {
		u, err := url.Parse(rawurl)
		if err != nil {
			return fmt.Errorf("ant: parse url %q - %w", rawurl, err)
		}

		switch u.Scheme {
		case "https", "http":
		default:
			return fmt.Errorf("ant: cannot enqueue invalid URL %q", u)
		}

		batch = append(batch, u)
	}

	return eng.enqueue(ctx, batch)
}

// Enqueue enqueues the given parsed urls.
func (eng *Engine) enqueue(ctx context.Context, batch URLs) error {
	for j := range batch {
		batch[j] = normalize.URL(batch[j])
	}

	next, err := eng.dedupe(ctx, eng.matches(batch))
	if err != nil {
		return err
	}

	if err := eng.queue.Enqueue(ctx, next); err != nil {
		return err
	}

	// Account the de-duplicated URLs that actually reached the
	// queue against their origin's scheduling counters.
	eng.sched.enqueue(next)

	return nil
}

// Run runs a single crawl worker.
//
// The worker dequeues a URL and hands it to the origin scheduler.
// When the context is canceled the scheduler stops dispatching and
// the worker returns as soon as its in-flight URLs finish.
func (eng *Engine) run(ctx context.Context) error {
	eg, subctx := errgroup.WithContext(ctx)
	for {
		url, err := eng.queue.Dequeue(ctx)

		if errors.Is(err, io.EOF) {
			return eg.Wait()
		}
		if errors.Is(err, context.Canceled) {
			return eg.Wait()
		}
		if err != nil {
			return err
		}

		// Acquire the origin's concurrency window and download
		// delay. On cancel the URL is not dispatched, it is
		// discarded and the worker stops pulling.
		if err := eng.sched.Acquire(ctx, url); err != nil {
			eng.sched.Discard(url)
			_ = eng.queue.Done(ctx, url)
			return eg.Wait()
		}

		eg.Go(func() error {
			defer eng.sched.Release(url)
			defer eng.queue.Done(subctx, url)

			err := eng.process(subctx, url)
			eng.sched.Done(url, err)
			return err
		})
	}
}

// Process processes a single url.
func (eng *Engine) process(ctx context.Context, url *URL) error {
	// Check robots.txt.
	if !eng.impolite {
		allowed, err := eng.robots.Allowed(ctx, robots.Request{
			URL:       url,
			UserAgent: UserAgent.String(),
		})
		if err != nil {
			return err
		}
		if !allowed {
			return nil
		}
	}

	// Potential limits.
	if err := eng.limit(ctx, url); err != nil {
		return err
	}

	// Scrape the URL.
	urls, err := eng.scrape(ctx, url)
	if err != nil {
		return err
	}

	// Enqueue URLs.
	if err := eng.enqueue(ctx, urls); err != nil {
		return fmt.Errorf("ant: enqueue - %w", err)
	}

	return nil
}

// Scrape scrapes the given URL and returns the next URLs.
func (eng *Engine) scrape(ctx context.Context, url *URL) (URLs, error) {
	page, err := eng.fetcher.Fetch(ctx, url)

	if err != nil {
		return nil, fmt.Errorf("ant: fetch %q - %w", url, err)
	}

	if page == nil {
		return nil, nil
	}

	defer page.close()

	urls, err := eng.scraper.Scrape(ctx, page)
	if err != nil {
		return nil, fmt.Errorf("ant: scrape %q - %w", url, err)
	}

	return urls, nil
}

// Dedupe de-duplicates the given slice of URLs.
func (eng *Engine) dedupe(ctx context.Context, urls URLs) (URLs, error) {
	deduped, err := eng.deduper.Dedupe(ctx, urls)
	if err != nil {
		return nil, fmt.Errorf("ant: dedupe - %w", err)
	}

	return deduped, nil
}

// Limit runs all configured limiters.
func (eng *Engine) limit(ctx context.Context, url *URL) error {
	if eng.limiter != nil {
		if err := eng.limiter.Limit(ctx, url); err != nil {
			return fmt.Errorf("limit %q - %w", url, err)
		}
	}

	if eng.impolite {
		return nil
	}

	err := eng.robots.Wait(ctx, robots.Request{
		URL:       url,
		UserAgent: UserAgent.String(),
	})
	if err != nil {
		return fmt.Errorf("ant: robots wait - %w", err)
	}

	return nil
}

// Matches returns all URLs that match the matcher.
func (eng *Engine) matches(urls URLs) URLs {
	if eng.matcher != nil {
		ret := make(URLs, 0, len(urls))
		for _, u := range urls {
			if eng.matcher.Match(u) {
				ret = append(ret, u)
			}
		}
		return ret
	}
	return urls
}

// OriginKey returns the stable origin key used to schedule the
// given URL, built from its scheme, hostname and effective port.
//
// It returns an error if the raw URL cannot be parsed.
func OriginKey(rawurl string) (string, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return "", err
	}
	return originKey(u), nil
}

// Origins returns the origin keys currently tracked by the engine.
//
// The returned slice is a snapshot, origins that have been idle
// for longer than OriginIdleTimeout may have been reaped.
func (eng *Engine) Origins() []string {
	return eng.sched.Origins()
}

// OriginStats returns the scheduling counters of the given origin
// key, use OriginKey to compute the key of a URL.
//
// The boolean is false if the origin is not (or no longer) known.
func (eng *Engine) OriginStats(key string) (OriginStats, bool) {
	return eng.sched.OriginStats(key)
}

// Stats returns a snapshot of the per-origin scheduling counters,
// keyed by origin key.
func (eng *Engine) Stats() map[string]OriginStats {
	return eng.sched.Stats()
}
