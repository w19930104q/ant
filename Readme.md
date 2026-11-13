
<br>
<br>
<br>

<p align=center>
  ant (<em>alpha</em>) is a web crawler for Go.
</p>

<br>
<br>
<br>

<p align=center>
  <a href="https://github.com/yields/ant/workflows/test">
    <img src="https://github.com/yields/ant/workflows/test/badge.svg?event=push" />
  </a>
  <a href="https://pkg.go.dev/github.com/yields/ant">
    <img src="https://pkg.go.dev/badge/github.com/yields/ant" />
  </a>
  <a href="https://goreportcard.com/report/github.com/yields/ant">
    <img src="https://goreportcard.com/badge/github.com/yields/ant" />
  </a>
</p>

<br>
<br>
<br>


<br>

#### Declarative

  The package includes functions that can scan data from the page into your structs
  or slice of structs, this allows you to reduce the noise and complexity in your source-code.

  You can also use a jQuery-like API that allows you to scrape complex HTML pages if needed.

  ```go

  var data struct { Title string `css:"title"` }
  page, _ := ant.Fetch(ctx, "https://apple.com")
  page.Scan(&data)
  data.Title // => Apple
  ```

<br>

#### Headless

  By default the crawler uses `http.Client`, however if you're crawling SPAs
  youc an use the `antcdp.Client` implementation which allows you to use chrome
  headless browser to crawl pages.

  ```go
  eng, err := ant.Engine(ant.EngineConfig{
    Fetcher: &ant.Fetcher{
      Client: antcdp.Client{},
    },
  })
  ```

<br>

#### Polite

  The crawler automatically fetches and caches `robots.txt`, making sure that
  it never causes issues to small website owners. Of-course you can disable
  this behavior.

  ```go
  eng, err := ant.NewEngine(ant.EngineConfig{
    Impolite: true,
  })
  eng.Run(ctx)
  ```

<br>

#### Concurrent

  The crawler maintains a configurable amount of "worker" goroutines that read
  URLs off the queue, and spawn a goroutine for each URL.

  Depending on your configuration, you may want to increase the number of workers
  to speed up URL reads, of-course if you don't have enough resources you can reduce
  the number of workers too.

  ```go
  eng, err := ant.NewEngine(ant.EngineConfig{
    // Spawn 5 worker goroutines that dequeue
    // URLs and spawn a new goroutine for each URL.
    Workers: 5,
  })
  eng.Run(ctx)
  ```

<br>

#### Rate limits

  The package includes a powerful `ant.Limiter` interface that allows you to
  define rate limits per URL. There are some built-in limiters as well.

  ```go
  ant.Limit(1) // 1 rps on all URLs.
  ant.LimitHostname(5, "amazon.com") // 5 rps on amazon.com hostname.
  ant.LimitPattern(5, "amazon.com.*") // 5 rps on URLs starting with `amazon.co.`.
  ant.LimitRegexp(5, "^apple.com\/iphone\/*") // 5 rps on URLs that match the regex.
  ```
  
  Note that `LimitPattern` and `LimitRegexp` only match on the host and path of the URL.

<br>

#### Per-origin scheduling

  Limits that must apply to a whole site are scheduled per origin, where an
  origin is the combination of the scheme, the hostname and the effective port.
  For example `http://example.com` and `http://example.com:80` share an origin
  (the implied default port is filled in), while `http://example.com:8080` and
  `https://example.com` each get their own.

  ```go
  eng, err := ant.NewEngine(ant.EngineConfig{
    Scraper:                      scraper,
    MaxConcurrentRequestsPerSite: 2,                       // at most 2 concurrent requests per origin.
    DownloadDelay:                500 * time.Millisecond,  // half a second between two same-origin requests.
    OriginIdleTimeout:            5 * time.Minute,         // reclaim idle origin state after this duration.
  })
  ```

  URLs of the same origin share a concurrency window and a download delay, while
  URLs of different origins keep running in parallel. Fetcher retries and HTTP
  redirects stay attributed to the origin that dispatched the URL. Cancelling
  the run's context stops further dispatch and lets the in-flight requests
  finish before `Run` returns.

  `Engine.Stats`, `Engine.OriginStats` and `Engine.Origins` expose stable
  per-origin counters for queued, in-flight, successful and failed URLs.

<br>

#### Matchers

  Another powerful interface is `ant.Matcher` which allows you to define URL
  matchers, the matchers are called before URLs are queued.

  ```go
  ant.MatchHostname("amazon.com") // scrape amazon.com URLs only.
  ant.MatchPattern("amazon.com/help/*")
  ant.MatchRegexp("amazon\.com\/help/.+")
  ```

<br>

#### Robust

  The crawl engine automatically retries any errors that implement `Temporary()`
  error that returns true.

  Becuase the standard library returns errors that implement that interface
  the engine will retry most temporary network and HTTP errors.

  ```go
  eng, err := ant.NewEngine(ant.EngineConfig{
    Scraper: myscraper{},
    MaxAttempts: 5,
  })

  // Blocks until one of the following is true:
  //
  // 1. No more URLs to crawl (the scraper stops returning URLs)
  // 2. A non-temporary error occured.
  // 3. MaxAttempts was reached.
  //
  err = eng.Run(ctx)
  ```

<br>

#### Built-in Scrapers

  The whole point of scraping is to extract data from websites into a machine readable
  format such as CSV or JSON, ant comes with built-in scrapers to make this ridiculously
  easy, here's a full cralwer that extracts quotes into stdout.


[embedmd]:# (_examples/jsonquotes/main.go /func main/ $)
```go
func main() {
	var url = "http://quotes.toscrape.com"
	var ctx = context.Background()
	var start = time.Now()

	type quote struct {
		Text string   `css:".text"   json:"text"`
		By   string   `css:".author" json:"by"`
		Tags []string `css:".tag"    json:"tags"`
	}

	type page struct {
		Quotes []quote `css:".quote" json:"quotes"`
	}

	eng, err := ant.NewEngine(ant.EngineConfig{
		Scraper: ant.JSON(os.Stdout, page{}, `li.next > a`),
		Matcher: ant.MatchHostname("quotes.toscrape.com"),
	})
	if err != nil {
		log.Fatalf("new engine: %s", err)
	}

	if err := eng.Run(ctx, url); err != nil {
		log.Fatal(err)
	}

	log.Printf("scraped in %s :)", time.Since(start))
}
```
<br>

#### Testing

  `anttest` package makes it easy to test your scraper implementation
  it fetches a page by a URL, caches it in the OS's temporary directory and re-uses it.

  The func depends on the file's modtime, the file expires daily, you can adjust
  the TTL by setting `antttest.FetchTTL`.

  ```Go
  // Fetch calls `t.Fatal` on errors.
  page := anttest.Fetch(t, "https://apple.com")
  _, err := myscraper.Scrape(ctx, page)
  assert.NoError(err)
  ```

<br>
<br>
