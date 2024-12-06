package remotewrite

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/nats-io/nats.go"
	"net/url"
	"sync"
	"time"
)

type natClient struct {
	sanitizedURL    string
	remoteWriteURL  string
	subject         string
	userName        string
	password        string
	useVMProto      bool
	fq              *persistentqueue.FastQueue
	concurrency     int
	connections     []*nats.Conn
	sendBlock       func(nc *nats.Conn, block []byte) bool
	authConfig      *promauth.Config
	rl              rateLimiter
	bytesSent       *metrics.Counter
	blocksSent      *metrics.Counter
	requestDuration *metrics.Histogram
	requestsOKCount *metrics.Counter
	errorsCount     *metrics.Counter
	packetsDropped  *metrics.Counter
	rateLimit       *metrics.Gauge
	retriesCount    *metrics.Counter
	sendDuration    *metrics.FloatCounter
	wg              sync.WaitGroup
	stopCh          chan struct{}
}

func newNatClient(argIdx int, remoteWriteURL, sanitizedURL string, fq *persistentqueue.FastQueue, concurrency int) Client {
	authConfig, err := getNatsAuthConfig(argIdx)
	if err != nil {
		logger.Fatalf("cannot initialize auth config for -remoteWrite.url=%q: %s", remoteWriteURL, err)
	}
	parsedURL, e := parseNatsURL(remoteWriteURL)
	if e != nil {
		logger.Fatalf("cannot initialize remoteURL for -remoteWrite.url=%q: %s", remoteWriteURL, e)
	}
	scheme := parsedURL.Scheme
	host := parsedURL.Host
	userInfo := parsedURL.User
	var userName, password string
	if userInfo != nil {
		userName = userInfo.Username()
		password, _ = userInfo.Password()
	} else if authConfig != nil {
		userName = authConfig.Username
		password = authConfig.Password.S
	}

	subject := parsedURL.Query().Get("subject")
	if len(subject) == 0 {
		logger.Fatalf("cannot initialize remoteURL for -remoteWrite.url=%q: %s", remoteWriteURL, "subject is blank")
	}
	natsServerURL := fmt.Sprintf("%s://%s", scheme, host)
	client := natClient{
		sanitizedURL:   sanitizedURL,
		remoteWriteURL: natsServerURL,
		subject:        subject,
		useVMProto:     false,
		concurrency:    concurrency,
		fq:             fq,
		userName:       userName,
		password:       password,
		stopCh:         make(chan struct{}),
	}

	client.sendBlock = client.publishNatsBlock
	return &client
}

func (c *natClient) MustStop() {
	close(c.stopCh)
	c.wg.Wait()
	logger.Infof("stopped client for -remoteWrite.url=%q", c.sanitizedURL)
}

func (c *natClient) Init(argIdx int, concurrency int, sanitizedURL string) {
	if bytesPerSec := rateLimit.GetOptionalArg(argIdx); bytesPerSec > 0 {
		logger.Infof("applying %d bytes per second rate limit for -remoteWrite.url=%q", bytesPerSec, sanitizedURL)
		c.rl.perSecondLimit = int64(bytesPerSec)
	}
	c.rl.limitReached = metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_rate_limit_reached_total{url=%q}`, c.sanitizedURL))

	c.bytesSent = metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_bytes_sent_total{url=%q}`, c.sanitizedURL))
	c.blocksSent = metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_blocks_sent_total{url=%q}`, c.sanitizedURL))
	c.rateLimit = metrics.GetOrCreateGauge(fmt.Sprintf(`vmagent_remotewrite_rate_limit{url=%q}`, c.sanitizedURL), func() float64 {
		return float64(rateLimit.GetOptionalArg(argIdx))
	})
	c.requestDuration = metrics.GetOrCreateHistogram(fmt.Sprintf(`vmagent_remotewrite_duration_seconds{url=%q}`, c.sanitizedURL))
	c.requestsOKCount = metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_requests_total{url=%q, status_code="2XX"}`, c.sanitizedURL))
	c.errorsCount = metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_errors_total{url=%q}`, c.sanitizedURL))
	c.packetsDropped = metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_packets_dropped_total{url=%q}`, c.sanitizedURL))
	c.retriesCount = metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_retries_count_total{url=%q}`, c.sanitizedURL))
	c.sendDuration = metrics.GetOrCreateFloatCounter(fmt.Sprintf(`vmagent_remotewrite_send_duration_seconds_total{url=%q}`, c.sanitizedURL))
	metrics.GetOrCreateGauge(fmt.Sprintf(`vmagent_remotewrite_queues{url=%q}`, c.sanitizedURL), func() float64 {
		return float64(*queues)
	})

	for i := 0; i < concurrency; i++ {
		c.wg.Add(1)
		opts := []nats.Option{nats.Name("NATS Metrics Publisher")}
		if len(c.userName) > 0 && len(c.password) > 0 {
			option := nats.UserInfo(c.userName, c.password)
			opts = append(opts, option)
		}
		nc, err := nats.Connect(c.remoteWriteURL, opts...)
		if err != nil {
			logger.Panicf("cannot initialize connection to nats server config for -remoteWrite.url=%q: %s", c.remoteWriteURL, err)
		}
		go func() {
			defer c.wg.Done()
			c.runWorker(nc)
		}()
	}
	logger.Infof("initialized client for -remoteWrite.url=%q", c.sanitizedURL)
}

func (c *natClient) runWorker(nc *nats.Conn) {
	var ok bool
	var block []byte
	ch := make(chan bool, 1)
	for {
		block, ok = c.fq.MustReadBlock(block[:0])
		if !ok {
			return
		}
		go func() {
			startTime := time.Now()
			ch <- c.sendBlock(nc, block)
			c.sendDuration.Add(time.Since(startTime).Seconds())
		}()
		select {
		case ok := <-ch:
			if ok {
				// The block has been sent successfully
				continue
			}
			// Return unsent block to the queue.
			c.fq.MustWriteBlockIgnoreDisabledPQ(block)
			return
		case <-c.stopCh:
			// c must be stopped. Wait for a while in the hope the block will be sent.
			graceDuration := 5 * time.Second
			select {
			case ok := <-ch:
				if !ok {
					// Return unsent block to the queue.
					c.fq.MustWriteBlockIgnoreDisabledPQ(block)
				}
			case <-time.After(graceDuration):
				// Return unsent block to the queue.
				c.fq.MustWriteBlockIgnoreDisabledPQ(block)
			}
			return
		}
	}
}

// sendBlockHTTP sends the given block to c.remoteWriteURL.
//
// The function returns false only if c.stopCh is closed.
// Otherwise it tries sending the block to remote storage indefinitely.
func (c *natClient) publishNatsBlock(con *nats.Conn, block []byte) bool {
	c.rl.register(len(block), c.stopCh)
	maxRetryDuration := timeutil.AddJitterToDuration(time.Minute)
	retryDuration := timeutil.AddJitterToDuration(time.Second)
	retriesCount := 0
	maxRetriesCount := 10
again:
	startTime := time.Now()
	err := con.Publish(c.subject, block)
	c.requestDuration.UpdateDuration(startTime)

	if err != nil {
		c.errorsCount.Inc()
		retryDuration *= 2
		if retryDuration > maxRetryDuration {
			retryDuration = maxRetryDuration
		}
		logger.Warnf("couldn't send a block with size %d bytes to %q: %s during retry $%d; re-sending the block in %.3f seconds",
			len(block), c.sanitizedURL, err, retriesCount, retryDuration.Seconds())
		t := timerpool.Get(retryDuration)
		select {
		case <-c.stopCh:
			timerpool.Put(t)
			return false
		case <-t.C:
			timerpool.Put(t)
		}
		metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_requests_total{url=%q,protocal="nat",status_code="500"}`, c.sanitizedURL)).Inc()
		c.retriesCount.Inc()
		retriesCount++
		if retriesCount > maxRetriesCount {
			logger.Warnf("couldn't send a block with size %d bytes to %q: %s during retry $%d; retriesCount reach the maxRetries #%d, skip re-send",
				len(block), c.sanitizedURL, err, retriesCount, maxRetriesCount)
			c.packetsDropped.Inc()
			return true
		}
		goto again
	} else {
		metrics.GetOrCreateCounter(fmt.Sprintf(`vmagent_remotewrite_requests_total{url=%q,protocal="nat",status_code="200"}`, c.sanitizedURL)).Inc()
		return true
	}
}

func (c *natClient) UseVMProto() bool {
	//TODO implement me
	return c.useVMProto
}

func parseNatsURL(uri string) (u *url.URL, err error) {
	return url.Parse(uri)
}

func getNatsAuthConfig(argIdx int) (*promauth.BasicAuthConfig, error) {
	username := basicAuthUsername.GetOptionalArg(argIdx)
	password := basicAuthPassword.GetOptionalArg(argIdx)
	var basicAuthCfg *promauth.BasicAuthConfig
	if username != "" || password != "" {
		basicAuthCfg = &promauth.BasicAuthConfig{
			Username: username,
			Password: promauth.NewSecret(password),
		}
	}
	return basicAuthCfg, nil
}
