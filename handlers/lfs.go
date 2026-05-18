package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jetersen/lfsproxy/cache"
	"github.com/jetersen/lfsproxy/config"
	"github.com/jetersen/lfsproxy/exporter"
	"github.com/jetersen/lfsproxy/services"
)

type LFSHandler struct {
	baseCtx        context.Context
	cache          cache.Cache
	manifest       *cache.ManifestCache
	promCollector  *exporter.LFSProxyCollector
	awsService     services.AWSService
	config         *config.Config
	upstreamURL    *url.URL
	metaClient     *http.Client
	transferClient *http.Client
	s3Semaphore    chan struct{}
	uploadWG       sync.WaitGroup
}

func NewLFSHandler(ctx context.Context, cfg *config.Config) (*LFSHandler, error) {
	errorCache, err := cache.NewCache(ctx, cfg.CacheEviction)
	if err != nil {
		return nil, err
	}

	awsService, err := services.NewAWSService(ctx, cfg.S3Bucket, cfg.S3UseAccelerate)
	if err != nil {
		return nil, err
	}

	manifest, err := cache.NewManifestCache(ctx, awsService, cfg.ManifestSaveInterval)
	if err != nil {
		return nil, err
	}

	upstreamURL, err := url.Parse(cfg.UpstreamHost)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		MaxIdleConnsPerHost:   cfg.S3Concurrency,
	}

	return &LFSHandler{
		baseCtx:        ctx,
		cache:          errorCache,
		manifest:       manifest,
		promCollector:  exporter.NewCollector(cfg.EnablePrometheusExporter),
		config:         cfg,
		awsService:     awsService,
		upstreamURL:    upstreamURL,
		metaClient:     &http.Client{Timeout: 30 * time.Second, Transport: transport},
		transferClient: &http.Client{Transport: transport},
		s3Semaphore:    make(chan struct{}, cfg.S3Concurrency),
	}, nil
}

func (l *LFSHandler) PostBatch(c *gin.Context) {
	if !strings.HasSuffix(c.Request.URL.Path, "/objects/batch") {
		c.AbortWithStatus(404)
		return
	}

	var batchRequest BatchRequest
	if err := c.ShouldBindJSON(&batchRequest); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"message": err.Error()})
		return
	}

	finalBatchResponse := BatchResponse{
		Objects:  []*BatchObjectResponse{},
		Transfer: preferredTransfer(batchRequest.Transfers, "basic"),
	}
	ctx := c.Request.Context()

	// needS3Check: objects not resolved by error cache or manifest.
	needS3Check := make([]*BatchObjectResponse, 0, len(batchRequest.Objects))

	// Phase 1: error cache (bigcache) + manifest — both are synchronous in-memory lookups.
	for _, object := range batchRequest.Objects {
		// Error cache: stores upstream error responses (e.g. 404s) to avoid hammering upstream.
		if data, err := l.cache.Get(object.OID); err == nil {
			var cached BatchObjectResponse
			if json.Unmarshal(data, &cached) == nil && cached.Error != nil {
				l.promCollector.CacheHits.Add(1)
				finalBatchResponse.Objects = append(finalBatchResponse.Objects, &cached)
				continue
			}
		}

		// Manifest: OID confirmed in S3. Return direct URL immediately — no HeadObject needed.
		if l.manifest.Contains(object.OID) {
			l.promCollector.CacheHits.Add(1)
			finalBatchResponse.Objects = append(finalBatchResponse.Objects, &BatchObjectResponse{
				OID:  object.OID,
				Size: object.Size,
				Actions: map[string]*BatchObjectActionResponse{
					"download": {Href: l.awsService.GetDirectURL(object.OID)},
				},
			})
			continue
		}

		l.promCollector.CacheMiss.Add(1)
		needS3Check = append(needS3Check, object)
	}

	if len(needS3Check) == 0 {
		c.JSON(200, finalBatchResponse)
		return
	}

	// Phase 2: parallel S3 HeadObject for objects not in manifest.
	// Avoids upstream calls (and 429s) for objects already in S3 after a cold start.
	type s3PreCheck struct {
		obj    *BatchObjectResponse
		exists bool
	}
	preCheckCh := make(chan s3PreCheck, len(needS3Check))

	for _, obj := range needS3Check {
		go func(o *BatchObjectResponse) {
			l.s3Semaphore <- struct{}{}
			defer func() { <-l.s3Semaphore }()
			exists, err := l.awsService.OIDExists(ctx, o.OID)
			if err != nil {
				log.Printf("error checking S3 for %v: %v\n", o.OID, err)
				exists = false
			}
			preCheckCh <- s3PreCheck{obj: o, exists: exists}
		}(obj)
	}

	upstreamOnlyRequest := BatchRequest{
		Operation: batchRequest.Operation,
		Transfers: batchRequest.Transfers,
		Ref:       batchRequest.Ref,
		HashAlgo:  batchRequest.HashAlgo,
		Objects:   []*BatchObjectResponse{},
	}

	for range len(needS3Check) {
		result := <-preCheckCh
		if result.exists {
			l.manifest.Add(result.obj.OID)
			l.promCollector.S3Hits.Add(1)
			finalBatchResponse.Objects = append(finalBatchResponse.Objects, &BatchObjectResponse{
				OID:  result.obj.OID,
				Size: result.obj.Size,
				Actions: map[string]*BatchObjectActionResponse{
					"download": {Href: l.awsService.GetDirectURL(result.obj.OID)},
				},
			})
		} else {
			upstreamOnlyRequest.Objects = append(upstreamOnlyRequest.Objects, result.obj)
		}
	}

	if len(upstreamOnlyRequest.Objects) == 0 {
		c.JSON(200, finalBatchResponse)
		return
	}

	// Phase 3: call upstream only for true S3 misses.
	upstreamBatchResponse, statusCode, rawBody, err := l.getFromUpstream(ctx, upstreamOnlyRequest, c.Request.URL.Path, c.Request.Header)
	if err != nil {
		c.AbortWithStatusJSON(statusCode, gin.H{"message": err.Error()})
		return
	}
	if rawBody != nil {
		c.Data(statusCode, "application/vnd.git-lfs+json", rawBody)
		return
	}

	// Prefer any custom transfer the client advertised over "basic".
	// lfsproxy always returns S3 URLs, so if the client has a custom
	// transfer agent (e.g. "s3") it should be used for the download.
	finalBatchResponse.Transfer = preferredTransfer(batchRequest.Transfers, upstreamBatchResponse.Transfer)

	urls := make(chan BatchObjectResponse, len(upstreamBatchResponse.Objects))
	totalUrls := 0

	for _, obj := range upstreamBatchResponse.Objects {
		if obj.Error != nil {
			if err := l.cacheError(obj.OID, *obj); err != nil {
				log.Printf("error caching error response %v\n", err.Error())
			}
			finalBatchResponse.Objects = append(finalBatchResponse.Objects, obj)
			continue
		}

		_, ok := obj.Actions["download"]
		if !ok {
			finalBatchResponse.Objects = append(finalBatchResponse.Objects, obj)
			continue
		}

		totalUrls++
		go func(o BatchObjectResponse) {
			l.s3Semaphore <- struct{}{}
			defer func() { <-l.s3Semaphore }()
			l.pullS3(ctx, o, urls)
		}(*obj)
	}

	for range totalUrls {
		r := <-urls
		finalBatchResponse.Objects = append(finalBatchResponse.Objects, &r)
	}

	c.JSON(200, finalBatchResponse)
}

func (l *LFSHandler) GetManifest(c *gin.Context) {
	data, etag, err := l.manifest.SnapshotBytes()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}
	if match := c.GetHeader("If-None-Match"); match != "" && match == etag {
		c.Status(http.StatusNotModified)
		return
	}

	c.Header("ETag", etag)
	c.Header("Cache-Control", "max-age=300")
	c.Data(http.StatusOK, "application/gzip", data)
}

func (l *LFSHandler) Shutdown(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		l.uploadWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		log.Printf("shutdown: timed out waiting for S3 uploads: %v\n", ctx.Err())
	}

	l.manifest.Flush(ctx)
}

func (l *LFSHandler) getFromUpstream(ctx context.Context, batchRequest BatchRequest, urlPath string, headers http.Header) (*BatchResponse, int, []byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(batchRequest); err != nil {
		return nil, 500, nil, err
	}

	fullURL := l.upstreamURL.Scheme + "://" + l.upstreamURL.Host + strings.TrimRight(l.upstreamURL.Path, "/") + urlPath
	req, err := http.NewRequestWithContext(ctx, "POST", fullURL, &buf)
	if err != nil {
		log.Printf("unexpected error creating request %v\n", err.Error())
		return nil, 500, nil, err
	}

	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	if l.config.UpstreamToken != "" {
		req.Header.Set("Authorization", "Bearer "+l.config.UpstreamToken)
	} else if auth := headers.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := l.metaClient.Do(req)
	if err != nil {
		log.Printf("unexpected error from upstream %v\n", err.Error())
		return nil, 500, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBytes, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, respBytes, nil
	}

	var upstreamBatchResponse BatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&upstreamBatchResponse); err != nil {
		return nil, 500, nil, err
	}

	return &upstreamBatchResponse, resp.StatusCode, nil, nil
}

// pullS3 is called only for confirmed S3 misses. Downloads from upstream URL and
// triggers async upload to S3, after which the OID is added to the manifest.
func (l *LFSHandler) pullS3(ctx context.Context, obj BatchObjectResponse, urls chan<- BatchObjectResponse) {
	batchResp := BatchObjectResponse{
		OID:           obj.OID,
		Size:          obj.Size,
		Authenticated: obj.Authenticated,
		Actions:       obj.Actions,
	}

	req, err := http.NewRequestWithContext(l.baseCtx, http.MethodGet, batchResp.Actions["download"].Href, nil) //nolint:gosec
	if err == nil {
		resp, err := l.transferClient.Do(req)
		if err == nil && resp.StatusCode == 200 {
			l.uploadWG.Add(1)
			go l.pushToS3(obj, resp.Body)
		} else if err == nil {
			resp.Body.Close()
		}
	}
	l.promCollector.S3Miss.Add(1)
	urls <- batchResp
}

func (l *LFSHandler) pushToS3(obj BatchObjectResponse, body io.ReadCloser) {
	defer l.uploadWG.Done()
	if err := l.awsService.UploadOID(context.Background(), obj.OID, body); err != nil {
		log.Printf("error uploading to S3: %v\n", err.Error())
		return
	}
	l.manifest.Add(obj.OID)
}

func (l *LFSHandler) cacheError(key string, obj BatchObjectResponse) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return l.cache.Set(key, data)
}

// preferredTransfer returns the first custom transfer from the client's list
// (skipping git-lfs built-ins: basic, multipart-basic, ssh), falling back to
// the upstream's transfer (typically "basic").
func preferredTransfer(clientTransfers []string, upstreamTransfer string) string {
	builtins := map[string]bool{"basic": true, "multipart-basic": true, "ssh": true, "lfs-standalone-file": true}
	for _, t := range clientTransfers {
		if !builtins[t] {
			return t
		}
	}
	return upstreamTransfer
}
