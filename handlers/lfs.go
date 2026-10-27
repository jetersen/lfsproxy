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
	presignCache   *cache.PresignCache
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
	errorCache, err := cache.NewCache(cfg.CacheEviction)
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

	presignCache, err := cache.NewPresignCache()
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
		presignCache:   presignCache,
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

	ctx := c.Request.Context()

	resp := BatchResponse{Objects: make([]*BatchObjectResponse, 0, len(batchRequest.Objects))}

	// Phase 1: error cache + manifest — both synchronous in-memory lookups.
	// Manifest hits return S3 download URLs immediately; misses go to upstream.
	var upstreamObjects []*BatchObjectResponse

	for _, object := range batchRequest.Objects {
		if data, err := l.cache.Get(object.OID); err == nil {
			var cached BatchObjectResponse
			if json.Unmarshal(data, &cached) == nil && cached.Error != nil {
				l.promCollector.CacheHits.Add(1)
				resp.Objects = append(resp.Objects, &cached)
				continue
			}
		}

		if l.manifest.Contains(object.OID) {
			href, err := l.downloadURL(ctx, object.OID)
			if err != nil {
				log.Printf("presign error for %v: %v\n", object.OID, err)
				upstreamObjects = append(upstreamObjects, object)
				continue
			}
			l.promCollector.CacheHits.Add(1)
			resp.Objects = append(resp.Objects, &BatchObjectResponse{
				OID:  object.OID,
				Size: object.Size,
				Actions: map[string]*BatchObjectActionResponse{
					"download": {Href: href},
				},
			})
			continue
		}

		upstreamObjects = append(upstreamObjects, object)
	}

	if len(upstreamObjects) == 0 {
		c.JSON(200, resp)
		return
	}

	// Phase 2: parallel S3 HeadObject for manifest misses.
	// Catches OIDs uploaded by other instances whose manifest hasn't synced yet.
	var (
		mu           sync.Mutex
		trueUpstream []*BatchObjectResponse
		wg           sync.WaitGroup
	)
	for _, object := range upstreamObjects {
		wg.Add(1)
		go func(obj *BatchObjectResponse) {
			defer wg.Done()
			l.s3Semaphore <- struct{}{}
			exists, err := l.awsService.OIDExists(ctx, obj.OID)
			<-l.s3Semaphore
			if err == nil && exists {
				l.manifest.Add(obj.OID)
				href, presignErr := l.downloadURL(ctx, obj.OID)
				mu.Lock()
				defer mu.Unlock()
				if presignErr != nil {
					trueUpstream = append(trueUpstream, obj)
					return
				}
				l.promCollector.CacheHits.Add(1)
				resp.Objects = append(resp.Objects, &BatchObjectResponse{
					OID:  obj.OID,
					Size: obj.Size,
					Actions: map[string]*BatchObjectActionResponse{
						"download": {Href: href},
					},
				})
				return
			}
			l.promCollector.CacheMiss.Add(1)
			mu.Lock()
			trueUpstream = append(trueUpstream, obj)
			mu.Unlock()
		}(object)
	}
	wg.Wait()

	if len(trueUpstream) == 0 {
		c.JSON(200, resp)
		return
	}

	// Phase 3: upstream for true misses.
	// Returns pre-signed GitHub CDN URLs the client can fetch directly.
	upstreamRequest := BatchRequest{
		Operation: batchRequest.Operation,
		Transfers: batchRequest.Transfers,
		Ref:       batchRequest.Ref,
		HashAlgo:  batchRequest.HashAlgo,
		Objects:   trueUpstream,
	}
	upstreamBatchResponse, statusCode, rawBody, err := l.getFromUpstream(ctx, upstreamRequest, c.Request.URL.Path, c.Request.Header)
	if err != nil {
		c.AbortWithStatusJSON(statusCode, gin.H{"message": err.Error()})
		return
	}
	if rawBody != nil {
		c.Data(statusCode, "application/vnd.git-lfs+json", rawBody)
		return
	}

	for _, obj := range upstreamBatchResponse.Objects {
		if obj.Error != nil {
			if err := l.cacheError(obj.OID, *obj); err != nil {
				log.Printf("error caching error response %v\n", err.Error())
			}
			resp.Objects = append(resp.Objects, obj)
			continue
		}
		if _, ok := obj.Actions["download"]; !ok {
			resp.Objects = append(resp.Objects, obj)
			continue
		}
		go l.startCacheFill(*obj)
		l.promCollector.S3Miss.Add(1)
		resp.Objects = append(resp.Objects, obj)
	}

	c.JSON(200, resp)
}

func (l *LFSHandler) downloadURL(ctx context.Context, oid string) (string, error) {
	if !l.config.S3PresignEnabled {
		return l.awsService.GetDirectURL(oid), nil
	}
	if url, ok := l.presignCache.Get(oid); ok {
		return url, nil
	}
	url, expiresAt, err := l.awsService.PresignObject(ctx, oid, l.config.PresignTTL)
	if err != nil {
		return "", err
	}
	l.presignCache.Set(oid, url, expiresAt)
	return url, nil
}

// startCacheFill downloads an OID from its upstream URL and uploads it to S3
// so future requests can be served from S3 directly.
func (l *LFSHandler) startCacheFill(obj BatchObjectResponse) {
	l.s3Semaphore <- struct{}{}
	defer func() { <-l.s3Semaphore }()

	req, err := http.NewRequestWithContext(l.baseCtx, http.MethodGet, obj.Actions["download"].Href, nil) //nolint:gosec
	if err != nil {
		return
	}
	resp, err := l.transferClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		if err == nil {
			_ = resp.Body.Close()
		}
		return
	}
	l.uploadWG.Add(1)
	go l.pushToS3(obj, resp.Body)
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
	defer func() { _ = resp.Body.Close() }()

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
