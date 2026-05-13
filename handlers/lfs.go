package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/gin-gonic/gin"
	"github.com/jetersen/lfsproxy/cache"
	"github.com/jetersen/lfsproxy/config"
	"github.com/jetersen/lfsproxy/exporter"
	"github.com/jetersen/lfsproxy/services"
)

type LFSHandler struct {
	baseCtx        context.Context
	cache          cache.Cache
	promCollector  *exporter.LFSProxyCollector
	awsService     services.AWSService
	config         *config.Config
	upstreamURL    *url.URL
	metaClient     *http.Client
	transferClient *http.Client
	s3Semaphore    chan struct{}
}

func NewLFSHandler(ctx context.Context, cfg *config.Config) (*LFSHandler, error) {
	cache, err := cache.NewCache(ctx, cfg.CacheEviction)
	if err != nil {
		return nil, err
	}

	awsService, err := services.NewAWSService(ctx, cfg.S3Bucket, cfg.S3UseAccelerate, cfg.S3PresignEnabled, cfg.S3PresignExpiration)
	if err != nil {
		return nil, err
	}

	upstreamURL, err := url.Parse(cfg.UpstreamHost)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		DialContext:            (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout:  30 * time.Second,
		MaxIdleConnsPerHost:    cfg.S3Concurrency,
	}

	return &LFSHandler{
		baseCtx:        ctx,
		cache:          cache,
		promCollector:  exporter.NewCollector(cfg.EnablePrometheusExporter),
		config:         cfg,
		awsService:     awsService,
		upstreamURL:    upstreamURL,
		metaClient:     &http.Client{Timeout: 30 * time.Second, Transport: transport},
		transferClient: &http.Client{Transport: transport},
		s3Semaphore:    make(chan struct{}, cfg.S3Concurrency),
	}, nil
}

func (l LFSHandler) PostBatch(c *gin.Context) {
	if !strings.HasSuffix(c.Request.URL.Path, "/objects/batch") {
		c.AbortWithStatus(404)
		return
	}

	var batchRequest BatchRequest
	if err := c.ShouldBindJSON(&batchRequest); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"message": err.Error()})
		return
	}

	modifiedBatchRequest := BatchRequest{
		Operation: batchRequest.Operation,
		Transfers: batchRequest.Transfers,
		Ref:       batchRequest.Ref,
		HashAlgo:  batchRequest.HashAlgo,
		Objects:   []*BatchObjectResponse{},
	}

	finalBatchResponse := BatchResponse{
		Objects: []*BatchObjectResponse{},
	}

	ctx := c.Request.Context()

	var cacheWg sync.WaitGroup
	for _, object := range batchRequest.Objects {
		data, err := l.cache.Get(object.OID)
		if err == nil {
			l.promCollector.CacheHits.Add(1)
			var cachedBatchObjectResponse BatchObjectResponse
			if err := json.Unmarshal(data, &cachedBatchObjectResponse); err == nil {
				if cachedBatchObjectResponse.Error == nil && l.config.S3PresignEnabled {
					cacheWg.Add(1)
					go func(oid, headHref string) {
						defer cacheWg.Done()
						l.s3Semaphore <- struct{}{}
						defer func() { <-l.s3Semaphore }()
						l.checkCachedLink(oid, headHref)
					}(object.OID, cachedBatchObjectResponse.Actions["download"].HeadHref)
				}
				finalBatchResponse.Objects = append(finalBatchResponse.Objects, &cachedBatchObjectResponse)
				continue
			}
		} else if errors.Is(err, bigcache.ErrEntryNotFound) {
			l.promCollector.CacheMiss.Add(1)
		}

		modifiedBatchRequest.Objects = append(modifiedBatchRequest.Objects, object)
	}

	if len(modifiedBatchRequest.Objects) > 0 {
		upstreamBatchResponse, statusCode, rawBody, err := l.getFromUpstream(ctx, modifiedBatchRequest, c.Request.URL.Path, c.Request.Header)
		if err != nil {
			c.AbortWithStatusJSON(statusCode, gin.H{"message": err.Error()})
			return
		}
		if rawBody != nil {
			c.Data(statusCode, "application/vnd.git-lfs+json", rawBody)
			return
		}

		finalBatchResponse.Transfer = upstreamBatchResponse.Transfer

		urls := make(chan BatchObjectResponse, len(upstreamBatchResponse.Objects))

		totalUrls := 0

		for _, obj := range upstreamBatchResponse.Objects {
			if obj.Error != nil {
				if err := l.cacheObjResponse(obj.OID, *obj); err != nil {
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
	}

	cacheWg.Wait()
	c.JSON(200, finalBatchResponse)
}

func (l LFSHandler) getFromUpstream(ctx context.Context, batchRequest BatchRequest, urlPath string, headers http.Header) (*BatchResponse, int, []byte, error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(batchRequest)
	if err != nil {
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

func (l LFSHandler) pullS3(ctx context.Context, obj BatchObjectResponse, urls chan<- BatchObjectResponse) {
	batchResp := BatchObjectResponse{
		OID:           obj.OID,
		Size:          obj.Size,
		Authenticated: obj.Authenticated,
		Actions:       obj.Actions,
	}
	objectAction := obj.Actions["download"]

	exists, err := l.awsService.OIDExists(ctx, obj.OID)
	if err != nil {
		log.Printf("error: %v\n", err.Error())
		urls <- batchResp
		return
	}

	if exists {
		url, headUrl, err := l.awsService.GetOIDPreSignedURL(ctx, obj.OID)
		if err != nil {
			log.Printf("error presigned: %v\n", err.Error())
			urls <- batchResp
			return
		}

		objectAction.Href = url
		objectAction.HeadHref = headUrl

		batchResp.Actions["download"] = objectAction
		if err := l.cacheObjResponse(obj.OID, batchResp); err != nil {
			log.Printf("error caching response %v\n", err.Error())
		}

		l.promCollector.S3Hits.Add(1)
	} else {
		resp, err := l.transferClient.Get(batchResp.Actions["download"].Href) //nolint:gosec
		if err == nil && resp.StatusCode == 200 {
			go l.pushToS3(obj, resp.Body)
		} else if err == nil {
			resp.Body.Close()
		}
		l.promCollector.S3Miss.Add(1)
	}
	urls <- batchResp
}

func (l LFSHandler) pushToS3(obj BatchObjectResponse, body io.ReadCloser) {
	ctx := l.baseCtx

	err := l.awsService.UploadOID(ctx, obj.OID, body)
	if err != nil {
		log.Printf("error uploading to S3: %v\n", err.Error())
		return
	}

	url, headUrl, err := l.awsService.GetOIDPreSignedURL(ctx, obj.OID)
	if err != nil {
		log.Printf("error getting presigned: %v\n", err.Error())
		return
	}

	cacheResp := BatchObjectResponse{
		OID:           obj.OID,
		Size:          obj.Size,
		Authenticated: obj.Authenticated,
		Actions: map[string]*BatchObjectActionResponse{
			"download": {
				Href:      url,
				HeadHref:  headUrl,
				Header:    obj.Actions["download"].Header,
				ExpiresIn: obj.Actions["download"].ExpiresIn,
				ExpiresAt: obj.Actions["download"].ExpiresAt,
			},
		},
	}

	if err := l.cacheObjResponse(obj.OID, cacheResp); err != nil {
		log.Printf("error pre-caching response %v\n", err.Error())
	}
}

func (l LFSHandler) checkCachedLink(oid string, headHref string) {
	r, err := l.metaClient.Head(headHref) //nolint:gosec
	if err != nil {
		log.Printf("error checking cached link for %v: %v\n", oid, err.Error())
		return
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		log.Printf("removing %v from cache due to expired presigned link: %v\n", headHref, r.StatusCode)
		l.cache.Delete(oid) //nolint:errcheck
	}
}

func (l LFSHandler) cacheObjResponse(key string, obj BatchObjectResponse) error {
	var err error
	var data []byte

	if data, err = json.Marshal(obj); err == nil {
		err = l.cache.Set(key, data)
	}

	return err
}
