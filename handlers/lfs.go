package handlers

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/gin-gonic/gin"
	"github.com/jetersen/lfsproxy/cache"
	"github.com/jetersen/lfsproxy/config"
	"github.com/jetersen/lfsproxy/exporter"
	"github.com/jetersen/lfsproxy/services"
)

type LFSHandler struct {
	cache         cache.Cache
	promCollector *exporter.LFSProxyCollector
	awsService    services.AWSService
	config        *config.Config
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

	return &LFSHandler{
		cache:         cache,
		promCollector: exporter.NewCollector(),
		config:        cfg,
		awsService:    awsService,
	}, nil
}

func (l LFSHandler) PostBatch(c *gin.Context) {
	log.Printf("incoming: %s %s", c.Request.Method, c.Request.URL.Path)

	if !strings.HasSuffix(c.Request.URL.Path, "/objects/batch") {
		c.AbortWithStatus(404)
		return
	}

	var batchRequest BatchRequest
	if err := c.ShouldBindJSON(&batchRequest); err != nil {
		c.AbortWithError(500, err) //nolint:errcheck
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

	for _, object := range batchRequest.Objects {
		data, err := l.cache.Get(object.OID)
		if err == nil {
			l.promCollector.CacheHits.Add(1)
			var cachedBatchObjectResponse BatchObjectResponse
			if err := json.Unmarshal(data, &cachedBatchObjectResponse); err == nil {
				if l.config.S3PresignEnabled {
					go l.checkCachedLink(object.OID, cachedBatchObjectResponse.Actions["download"].HeadHref)
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
		log.Printf("forwarding %d objects to upstream", len(modifiedBatchRequest.Objects))
		upstreamBatchResponse, statusCode, err := l.getFromUpstream(ctx, modifiedBatchRequest, c.Request.URL.Path, c.Request.Header)
		if err != nil {
			c.AbortWithError(statusCode, err) //nolint:errcheck
			return
		}

		finalBatchResponse.Transfer = upstreamBatchResponse.Transfer

		urls := make(chan BatchObjectResponse)

		totalUrls := 0

		for _, obj := range upstreamBatchResponse.Objects {
			_, ok := obj.Actions["download"]
			if !ok {
				finalBatchResponse.Objects = append(finalBatchResponse.Objects, obj)
				continue
			}

			totalUrls++
			go l.pullS3(ctx, *obj, urls)
		}

		for range totalUrls {
			r := <-urls
			finalBatchResponse.Objects = append(finalBatchResponse.Objects, &r)
		}
	}

	c.JSON(200, finalBatchResponse)
}

func (l LFSHandler) getFromUpstream(ctx context.Context, batchRequest BatchRequest, urlPath string, headers http.Header) (*BatchResponse, int, error) {
	upstreamURL, err := url.Parse(l.config.UpstreamHost)
	if err != nil {
		return nil, 500, err
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(batchRequest)
	if err != nil {
		return nil, 500, err
	}

	fullURL := upstreamURL.Scheme + "://" + upstreamURL.Host + strings.TrimRight(upstreamURL.Path, "/") + urlPath
	req, err := http.NewRequestWithContext(ctx, "POST", fullURL, &buf)
	if err != nil {
		log.Printf("unexpected error creating request %v\n", err.Error())
		return nil, 500, err
	}

	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	if auth := headers.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	log.Printf("upstream request: %s %s (auth: %v)", req.Method, fullURL, headers.Get("Authorization") != "")
	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			log.Printf("redirect: %s -> %s", via[len(via)-1].URL, req.URL)
			return nil
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("unexpected error from upstream %v\n", err.Error())
		return nil, 500, err
	}

	if !resp.Uncompressed && strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		var err error
		if resp.Body, err = gzip.NewReader(resp.Body); err != nil {
			log.Printf("unexpected error uncompressing response %v\n", err.Error())
			return nil, 500, err
		}
	}

	if resp.StatusCode != 200 {
		respBytes, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, errors.New(string(respBytes))
	}

	var upstreamBatchResponse BatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&upstreamBatchResponse); err != nil {
		return nil, 500, err
	}

	return &upstreamBatchResponse, resp.StatusCode, nil
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
		resp, err := http.Get(batchResp.Actions["download"].Href) //nolint:gosec
		if err == nil && resp.StatusCode == 200 {
			go l.pushToS3(obj, resp.Body)
		}
		l.promCollector.S3Miss.Add(1)
	}
	urls <- batchResp
}

func (l LFSHandler) pushToS3(obj BatchObjectResponse, body io.ReadCloser) {
	ctx := context.Background()

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
	r, err := http.DefaultClient.Head(headHref) //nolint:gosec
	if err != nil {
		log.Printf("error checking cached link for %v: %v\n", oid, err.Error())
		return
	}
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
