package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	cachemgr "github.com/jetersen/lfsproxy/cache"
	"github.com/jetersen/lfsproxy/config"
	"github.com/jetersen/lfsproxy/exporter"
	lfsmanifest "github.com/jetersen/lfsproxy/manifest"
)

func newTestLFSHandler(cfg *config.Config, c *MockCache, aws *MockAWSService, prom *exporter.LFSProxyCollector) LFSHandler {
	u, _ := url.Parse(cfg.UpstreamHost)
	concurrency := cfg.S3Concurrency
	if concurrency == 0 {
		concurrency = 64
	}
	manifest, _ := cachemgr.NewManifestCache(context.Background(), aws, 1*time.Hour)
	return LFSHandler{
		baseCtx:        context.Background(),
		cache:          c,
		manifest:       manifest,
		presignCache:   func() *cachemgr.PresignCache { c, _ := cachemgr.NewPresignCache(); return c }(),
		promCollector:  prom,
		config:         cfg,
		awsService:     aws,
		upstreamURL:    u,
		metaClient:     http.DefaultClient,
		transferClient: http.DefaultClient,
		s3Semaphore:    make(chan struct{}, concurrency),
	}
}

type MockCache struct {
	mu      sync.Mutex
	cache   map[string][]byte
	keysHit []string
}

func NewMockCache() *MockCache {
	return &MockCache{cache: make(map[string][]byte)}
}

func (m *MockCache) Get(key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.cache[key]
	if !ok {
		return nil, errors.New("Entry not found")
	}
	m.keysHit = append(m.keysHit, key)
	return data, nil
}

func (m *MockCache) Set(key string, entry []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache[key] = entry
	return nil
}

func (m *MockCache) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cache, key)
	return nil
}

func (m *MockCache) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.cache)
	m.keysHit = nil
}

func (m *MockCache) KeysHitLen() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.keysHit)
}

type MockAWSService struct {
	mu           sync.Mutex
	urls         map[string]string
	savedOIDs    []string
	uploadCalled atomic.Bool
}

func NewMockAWSService() *MockAWSService {
	return &MockAWSService{urls: make(map[string]string)}
}

func (m *MockAWSService) OIDExists(_ context.Context, oid string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.urls[oid]
	return ok, nil
}

func (m *MockAWSService) PresignObject(_ context.Context, oid string, _ time.Duration) (string, time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.urls[oid], time.Now().Add(time.Hour), nil
}

func (m *MockAWSService) UploadOID(_ context.Context, _ string, _ io.ReadCloser) error {
	m.uploadCalled.Store(true)
	return nil
}

func (m *MockAWSService) GetDirectURL(oid string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if url, ok := m.urls[oid]; ok {
		return url
	}
	return "https://s3.example.com/" + oid
}

func (m *MockAWSService) LoadManifest(_ context.Context) ([]string, bool, error) {
	return nil, false, nil
}

func (m *MockAWSService) SaveManifest(_ context.Context, oids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.savedOIDs = append([]string(nil), oids...)
	return nil
}

func (m *MockAWSService) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadCalled.Store(false)
	m.savedOIDs = nil
	clear(m.urls)
}

func (m *MockAWSService) SavedOIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.savedOIDs...)
}

func TestLFSHandler(t *testing.T) {
	cfg := &config.Config{
		UpstreamHost:  "https://fake-git-server.com",
		CacheEviction: 1 * time.Minute,
	}

	cache := NewMockCache()
	mockAWSService := NewMockAWSService()

	lfsHandler := newTestLFSHandler(cfg, cache, mockAWSService, exporter.NewCollector(false))

	t.Run("it should get from upstream", func(t *testing.T) {
		defer cache.Reset()
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("POST", "https://fake-git-server.com/org/repo.git/info/lfs/objects/batch",
			func(req *http.Request) (*http.Response, error) {
				resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
					"transfer": "basic",
					"objects": []map[string]interface{}{
						{
							"oid":           "1111111",
							"size":          123,
							"authenticated": true,
							"actions": map[string]interface{}{
								"download": map[string]interface{}{
									"href": "https://some-download.com",
									"header": map[string]interface{}{
										"Key": "value",
									},
									"expires_at": "2016-11-10T15:29:07Z",
								},
							},
						},
					},
					"hash_algo": "sha256",
				})
				return resp, err
			},
		)

		batchRequest := BatchRequest{
			Operation: "download",
			Transfers: []string{"basic"},
			Objects: []*BatchObjectResponse{
				{
					OID:  "123",
					Size: 123,
				},
				{
					OID:  "asd1234",
					Size: 123,
				},
			},
			Ref:      map[string]string{"name": "refs/heads/main"},
			HashAlgo: "sha256",
		}

		batchResponse, statusCode, rawBody, err := lfsHandler.getFromUpstream(context.TODO(), batchRequest, "/org/repo.git/info/lfs/objects/batch", http.Header{})
		assert.NoError(t, err)
		assert.Nil(t, rawBody)
		assert.Equal(t, 200, statusCode)
		assert.Equal(t, "basic", batchResponse.Transfer)
	})

	t.Run("it should return all responses from S3 without calling upstream", func(t *testing.T) {
		defer cache.Reset()
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("POST", "https://fake-git-server.com/org/repo.git/info/lfs/objects/batch",
			func(req *http.Request) (*http.Response, error) {
				assert.FailNow(t, "should not call upstream")
				return nil, nil
			},
		)

		mockAWSService.mu.Lock()
		mockAWSService.urls["123"] = "https://fake-url.com"
		mockAWSService.mu.Unlock()
		lfsHandler.manifest.Add("123")

		w := httptest.NewRecorder()
		c, r := gin.CreateTestContext(w)

		r.POST("/*path", lfsHandler.PostBatch)

		var jsonData = []byte(`{
			"operation": "download",
			"transfers": [ "basic" ],
			"ref": { "name": "refs/heads/main" },
			"objects": [
				{
					"oid": "123",
					"size": 123
				}
			],
			"hash_algo": "sha256"
		}`)

		var err error
		c.Request, err = http.NewRequest("POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		c.Request.Header.Set("Content-Type", "application/vnd.git-lfs+json")

		r.ServeHTTP(w, c.Request)

		b, err := io.ReadAll(w.Body)
		assert.NoError(t, err)

		assert.Equal(t, 200, w.Code)
		assert.Equal(t, 0, cache.KeysHitLen())

		assert.Equal(t, `{"objects":[{"oid":"123","size":123,"actions":{"download":{"href":"https://fake-url.com","expires_at":"0001-01-01T00:00:00Z"}}}]}`, string(b))
	})

	t.Run("it should return a mix of S3 and upstream responses", func(t *testing.T) {
		defer cache.Reset()
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		// "123" is in S3 — served directly, no upstream call needed for it.
		mockAWSService.mu.Lock()
		mockAWSService.urls["123"] = "https://fake-url.com"
		mockAWSService.mu.Unlock()
		lfsHandler.manifest.Add("123")

		httpmock.RegisterResponder("POST", "https://fake-git-server.com/org/repo.git/info/lfs/objects/batch",
			func(req *http.Request) (*http.Response, error) {
				resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
					"transfer": "basic",
					"objects": []map[string]interface{}{
						{
							"oid":           "1234",
							"size":          123,
							"authenticated": true,
							"actions": map[string]interface{}{
								"download": map[string]interface{}{
									"href": "https://some-download.com",
									"header": map[string]interface{}{
										"Key": "value",
									},
									"expires_at": "2016-11-10T15:29:07Z",
								},
							},
						},
					},
					"hash_algo": "sha256",
				})
				return resp, err
			},
		)

		httpmock.RegisterResponder("GET", "https://some-download.com", httpmock.NewStringResponder(200, ""))

		w := httptest.NewRecorder()
		c, r := gin.CreateTestContext(w)

		r.POST("/*path", lfsHandler.PostBatch)

		var jsonData = []byte(`{
			"operation": "download",
			"transfers": [ "basic" ],
			"ref": { "name": "refs/heads/main" },
			"objects": [
				{
					"oid": "123",
					"size": 123
				},
				{
					"oid": "1234",
					"size": 123
				}
			],
			"hash_algo": "sha256"
		}`)

		var err error
		c.Request, err = http.NewRequest("POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		c.Request.Header.Set("Content-Type", "application/vnd.git-lfs+json")

		r.ServeHTTP(w, c.Request)

		b, err := io.ReadAll(w.Body)
		assert.NoError(t, err)

		assert.Equal(t, 200, w.Code)
		assert.Equal(t, 0, cache.KeysHitLen())

		var resp BatchResponse
		assert.NoError(t, json.Unmarshal(b, &resp))
		assert.Len(t, resp.Objects, 2)

		byOID := map[string]*BatchObjectResponse{}
		for _, o := range resp.Objects {
			byOID[o.OID] = o
		}
		assert.Equal(t, "https://fake-url.com", byOID["123"].Actions["download"].Href)
		assert.Equal(t, "https://some-download.com", byOID["1234"].Actions["download"].Href)

		assert.Eventually(t, func() bool {
			return mockAWSService.uploadCalled.Load()
		}, 1*time.Second, 100*time.Millisecond)
	})

	t.Run("it should return both objects from S3 without calling upstream", func(t *testing.T) {
		// Drain background goroutines from previous test before resetting shared state
		time.Sleep(100 * time.Millisecond)
		cache.Reset()
		mockAWSService.Reset()
		defer cache.Reset()
		defer mockAWSService.Reset()

		mockAWSService.mu.Lock()
		mockAWSService.urls["123"] = "https://fake-url.com"
		mockAWSService.urls["1234"] = "https://this-is-from-s3.com"
		mockAWSService.mu.Unlock()
		lfsHandler.manifest.Add("1234")

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		upstreamCalls := 0
		httpmock.RegisterResponder("POST", "https://fake-git-server.com/org/repo.git/info/lfs/objects/batch",
			func(req *http.Request) (*http.Response, error) {
				upstreamCalls++
				return httpmock.NewJsonResponse(200, map[string]interface{}{})
			},
		)

		w := httptest.NewRecorder()
		c, r := gin.CreateTestContext(w)

		r.POST("/*path", lfsHandler.PostBatch)

		var jsonData = []byte(`{
			"operation": "download",
			"transfers": [ "basic" ],
			"ref": { "name": "refs/heads/main" },
			"objects": [
				{
					"oid": "123",
					"size": 123
				},
				{
					"oid": "1234",
					"size": 123
				}
			],
			"hash_algo": "sha256"
		}`)

		var err error
		c.Request, err = http.NewRequest("POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		c.Request.Header.Set("Content-Type", "application/vnd.git-lfs+json")

		r.ServeHTTP(w, c.Request)

		b, err := io.ReadAll(w.Body)
		assert.NoError(t, err)

		assert.Equal(t, 200, w.Code)
		assert.Equal(t, 0, upstreamCalls, "upstream should not be called for objects already in S3")
		assert.False(t, mockAWSService.uploadCalled.Load())

		var resp BatchResponse
		assert.NoError(t, json.Unmarshal(b, &resp))
		assert.Len(t, resp.Objects, 2)

		byOID := map[string]*BatchObjectResponse{}
		for _, o := range resp.Objects {
			byOID[o.OID] = o
		}
		assert.Equal(t, "https://fake-url.com", byOID["123"].Actions["download"].Href)
		assert.Equal(t, "https://this-is-from-s3.com", byOID["1234"].Actions["download"].Href)
	})

	t.Run("it should reject requests for disallowed orgs", func(t *testing.T) {
		restrictedHandler := newTestLFSHandler(&config.Config{
			UpstreamHost:  "https://fake-git-server.com",
			CacheEviction: 1 * time.Minute,
			AllowedOrgs:   []string{"allowed-org"},
		}, cache, mockAWSService, lfsHandler.promCollector)

		w := httptest.NewRecorder()
		c, r := gin.CreateTestContext(w)

		r.POST("/allowed-org/*path", restrictedHandler.PostBatch)

		var jsonData = []byte(`{
			"operation": "download",
			"transfers": [ "basic" ],
			"ref": { "name": "refs/heads/main" },
			"objects": [{"oid": "123", "size": 123}],
			"hash_algo": "sha256"
		}`)

		var err error
		c.Request, err = http.NewRequest("POST", "http://localhost:9999/blocked-org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		r.ServeHTTP(w, c.Request)
		assert.Equal(t, 404, w.Code)
	})

	t.Run("it should allow requests for permitted orgs", func(t *testing.T) {
		defer cache.Reset()
		defer mockAWSService.Reset()

		restrictedHandler := newTestLFSHandler(&config.Config{
			UpstreamHost:  "https://fake-git-server.com",
			CacheEviction: 1 * time.Minute,
			AllowedOrgs:   []string{"allowed-org"},
		}, cache, mockAWSService, lfsHandler.promCollector)

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("POST", "https://fake-git-server.com/allowed-org/repo.git/info/lfs/objects/batch",
			func(req *http.Request) (*http.Response, error) {
				resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
					"transfer": "basic",
					"objects": []map[string]interface{}{
						{
							"oid":           "123",
							"size":          123,
							"authenticated": true,
							"actions": map[string]interface{}{
								"download": map[string]interface{}{
									"href":       "https://some-download.com",
									"expires_at": "2016-11-10T15:29:07Z",
								},
							},
						},
					},
				})
				return resp, err
			},
		)

		httpmock.RegisterResponder("GET", "https://some-download.com", httpmock.NewStringResponder(200, ""))

		w := httptest.NewRecorder()
		c, r := gin.CreateTestContext(w)

		r.POST("/allowed-org/*path", restrictedHandler.PostBatch)

		var jsonData = []byte(`{
			"operation": "download",
			"transfers": [ "basic" ],
			"ref": { "name": "refs/heads/main" },
			"objects": [{"oid": "123", "size": 123}],
			"hash_algo": "sha256"
		}`)

		var err error
		c.Request, err = http.NewRequest("POST", "http://localhost:9999/allowed-org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		r.ServeHTTP(w, c.Request)
		assert.Equal(t, 200, w.Code)
	})

	t.Run("it should pass through error objects from upstream", func(t *testing.T) {
		defer cache.Reset()
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("POST", "https://fake-git-server.com/org/repo.git/info/lfs/objects/batch",
			func(req *http.Request) (*http.Response, error) {
				resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
					"objects": []map[string]interface{}{
						{
							"oid":  "nonexistent",
							"size": 1,
							"error": map[string]interface{}{
								"code":    404,
								"message": "Object does not exist on the server",
							},
						},
					},
				})
				return resp, err
			},
		)

		w := httptest.NewRecorder()
		c, r := gin.CreateTestContext(w)

		r.POST("/*path", lfsHandler.PostBatch)

		var jsonData = []byte(`{
			"operation": "download",
			"transfers": [ "basic" ],
			"ref": { "name": "refs/heads/main" },
			"objects": [{"oid": "nonexistent", "size": 1}],
			"hash_algo": "sha256"
		}`)

		var err error
		c.Request, err = http.NewRequest("POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		r.ServeHTTP(w, c.Request)
		assert.Equal(t, 200, w.Code)

		var batchResp BatchResponse
		err = json.Unmarshal(w.Body.Bytes(), &batchResp)
		assert.NoError(t, err)
		assert.Len(t, batchResp.Objects, 1)
		assert.Equal(t, "nonexistent", batchResp.Objects[0].OID)
		assert.NotNil(t, batchResp.Objects[0].Error)
		assert.Equal(t, 404, batchResp.Objects[0].Error.Code)
		assert.Equal(t, "Object does not exist on the server", batchResp.Objects[0].Error.Message)
	})

	t.Run("it should cache error responses and serve them without hitting upstream", func(t *testing.T) {
		defer cache.Reset()
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		upstreamCalls := 0
		httpmock.RegisterResponder("POST", "https://fake-git-server.com/org/repo.git/info/lfs/objects/batch",
			func(req *http.Request) (*http.Response, error) {
				upstreamCalls++
				resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
					"objects": []map[string]interface{}{
						{
							"oid":  "dead-oid",
							"size": 1,
							"error": map[string]interface{}{
								"code":    404,
								"message": "Object does not exist on the server",
							},
						},
					},
				})
				return resp, err
			},
		)

		w := httptest.NewRecorder()
		c, r := gin.CreateTestContext(w)
		r.POST("/*path", lfsHandler.PostBatch)

		jsonData := []byte(`{
			"operation": "download",
			"transfers": [ "basic" ],
			"ref": { "name": "refs/heads/main" },
			"objects": [{"oid": "dead-oid", "size": 1}],
			"hash_algo": "sha256"
		}`)

		var err error
		c.Request, err = http.NewRequest("POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)
		r.ServeHTTP(w, c.Request)

		assert.Equal(t, 200, w.Code)
		assert.Equal(t, 1, upstreamCalls)

		var batchResp BatchResponse
		err = json.Unmarshal(w.Body.Bytes(), &batchResp)
		assert.NoError(t, err)
		assert.Len(t, batchResp.Objects, 1)
		assert.Equal(t, 404, batchResp.Objects[0].Error.Code)

		// Second request should come from cache, not upstream
		w2 := httptest.NewRecorder()
		c2, r2 := gin.CreateTestContext(w2)
		r2.POST("/*path", lfsHandler.PostBatch)
		c2.Request, err = http.NewRequest("POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)
		r2.ServeHTTP(w2, c2.Request)

		assert.Equal(t, 200, w2.Code)
		assert.Equal(t, 1, upstreamCalls, "upstream should not be called again for cached error")

		var batchResp2 BatchResponse
		err = json.Unmarshal(w2.Body.Bytes(), &batchResp2)
		assert.NoError(t, err)
		assert.Len(t, batchResp2.Objects, 1)
		assert.Equal(t, "dead-oid", batchResp2.Objects[0].OID)
		assert.Equal(t, 404, batchResp2.Objects[0].Error.Code)
	})

	t.Run("it should handle mix of found and not-found objects", func(t *testing.T) {
		defer cache.Reset()
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("POST", "https://fake-git-server.com/org/repo.git/info/lfs/objects/batch",
			func(req *http.Request) (*http.Response, error) {
				resp, err := httpmock.NewJsonResponse(200, map[string]interface{}{
					"transfer": "basic",
					"objects": []map[string]interface{}{
						{
							"oid":           "found-oid",
							"size":          100,
							"authenticated": true,
							"actions": map[string]interface{}{
								"download": map[string]interface{}{
									"href":       "https://some-download.com",
									"expires_at": "2016-11-10T15:29:07Z",
								},
							},
						},
						{
							"oid":  "missing-oid",
							"size": 1,
							"error": map[string]interface{}{
								"code":    404,
								"message": "Object does not exist on the server",
							},
						},
					},
				})
				return resp, err
			},
		)

		httpmock.RegisterResponder("GET", "https://some-download.com", httpmock.NewStringResponder(200, ""))

		w := httptest.NewRecorder()
		c, r := gin.CreateTestContext(w)

		r.POST("/*path", lfsHandler.PostBatch)

		var jsonData = []byte(`{
			"operation": "download",
			"transfers": [ "basic" ],
			"ref": { "name": "refs/heads/main" },
			"objects": [
				{"oid": "found-oid", "size": 100},
				{"oid": "missing-oid", "size": 1}
			],
			"hash_algo": "sha256"
		}`)

		var err error
		c.Request, err = http.NewRequest("POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		r.ServeHTTP(w, c.Request)
		assert.Equal(t, 200, w.Code)

		var batchResp BatchResponse
		err = json.Unmarshal(w.Body.Bytes(), &batchResp)
		assert.NoError(t, err)
		assert.Len(t, batchResp.Objects, 2)

		var foundObj, missingObj *BatchObjectResponse
		for _, obj := range batchResp.Objects {
			switch obj.OID {
			case "found-oid":
				foundObj = obj
			case "missing-oid":
				missingObj = obj
			}
		}

		assert.NotNil(t, foundObj)
		assert.NotNil(t, foundObj.Actions["download"])

		assert.NotNil(t, missingObj)
		assert.NotNil(t, missingObj.Error)
		assert.Equal(t, 404, missingObj.Error.Code)
	})
}

func TestGetManifest(t *testing.T) {
	cfg := &config.Config{
		UpstreamHost:  "https://fake-git-server.com",
		CacheEviction: 1 * time.Minute,
	}

	cache := NewMockCache()
	mockAWSService := NewMockAWSService()
	lfsHandler := newTestLFSHandler(cfg, cache, mockAWSService, exporter.NewCollector(false))

	lfsHandler.manifest.Add("def")
	lfsHandler.manifest.Add("abc")

	w := httptest.NewRecorder()
	c, r := gin.CreateTestContext(w)
	r.GET("/manifest", lfsHandler.GetManifest)

	req, err := http.NewRequest("GET", "http://localhost:9999/manifest", nil)
	assert.NoError(t, err)

	c.Request = req
	r.ServeHTTP(w, c.Request)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/gzip", w.Header().Get("Content-Type"))
	assert.NotEmpty(t, w.Header().Get("ETag"))

	oids, err := lfsmanifest.Parse(bytes.NewReader(w.Body.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, []string{"abc", "def"}, oids)

	w2 := httptest.NewRecorder()
	c2, r2 := gin.CreateTestContext(w2)
	r2.GET("/manifest", lfsHandler.GetManifest)

	req2, err := http.NewRequest("GET", "http://localhost:9999/manifest", nil)
	assert.NoError(t, err)
	req2.Header.Set("If-None-Match", w.Header().Get("ETag"))

	c2.Request = req2
	r2.ServeHTTP(w2, c2.Request)

	assert.Equal(t, http.StatusNotModified, w2.Code)
	assert.Empty(t, w2.Body.Bytes())
}

func TestShutdownFlushesManifest(t *testing.T) {
	cfg := &config.Config{
		UpstreamHost:  "https://fake-git-server.com",
		CacheEviction: 1 * time.Minute,
	}

	cache := NewMockCache()
	mockAWSService := NewMockAWSService()
	lfsHandler := newTestLFSHandler(cfg, cache, mockAWSService, exporter.NewCollector(false))
	lfsHandler.manifest.Add("shutdown-oid")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	lfsHandler.Shutdown(ctx)

	assert.ElementsMatch(t, []string{"shutdown-oid"}, mockAWSService.SavedOIDs())
}
