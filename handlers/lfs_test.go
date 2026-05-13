package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/jetersen/lfsproxy/config"
	"github.com/jetersen/lfsproxy/exporter"
	"github.com/stretchr/testify/assert"
)

func newTestLFSHandler(cfg *config.Config, c *MockCache, aws *MockAWSService, prom *exporter.LFSProxyCollector) LFSHandler {
	u, _ := url.Parse(cfg.UpstreamHost)
	return LFSHandler{
		cache:          c,
		promCollector:  prom,
		config:         cfg,
		awsService:     aws,
		upstreamURL:    u,
		metaClient:     http.DefaultClient,
		transferClient: http.DefaultClient,
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

func (m *MockAWSService) GetOIDPreSignedURL(_ context.Context, oid string) (string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	url := m.urls[oid]
	return url, url, nil
}

func (m *MockAWSService) UploadOID(_ context.Context, _ string, _ io.ReadCloser) error {
	m.uploadCalled.Store(true)
	return nil
}

func (m *MockAWSService) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadCalled.Store(false)
	clear(m.urls)
}

func TestLFSHandler(t *testing.T) {
	cfg := &config.Config{
		UpstreamHost:  "https://fake-git-server.com",
		CacheEviction: 1 * time.Minute,
	}

	cache := NewMockCache()
	mockAWSService := NewMockAWSService()

	lfsHandler := newTestLFSHandler(cfg, cache, mockAWSService, exporter.NewCollector())

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

	t.Run("it should return all cached responses", func(t *testing.T) {
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

		now := time.Now()
		obj := BatchObjectResponse{
			OID:           "123",
			Size:          123,
			Authenticated: false,
			Actions: map[string]*BatchObjectActionResponse{
				"download": {
					Href:     "https://fake-url.com",
					HeadHref: "https://fake-url.com",
					Header: map[string]string{
						"Content-Type": "application/octet-stream",
					},
					ExpiresIn: 0,
					ExpiresAt: now,
				},
			},
		}

		if data, err := json.Marshal(obj); err == nil {
			cache.Set("123", data)
		}

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
		assert.Equal(t, 1, cache.KeysHitLen())

		expected := fmt.Sprintf(`{"objects":[{"oid":"123","size":123,"actions":{"download":{"href":"https://fake-url.com","head_href":"https://fake-url.com","header":{"Content-Type":"application/octet-stream"},"expires_at":"%v"}}}]}`, now.Format(time.RFC3339Nano))

		assert.Equal(t, expected, string(b))
	})

	t.Run("it should return a mix of cached and upstream responses - with no URLs from S3", func(t *testing.T) {
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

		now := time.Now()
		obj := BatchObjectResponse{
			OID:           "123",
			Size:          123,
			Authenticated: false,
			Actions: map[string]*BatchObjectActionResponse{
				"download": {
					Href:     "https://fake-url.com",
					HeadHref: "https://fake-url.com",
					Header: map[string]string{
						"Content-Type": "application/octet-stream",
					},
					ExpiresIn: 0,
					ExpiresAt: now,
				},
			},
		}

		if data, err := json.Marshal(obj); err == nil {
			cache.Set("123", data)
		}

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
		assert.Equal(t, 1, cache.KeysHitLen())

		expected := fmt.Sprintf(`{"transfer":"basic","objects":[{"oid":"123","size":123,"actions":{"download":{"href":"https://fake-url.com","head_href":"https://fake-url.com","header":{"Content-Type":"application/octet-stream"},"expires_at":"%v"}}},{"oid":"1234","size":123,"actions":{"download":{"href":"https://some-download.com","header":{"Key":"value"},"expires_at":"2016-11-10T15:29:07Z"}},"authenticated":true}]}`, now.Format(time.RFC3339Nano))

		assert.Equal(t, expected, string(b))

		assert.Eventually(t, func() bool {
			return mockAWSService.uploadCalled.Load()
		}, 1*time.Second, 100*time.Millisecond)
	})

	t.Run("it should return a mix of cached and upstream responses - with URLs from S3", func(t *testing.T) {
		// Drain background goroutines from previous test before resetting shared state
		time.Sleep(100 * time.Millisecond)
		cache.Reset()
		mockAWSService.Reset()
		defer cache.Reset()
		defer mockAWSService.Reset()

		mockAWSService.mu.Lock()
		mockAWSService.urls["1234"] = "https://this-is-from-s3.com"
		mockAWSService.mu.Unlock()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

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

		now := time.Now()
		obj := BatchObjectResponse{
			OID:           "123",
			Size:          123,
			Authenticated: false,
			Actions: map[string]*BatchObjectActionResponse{
				"download": {
					Href:     "https://fake-url.com",
					HeadHref: "https://fake-url.com",
					Header: map[string]string{
						"Content-Type": "application/octet-stream",
					},
					ExpiresIn: 0,
					ExpiresAt: now,
				},
			},
		}

		if data, err := json.Marshal(obj); err == nil {
			cache.Set("123", data)
		}

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
		assert.Equal(t, 1, cache.KeysHitLen())

		expected := fmt.Sprintf(`{"transfer":"basic","objects":[{"oid":"123","size":123,"actions":{"download":{"href":"https://fake-url.com","head_href":"https://fake-url.com","header":{"Content-Type":"application/octet-stream"},"expires_at":"%v"}}},{"oid":"1234","size":123,"actions":{"download":{"href":"https://this-is-from-s3.com","head_href":"https://this-is-from-s3.com","header":{"Key":"value"},"expires_at":"2016-11-10T15:29:07Z"}},"authenticated":true}]}`, now.Format(time.RFC3339Nano))

		assert.Equal(t, expected, string(b))

		assert.False(t, mockAWSService.uploadCalled.Load())
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
