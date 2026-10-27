package handlers

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/stretchr/testify/require"

	cachemgr "github.com/jetersen/lfsproxy/cache"
	"github.com/jetersen/lfsproxy/config"
	"github.com/jetersen/lfsproxy/exporter"
	lfsmanifest "github.com/jetersen/lfsproxy/manifest"
)

func newTestLFSHandler(cfg *config.Config, aws *MockAWSService, prom *exporter.LFSProxyCollector) LFSHandler {
	u, _ := url.Parse(cfg.UpstreamHost)
	concurrency := cfg.S3Concurrency
	if concurrency == 0 {
		concurrency = 64
	}
	manifest, _ := cachemgr.NewManifestCache(context.Background(), aws, 1*time.Hour)
	return LFSHandler{
		baseCtx:        context.Background(),
		manifest:       manifest,
		presignCache:   func() *cachemgr.PresignCache { c, _ := cachemgr.NewPresignCache(); return c }(),
		promCollector:  prom,
		config:         cfg,
		awsService:     aws,
		upstreamURL:    u,
		metaClient:     http.DefaultClient,
		transferClient: http.DefaultClient,
		s3Semaphore:    make(chan struct{}, concurrency),
		fillSlots:      make(chan struct{}, 4),
	}
}

type MockAWSService struct {
	mu           sync.Mutex
	urls         map[string]string
	savedOIDs    []string
	uploadCalled atomic.Bool
	presignCalls atomic.Int32
	directCalls  atomic.Int32
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
	m.presignCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.urls[oid], time.Now().Add(time.Hour), nil
}

func (m *MockAWSService) UploadOID(_ context.Context, _ string, _ io.ReadCloser) error {
	m.uploadCalled.Store(true)
	return nil
}

func (m *MockAWSService) GetDirectURL(oid string) string {
	m.directCalls.Add(1)
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
		S3PresignEnabled: true,
		UpstreamHost:     "https://fake-git-server.com",
	}

	mockAWSService := NewMockAWSService()

	lfsHandler := newTestLFSHandler(cfg, mockAWSService, exporter.NewCollector(false))

	t.Run("it should get from upstream", func(t *testing.T) {
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		defer lfsHandler.uploadWG.Wait()

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
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		defer lfsHandler.uploadWG.Wait()

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
		c.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		c.Request.Header.Set("Content-Type", "application/vnd.git-lfs+json")

		r.ServeHTTP(w, c.Request)

		b, err := io.ReadAll(w.Body)
		assert.NoError(t, err)

		assert.Equal(t, 200, w.Code)

		assert.Equal(t, `{"objects":[{"oid":"123","size":123,"actions":{"download":{"href":"https://fake-url.com","expires_at":"0001-01-01T00:00:00Z"}}}]}`, string(b))
	})

	t.Run("it should return a mix of S3 and upstream responses", func(t *testing.T) {
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		defer lfsHandler.uploadWG.Wait()

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
		c.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		c.Request.Header.Set("Content-Type", "application/vnd.git-lfs+json")

		r.ServeHTTP(w, c.Request)

		b, err := io.ReadAll(w.Body)
		assert.NoError(t, err)

		assert.Equal(t, 200, w.Code)

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
		mockAWSService.Reset()
		defer mockAWSService.Reset()

		mockAWSService.mu.Lock()
		mockAWSService.urls["123"] = "https://fake-url.com"
		mockAWSService.urls["1234"] = "https://this-is-from-s3.com"
		mockAWSService.mu.Unlock()
		lfsHandler.manifest.Add("1234")

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		defer lfsHandler.uploadWG.Wait()

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
		c.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
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
			UpstreamHost: "https://fake-git-server.com",
			AllowedOrgs:  []string{"allowed-org"},
		}, mockAWSService, lfsHandler.promCollector)

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
		c.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/blocked-org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		r.ServeHTTP(w, c.Request)
		assert.Equal(t, 404, w.Code)
	})

	t.Run("it should allow requests for permitted orgs", func(t *testing.T) {
		defer mockAWSService.Reset()

		restrictedHandler := newTestLFSHandler(&config.Config{
			UpstreamHost: "https://fake-git-server.com",
			AllowedOrgs:  []string{"allowed-org"},
		}, mockAWSService, lfsHandler.promCollector)

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		defer restrictedHandler.uploadWG.Wait()

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
		c.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/allowed-org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)

		r.ServeHTTP(w, c.Request)
		assert.Equal(t, 200, w.Code)
	})

	t.Run("it should pass through error objects from upstream", func(t *testing.T) {
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		defer lfsHandler.uploadWG.Wait()

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
		c.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
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

	t.Run("it should retry error responses instead of caching them", func(t *testing.T) {
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		defer lfsHandler.uploadWG.Wait()

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
		c.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)
		r.ServeHTTP(w, c.Request)

		assert.Equal(t, 200, w.Code)
		assert.Equal(t, 1, upstreamCalls)

		var batchResp BatchResponse
		err = json.Unmarshal(w.Body.Bytes(), &batchResp)
		assert.NoError(t, err)
		assert.Len(t, batchResp.Objects, 1)
		assert.Equal(t, 404, batchResp.Objects[0].Error.Code)

		// Retry goes upstream so a prior error cannot suppress recovery.
		w2 := httptest.NewRecorder()
		c2, r2 := gin.CreateTestContext(w2)
		r2.POST("/*path", lfsHandler.PostBatch)
		c2.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
		assert.NoError(t, err)
		r2.ServeHTTP(w2, c2.Request)

		assert.Equal(t, 200, w2.Code)
		assert.Equal(t, 2, upstreamCalls, "errors must remain retryable")

		var batchResp2 BatchResponse
		err = json.Unmarshal(w2.Body.Bytes(), &batchResp2)
		assert.NoError(t, err)
		assert.Len(t, batchResp2.Objects, 1)
		assert.Equal(t, "dead-oid", batchResp2.Objects[0].OID)
		assert.Equal(t, 404, batchResp2.Objects[0].Error.Code)
	})

	t.Run("it should handle mix of found and not-found objects", func(t *testing.T) {
		defer mockAWSService.Reset()

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		defer lfsHandler.uploadWG.Wait()

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
		c.Request, err = http.NewRequestWithContext(context.Background(), "POST", "http://localhost:9999/org/repo.git/info/lfs/objects/batch", bytes.NewBuffer(jsonData))
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

func TestCachedDownloadModes(t *testing.T) {
	for _, signed := range []bool{true, false} {
		for _, manifestHit := range []bool{true, false} {
			name := "unsigned"
			if signed {
				name = "presigned"
			}
			if manifestHit {
				name += "/manifest"
			} else {
				name += "/head-object"
			}
			t.Run(name, func(t *testing.T) {
				aws := NewMockAWSService()
				const href = "https://bucket.s3.eu-west-1.amazonaws.com/abc123"
				aws.urls["abc123"] = href
				cfg := &config.Config{UpstreamHost: "https://unused.invalid", S3PresignEnabled: signed}
				handler := newTestLFSHandler(cfg, aws, exporter.NewCollector(false))
				if manifestHit {
					handler.manifest.Add("abc123")
				}
				if !signed {
					// Disabling presigning must bypass even an existing cached signature.
					handler.presignCache.Set("abc123", "https://unused.invalid/old-signature", time.Now().Add(time.Hour))
				}
				for range 2 {
					w := httptest.NewRecorder()
					_, router := gin.CreateTestContext(w)
					router.POST("/*path", handler.PostBatch)
					req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/org/repo.git/info/lfs/objects/batch",
						bytes.NewBufferString(`{"operation":"download","transfers":["basic"],"objects":[{"oid":"abc123","size":123}]}`))
					req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
					router.ServeHTTP(w, req)
					require.Equal(t, http.StatusOK, w.Code)
					var response BatchResponse
					require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
					require.Len(t, response.Objects, 1)
					require.Contains(t, response.Objects[0].Actions, "download")
					assert.Equal(t, href, response.Objects[0].Actions["download"].Href)
				}
				if signed {
					assert.Equal(t, int32(1), aws.presignCalls.Load())
					assert.Zero(t, aws.directCalls.Load())
				} else {
					assert.Zero(t, aws.presignCalls.Load())
					assert.Equal(t, int32(2), aws.directCalls.Load())
				}
			})
		}
	}
}

func TestGetManifest(t *testing.T) {
	cfg := &config.Config{
		UpstreamHost: "https://fake-git-server.com",
	}

	mockAWSService := NewMockAWSService()
	lfsHandler := newTestLFSHandler(cfg, mockAWSService, exporter.NewCollector(false))

	lfsHandler.manifest.Add("def")
	lfsHandler.manifest.Add("abc")

	w := httptest.NewRecorder()
	c, r := gin.CreateTestContext(w)
	r.GET("/manifest", lfsHandler.GetManifest)

	req, err := http.NewRequestWithContext(context.Background(), "GET", "http://localhost:9999/manifest", nil)
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

	req2, err := http.NewRequestWithContext(context.Background(), "GET", "http://localhost:9999/manifest", nil)
	assert.NoError(t, err)
	req2.Header.Set("If-None-Match", w.Header().Get("ETag"))

	c2.Request = req2
	r2.ServeHTTP(w2, c2.Request)

	assert.Equal(t, http.StatusNotModified, w2.Code)
	assert.Empty(t, w2.Body.Bytes())
}

func TestShutdownFlushesManifest(t *testing.T) {
	cfg := &config.Config{
		UpstreamHost: "https://fake-git-server.com",
	}

	mockAWSService := NewMockAWSService()
	lfsHandler := newTestLFSHandler(cfg, mockAWSService, exporter.NewCollector(false))
	lfsHandler.manifest.Add("shutdown-oid")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	lfsHandler.Shutdown(ctx)

	assert.ElementsMatch(t, []string{"shutdown-oid"}, mockAWSService.SavedOIDs())
}
