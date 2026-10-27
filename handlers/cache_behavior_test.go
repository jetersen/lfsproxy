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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jetersen/lfsproxy/config"
	"github.com/jetersen/lfsproxy/exporter"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fillAWS struct {
	*MockAWSService
	upload func(context.Context, string, io.ReadCloser) error
}

func (s *fillAWS) UploadOID(ctx context.Context, oid string, body io.ReadCloser) error {
	return s.upload(ctx, oid, body)
}

func cacheTestHandler() *LFSHandler {
	h := newTestLFSHandler(&config.Config{UpstreamHost: "https://upstream.invalid", S3Concurrency: 1}, NewMockAWSService(), exporter.NewCollector(false))
	h.transferClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("payload")), Request: r}, nil
	})}
	return &h
}

func batchCall(t *testing.T, h *LFSHandler, ctx context.Context, path, auth string, batch BatchRequest) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(batch)
	require.NoError(t, err)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, path, bytes.NewReader(data))
	c.Request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	c.Request.Header.Set("Authorization", auth)
	h.PostBatch(c)
	return w
}

func fillObject(oid string) BatchObjectResponse {
	return BatchObjectResponse{OID: oid, Size: 7, Actions: map[string]*BatchObjectActionResponse{
		"download": {Href: "https://download.invalid/object", Header: map[string]string{"X-Download-Key": "test"}},
	}}
}

func TestFillLimitCoversUploadAndForwardsHeaders(t *testing.T) {
	h := cacheTestHandler()
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	defer func() { close(release); h.uploadWG.Wait() }()
	h.transferClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		assert.Equal(t, "test", r.Header.Get("X-Download-Key"))
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("payload")), Header: make(http.Header), Request: r}, nil
	})}
	h.awsService = &fillAWS{MockAWSService: NewMockAWSService(), upload: func(ctx context.Context, _ string, _ io.ReadCloser) error {
		deadline, ok := ctx.Deadline()
		assert.True(t, ok)
		assert.LessOrEqual(t, time.Until(deadline), 10*time.Minute)
		started <- struct{}{}
		<-release
		return nil
	}}
	for i := range 4 {
		require.True(t, h.startCacheFill(fillObject(fmt.Sprint(i))))
	}
	for range 4 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("admitted upload did not start")
		}
	}
	// GET has already completed, but the slots must remain held by uploads.
	require.False(t, h.startCacheFill(fillObject("overflow")))
	require.False(t, h.manifest.Contains("0"))
}

func TestFailedFillReleasesSlot(t *testing.T) {
	h := cacheTestHandler()
	h.awsService = &fillAWS{MockAWSService: NewMockAWSService(), upload: func(context.Context, string, io.ReadCloser) error {
		return errors.New("synthetic upload failure")
	}}
	for range 8 {
		require.True(t, h.startCacheFill(fillObject("retry")))
		h.uploadWG.Wait()
		require.False(t, h.manifest.Contains("retry"))
	}
}

func TestFillCancelledWithServiceContext(t *testing.T) {
	h := cacheTestHandler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.baseCtx = ctx
	started := make(chan struct{})
	h.awsService = &fillAWS{MockAWSService: NewMockAWSService(), upload: func(ctx context.Context, _ string, _ io.ReadCloser) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	require.True(t, h.startCacheFill(fillObject("cancelled")))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upload did not start")
	}
	cancel()
	done := make(chan struct{})
	go func() { h.uploadWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("upload ignored service cancellation")
	}
	require.False(t, h.manifest.Contains("cancelled"))
	require.False(t, h.startCacheFill(fillObject("after-cancel")))
}

func TestSaturatedFillsStillReturnUpstreamAction(t *testing.T) {
	h := cacheTestHandler()
	for range cap(h.fillSlots) {
		h.fillSlots <- struct{}{}
	}
	obj := fillObject("uncached")
	h.metaClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, err := json.Marshal(BatchResponse{Objects: []*BatchObjectResponse{&obj}})
		require.NoError(t, err)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header), Request: r}, nil
	})}
	w := batchCall(t, h, context.Background(), "/org/repo/objects/batch", "", BatchRequest{Operation: "download", Objects: []*BatchObjectResponse{{OID: obj.OID, Size: obj.Size}}})
	require.Equal(t, 200, w.Code)
	var response BatchResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.Objects, 1)
	require.Equal(t, obj.Actions, response.Objects[0].Actions)
	require.False(t, h.awsService.(*MockAWSService).uploadCalled.Load())
}

func TestNonDownloadOperationsRejected(t *testing.T) {
	for _, operation := range []string{"upload", "", "unsupported"} {
		t.Run(operation, func(t *testing.T) {
			h := cacheTestHandler()
			h.config.UpstreamToken = "application-token"
			h.manifest.Add("cached")
			var calls atomic.Int32
			h.metaClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("must not contact upstream")
			})}
			w := batchCall(t, h, context.Background(), "/org/repo/objects/batch", "Bearer caller", BatchRequest{Operation: operation, Objects: []*BatchObjectResponse{{OID: "cached", Size: 7}, {OID: "uncached", Size: 7}}})
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), "Only download operations are supported")
			require.Zero(t, calls.Load())
			require.Zero(t, h.awsService.(*MockAWSService).presignCalls.Load())
			require.Zero(t, h.awsService.(*MockAWSService).directCalls.Load())
			require.Zero(t, len(h.fillSlots))
		})
	}
}

func TestErrorsDoNotSuppressOtherRepositoriesOrRecovery(t *testing.T) {
	for _, code := range []int{401, 403, 404, 429, 500} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			h := cacheTestHandler()
			var calls int
			h.metaClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				obj := &BatchObjectResponse{OID: "same", Size: 7}
				if calls <= 2 {
					obj.Error = &BatchObjectErrorResponse{Code: code, Message: r.URL.Path}
				}
				body, err := json.Marshal(BatchResponse{Objects: []*BatchObjectResponse{obj}})
				require.NoError(t, err)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header), Request: r}, nil
			})}
			batch := BatchRequest{Operation: "download", Objects: []*BatchObjectResponse{{OID: "same", Size: 7}}}
			for _, repo := range []string{"A", "B", "A"} {
				w := batchCall(t, h, context.Background(), "/org/"+repo+"/objects/batch", "Bearer "+repo, batch)
				require.Equal(t, 200, w.Code)
				var response BatchResponse
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
				require.Len(t, response.Objects, 1)
				if calls <= 2 {
					require.Equal(t, "/org/"+repo+"/objects/batch", response.Objects[0].Error.Message)
				} else {
					require.Nil(t, response.Objects[0].Error)
				}
			}
			require.Equal(t, 3, calls)
			h.manifest.Add("same")
			w := batchCall(t, h, context.Background(), "/org/A/objects/batch", "Bearer A", batch)
			require.Contains(t, w.Body.String(), `"download"`)
			require.Equal(t, 3, calls)
		})
	}
}

func TestCancelledHeadAdmissionReturnsPromptly(t *testing.T) {
	h := cacheTestHandler()
	h.s3Semaphore <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan int, 1)
	go func() {
		w := batchCall(t, h, ctx, "/org/repo/objects/batch", "", BatchRequest{Operation: "download", Objects: []*BatchObjectResponse{{OID: "uncached"}}})
		done <- w.Code
	}()
	select {
	case status := <-done:
		require.Equal(t, http.StatusRequestTimeout, status)
	case <-time.After(time.Second):
		<-h.s3Semaphore
		t.Fatal("cancelled request remained blocked on HEAD admission")
	}
}
