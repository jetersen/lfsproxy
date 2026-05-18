//go:build integration

package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
)

// TestUpstreamBatchDownloadAction verifies what GitHub returns in the LFS batch
// download action — specifically whether the href is pre-authenticated (signed
// CDN URL) or requires client credentials, and whether auth headers are included.
//
// Required env vars:
//
//	LFS_UPSTREAM_URL  e.g. https://github.com
//	LFS_TOKEN         GitHub personal access token (read:lfs scope)
//	LFS_REPO_PATH     e.g. owner/repo (without .git)
//	LFS_OID           SHA-256 OID of an object known to exist in the repo
//	LFS_OID_SIZE      size in bytes of the object
func TestUpstreamBatchDownloadAction(t *testing.T) {
	upstreamURL := requireEnv(t, "LFS_UPSTREAM_URL")
	token := requireEnv(t, "LFS_TOKEN")
	repoPath := requireEnv(t, "LFS_REPO_PATH")
	oid := requireEnv(t, "LFS_OID")
	var size int64
	if _, err := fmt.Sscanf(os.Getenv("LFS_OID_SIZE"), "%d", &size); err != nil {
		t.Fatalf("LFS_OID_SIZE missing or not an integer: %v", err)
	}

	batchURL := fmt.Sprintf("%s/%s.git/info/lfs/objects/batch", upstreamURL, repoPath)

	payload := map[string]any{
		"operation": "download",
		"transfers": []string{"basic"},
		"objects": []map[string]any{
			{"oid": oid, "size": size},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, batchURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	t.Logf("HTTP status: %d", resp.StatusCode)

	var result any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	pretty, _ := json.MarshalIndent(result, "", "  ")
	t.Logf("Response:\n%s", pretty)

	// Extract and inspect the download action for the OID.
	type action struct {
		Href      string            `json:"href"`
		Header    map[string]string `json:"header"`
		ExpiresAt string            `json:"expires_at"`
		ExpiresIn int               `json:"expires_in"`
	}
	type object struct {
		OID           string            `json:"oid"`
		Authenticated bool              `json:"authenticated"`
		Actions       map[string]action `json:"actions"`
		Error         *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	type batchResp struct {
		Transfer string   `json:"transfer"`
		Objects  []object `json:"objects"`
	}

	var parsed batchResp
	raw, _ := json.Marshal(result)
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("re-parse: %v", err)
	}

	for _, obj := range parsed.Objects {
		if obj.Error != nil {
			t.Errorf("OID %s error %d: %s", obj.OID, obj.Error.Code, obj.Error.Message)
			continue
		}
		dl, ok := obj.Actions["download"]
		if !ok {
			t.Errorf("OID %s: no download action", obj.OID)
			continue
		}
		t.Logf("--- OID: %s", obj.OID)
		t.Logf("  authenticated: %v", obj.Authenticated)
		t.Logf("  href: %s", dl.Href)
		t.Logf("  headers: %v", dl.Header)
		t.Logf("  expires_at: %s  expires_in: %d", dl.ExpiresAt, dl.ExpiresIn)

		if len(dl.Header) == 0 {
			t.Log("  -> No auth headers in action. href may be pre-signed or unauthenticated.")
		} else {
			t.Log("  -> Auth headers present — client needs to forward these when downloading.")
		}
	}
}

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("skipping: %s not set", key)
	}
	return v
}
