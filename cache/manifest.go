package cache

import (
	"bytes"
	"context"
	"hash/fnv"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	lfsmanifest "github.com/jetersen/lfsproxy/manifest"
)

// ManifestStore is implemented by AWSService — avoids import cycle.
type ManifestStore interface {
	LoadManifest(ctx context.Context) (oids []string, bootstrapped bool, err error)
	SaveManifest(ctx context.Context, oids []string) error
}

// ManifestCache is an in-memory OID presence set backed by an S3 manifest object.
// OIDs in S3 are permanent so entries never expire.
type ManifestCache struct {
	mu             sync.RWMutex
	oids           map[string]struct{}
	dirty          bool
	cachedManifest []byte
	cachedETag     string
	store          ManifestStore
	saveWG         sync.WaitGroup // tracks in-flight saves so Flush can wait
}

func NewManifestCache(ctx context.Context, store ManifestStore, saveInterval time.Duration) (*ManifestCache, error) {
	mc := &ManifestCache{
		oids:  make(map[string]struct{}),
		store: store,
	}

	oids, bootstrapped, err := store.LoadManifest(ctx)
	if err != nil {
		return nil, err
	}
	for _, oid := range oids {
		mc.oids[oid] = struct{}{}
	}
	if bootstrapped {
		mc.dirty = true
		log.Printf("manifest: .lfsproxy-manifest not found — built from S3 ListObjectsV2 (%d OIDs), will persist on next save\n", len(mc.oids))
	} else {
		log.Printf("manifest: loaded %d OIDs from .lfsproxy-manifest\n", len(mc.oids))
	}

	go mc.periodicSave(ctx, saveInterval)
	return mc, nil
}

func (m *ManifestCache) Contains(oid string) bool {
	m.mu.RLock()
	_, ok := m.oids[oid]
	m.mu.RUnlock()
	return ok
}

func (m *ManifestCache) Add(oid string) {
	m.mu.Lock()
	if _, exists := m.oids[oid]; !exists {
		m.oids[oid] = struct{}{}
		m.dirty = true
		m.cachedManifest = nil
		m.cachedETag = ""
	}
	m.mu.Unlock()
}

func (m *ManifestCache) SnapshotBytes() ([]byte, string, error) {
	m.mu.RLock()
	if m.cachedManifest != nil {
		data := m.cachedManifest
		etag := m.cachedETag
		m.mu.RUnlock()
		return data, etag, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cachedManifest != nil {
		return m.cachedManifest, m.cachedETag, nil
	}

	oids := make([]string, 0, len(m.oids))
	for oid := range m.oids {
		oids = append(oids, oid)
	}
	sort.Strings(oids)
	etag := manifestETag(oids)

	var buf bytes.Buffer
	if err := lfsmanifest.Write(&buf, oids); err != nil {
		return nil, "", err
	}

	m.cachedManifest = buf.Bytes()
	m.cachedETag = etag
	return m.cachedManifest, etag, nil
}

func manifestETag(oids []string) string {
	h := fnv.New64a()
	for _, oid := range oids {
		_, _ = h.Write([]byte(oid))
		_, _ = h.Write([]byte{'\n'})
	}
	return `"` + strconv.FormatUint(h.Sum64(), 16) + `"`
}

func (m *ManifestCache) periodicSave(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.saveWG.Add(1)
			go func() {
				defer m.saveWG.Done()
				m.save(context.Background())
			}()
			return
		case <-t.C:
			m.mu.RLock()
			dirty := m.dirty
			m.mu.RUnlock()
			if dirty {
				m.save(ctx)
			}
		}
	}
}

func (m *ManifestCache) Flush(ctx context.Context) {
	// Wait for any in-flight save started by periodicSave on ctx.Done.
	m.saveWG.Wait()
	// Save again if dirty (e.g. new OIDs added after periodicSave's last run).
	m.mu.RLock()
	dirty := m.dirty
	m.mu.RUnlock()
	if dirty {
		m.save(ctx)
	}
}

func (m *ManifestCache) save(ctx context.Context) {
	m.mu.Lock()
	snapshot := make([]string, 0, len(m.oids))
	for oid := range m.oids {
		snapshot = append(snapshot, oid)
	}
	m.dirty = false
	m.mu.Unlock()

	if err := m.store.SaveManifest(ctx, snapshot); err != nil {
		log.Printf("manifest: save error: %v\n", err)
		m.mu.Lock()
		m.dirty = true
		m.mu.Unlock()
	} else {
		log.Printf("manifest: saved %d OIDs\n", len(snapshot))
	}
}
