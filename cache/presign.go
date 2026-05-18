package cache

import (
	"time"

	"github.com/maypok86/otter"
)

const presignRefreshBuffer = 10 * time.Minute

type PresignCache struct {
	c otter.CacheWithVariableTTL[string, string]
}

func NewPresignCache() (*PresignCache, error) {
	c, err := otter.MustBuilder[string, string](10_000).
		WithVariableTTL().
		Build()
	if err != nil {
		return nil, err
	}
	return &PresignCache{c: c}, nil
}

func (p *PresignCache) Get(oid string) (string, bool) {
	return p.c.Get(oid)
}

func (p *PresignCache) Set(oid, url string, expiresAt time.Time) {
	ttl := time.Until(expiresAt) - presignRefreshBuffer
	if ttl <= 0 {
		return
	}
	p.c.Set(oid, url, ttl)
}
