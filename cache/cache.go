package cache

import (
	"fmt"
	"time"

	"github.com/maypok86/otter"
)

type Cache interface {
	Get(key string) ([]byte, error)
	Set(key string, entry []byte) error
	Delete(key string) error
}

type otterCache struct {
	c   otter.CacheWithVariableTTL[string, []byte]
	ttl time.Duration
}

func NewCache(ttl time.Duration) (Cache, error) {
	c, err := otter.MustBuilder[string, []byte](10_000).
		WithVariableTTL().
		Build()
	if err != nil {
		return nil, err
	}
	return &otterCache{c: c, ttl: ttl}, nil
}

func (o *otterCache) Get(key string) ([]byte, error) {
	val, ok := o.c.Get(key)
	if !ok {
		return nil, fmt.Errorf("miss")
	}
	return val, nil
}

func (o *otterCache) Set(key string, entry []byte) error {
	o.c.Set(key, entry, o.ttl)
	return nil
}

func (o *otterCache) Delete(key string) error {
	o.c.Delete(key)
	return nil
}
