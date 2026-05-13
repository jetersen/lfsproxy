package exporter

import (
	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type LFSProxyCollector struct {
	CacheHits metrics.Counter
	CacheMiss metrics.Counter
	S3Hits    metrics.Counter
	S3Miss    metrics.Counter
}

type nopCounter struct{}

func (nopCounter) With(...string) metrics.Counter { return nopCounter{} }
func (nopCounter) Add(float64)                    {}

func NewCollector(enabled bool) *LFSProxyCollector {
	if !enabled {
		nop := nopCounter{}
		return &LFSProxyCollector{
			CacheHits: nop,
			CacheMiss: nop,
			S3Hits:    nop,
			S3Miss:    nop,
		}
	}

	return &LFSProxyCollector{
		CacheHits: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: "lfsproxy",
			Name:      "cache_hit",
			Help:      "In-memory Cache Hits",
		}, []string{}),
		CacheMiss: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: "lfsproxy",
			Name:      "cache_miss",
			Help:      "In-memory Cache Misses",
		}, []string{}),
		S3Hits: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: "lfsproxy",
			Name:      "s3_hit",
			Help:      "S3 Cache Hits",
		}, []string{}),
		S3Miss: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: "lfsproxy",
			Name:      "s3_miss",
			Help:      "S3 Cache Misses",
		}, []string{}),
	}
}

func PrometheusHandler() gin.HandlerFunc {
	h := promhttp.Handler()

	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}
