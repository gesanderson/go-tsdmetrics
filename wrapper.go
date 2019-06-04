package tsdmetrics

import (
	"context"
	"errors"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
)

type MetricWrapper struct {
	Registry TaggedRegistry
	OpenTSDB TaggedOpenTSDB
	baseName string
	logger   logrus.FieldLogger
}

//NewMetricWrapper returns a wrapper facilitating the creation of common metric objects
func NewMetricWrapper(baseName string, baseTags string, addr string, flushInterval int, logger logrus.FieldLogger) (*MetricWrapper, error) {
	w := &MetricWrapper{baseName: baseName, logger: logger}

	tags, err := TagsFromString(baseTags)
	if err != nil {
		return nil, errors.New("Invalid root metric tags '" + baseTags + "' [" + err.Error() + "]")
	}
	logger.Infof("Root metric tags: %v", tags)

	// Metrics initialization
	w.Registry = NewSegmentedTaggedRegistry("", tags, nil)
	w.OpenTSDB = TaggedOpenTSDB{
		Addr:          addr,
		Registry:      w.Registry,
		FlushInterval: time.Duration(flushInterval) * time.Second,
		DurationUnit:  time.Millisecond,
		Format:        Json,
		Compress:      true,
		BulkSize:      10000,
		Logger:        logger}

	go w.OpenTSDB.Run(context.Background())

	return w, nil
}

func (w *MetricWrapper) metricName(name string) string {
	if w.baseName != "" {
		return w.baseName + "." + name
	}
	return name
}

// RegisterMemStats will send go runtime stats to opentsdb
func (w *MetricWrapper) RegisterMemStats(tags Tags) {
	memRegistry := NewSegmentedTaggedRegistry("", tags, w.Registry)
	RegisterTaggedRuntimeMemStats(memRegistry)
}

// Gauge creates a metric gauge object
func (w *MetricWrapper) Gauge(name string, tags Tags) metrics.Gauge {
	return w.Registry.GetOrRegister(w.metricName(name), tags, metrics.NewGauge()).(metrics.Gauge)
}

// GaugeFloat64 creates a float64 metric gauge object
func (w *MetricWrapper) GaugeFloat64(name string, tags Tags) metrics.GaugeFloat64 {
	return w.Registry.GetOrRegister(w.metricName(name), tags, metrics.NewGaugeFloat64()).(metrics.GaugeFloat64)
}

// Counter creates a metric counter object
func (w *MetricWrapper) Counter(name string, tags Tags) metrics.Counter {
	return w.Registry.GetOrRegister(w.metricName(name), tags, metrics.NewCounter()).(metrics.Counter)
}

// Histogram creates a metric histogram object
func (w *MetricWrapper) Histogram(name string, sample metrics.Sample, tags Tags) metrics.Histogram {
	return w.Registry.GetOrRegister(w.metricName(name), tags, metrics.NewHistogram(sample)).(metrics.Histogram)
}

// Meter creates a metric meter object
func (w *MetricWrapper) Meter(name string, tags Tags) metrics.Meter {
	return w.Registry.GetOrRegister(w.metricName(name), tags, metrics.NewMeter()).(metrics.Meter)
}

// UnregisterMetric unregister metrics using the Metric object itself.
func (w *MetricWrapper) UnregisterMetric(i interface{}) {
	w.Registry.UnregisterMetric(i)
}
