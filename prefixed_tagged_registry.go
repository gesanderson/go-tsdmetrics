package tsdmetrics

import "fmt"

// Provide a single set of tags and a prefix for all metrics in registry
type PrefixedTaggedRegistry struct {
	underlying  TaggedRegistry
	prefix      string
	defaultTags Tags
}

func NewPrefixedTaggedRegistry(prefix string, tags Tags) TaggedRegistry {
	return &PrefixedTaggedRegistry{
		underlying:  NewTaggedRegistry(),
		prefix:      prefix,
		defaultTags: tags,
	}
}

// Call the given function for each registered metric.
func (r *PrefixedTaggedRegistry) Each(fn func(string, TaggedMetric)) {
	wrapperFn := func(n string, m TaggedMetric) (string, TaggedMetric) {
		var realName string
		if r.prefix != "" {
			realName = fmt.Sprintf("%s.%s", r.prefix, n)
		} else {
			realName = n
		}

		newMetric := m.AddTags(r.defaultTags)

		return realName, newMetric
	}

	r.underlying.WrappedEach(wrapperFn, fn)
}

func (r *PrefixedTaggedRegistry) WrappedEach(wrapperFn func(string, TaggedMetric) (string, TaggedMetric), fn func(string, TaggedMetric)) {
	r.underlying.WrappedEach(wrapperFn, fn)
}

// Get the metric by the given name or nil if none is registered.
func (r *PrefixedTaggedRegistry) Get(name string, tags Tags) interface{} {
	return r.underlying.Get(name, tags)
}

// Gets an existing metric or registers the given one.
// The interface can be the metric to register if not found in registry,
// or a function returning the metric for lazy instantiation.
func (r *PrefixedTaggedRegistry) GetOrRegister(name string, tags Tags, i interface{}) interface{} {
	return r.underlying.GetOrRegister(name, tags, i)
}

// Register the given metric under the given name. The name will be prefixed.
func (r *PrefixedTaggedRegistry) Register(name string, tags Tags, i interface{}) error {
	return r.underlying.Register(name, tags, i)
}

func (r *PrefixedTaggedRegistry) Add(name string, tags Tags, i interface{}) error {
	return r.underlying.Add(name, tags, i)
}

// Run all registered healthchecks.
func (r *PrefixedTaggedRegistry) RunHealthchecks() {
	r.underlying.RunHealthchecks()
}

// Unregister the metric with the given name. The name will be prefixed.
func (r *PrefixedTaggedRegistry) Unregister(name string, tags Tags) {
	r.underlying.Unregister(name, tags)
}

// Unregister the metric using the Metric object itself.
func (r *PrefixedTaggedRegistry) UnregisterMetric(i interface{}) {
	r.underlying.UnregisterMetric(i)
}

// Unregister all metrics.  (Mostly for testing.)
func (r *PrefixedTaggedRegistry) UnregisterAll() {
	r.underlying.UnregisterAll()
}
