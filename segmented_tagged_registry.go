package tsdmetrics

import "fmt"

type SegmentedTaggedRegistry struct {
	parent      TaggedRegistry
	prefix      string
	defaultTags Tags
}

func NewRootSegmentedTaggedRegistry(tags Tags) TaggedRegistry {
	return &SegmentedTaggedRegistry{
		parent:      NewTaggedRegistry(),
		defaultTags: tags,
	}
}

func NewSegmentedTaggedRegistry(prefix string, tags Tags, parentRegistry TaggedRegistry) TaggedRegistry {
	if parentRegistry == nil {
		parentRegistry = NewTaggedRegistry()
	}

	return &SegmentedTaggedRegistry{
		parent:      parentRegistry,
		prefix:      prefix,
		defaultTags: tags,
	}
}

func (r *SegmentedTaggedRegistry) GetRootRegistry() TaggedRegistry {
	switch parent := r.parent.(type) {
	case *SegmentedTaggedRegistry:
		return parent.GetRootRegistry()
	default:
		return parent
	}

	return nil
}

func (r *SegmentedTaggedRegistry) GetName(name string) string {
	p := r.GetPrefix()
	if p != "" {
		return fmt.Sprintf("%s.%s", p, name)
	}

	return name
}

func (r *SegmentedTaggedRegistry) GetPrefix() string {
	var n string

	switch parent := r.parent.(type) {
	case *SegmentedTaggedRegistry:
		n = parent.GetPrefix()
		if n != "" {
			n = fmt.Sprintf("%s.%s", n, r.prefix)
		} else {
			n = r.prefix
		}
	default:
		n = r.prefix
	}

	return n
}

func (r *SegmentedTaggedRegistry) GetTags(t Tags) Tags {
	tags := t
	if s, ok := r.parent.(*SegmentedTaggedRegistry); ok {
		tags = t.AddTags(s.GetTags(t))
	}

	return tags.AddTags(r.defaultTags)
}

// Call the given function for each registered metric.
func (r *SegmentedTaggedRegistry) Each(fn func(string, TaggedMetric)) {
	wrapperFn := func(n string, m TaggedMetric) (string, TaggedMetric) {
		return r.GetName(n), m.AddTags(r.GetTags(Tags{}))
	}

	r.parent.WrappedEach(wrapperFn, fn)
}

func (r *SegmentedTaggedRegistry) WrappedEach(wrapperFn func(string, TaggedMetric) (string, TaggedMetric), fn func(string, TaggedMetric)) {
	r.parent.WrappedEach(wrapperFn, fn)
}

// Get the metric by the given name or nil if none is registered.
func (r *SegmentedTaggedRegistry) Get(name string, tags Tags) interface{} {
	return r.GetRootRegistry().Get(r.GetName(name), r.GetTags(tags))
}

// Gets an existing metric or registers the given one.
// The interface can be the metric to register if not found in registry,
// or a function returning the metric for lazy instantiation.
func (r *SegmentedTaggedRegistry) GetOrRegister(name string, tags Tags, i interface{}) interface{} {
	return r.GetRootRegistry().GetOrRegister(r.GetName(name), r.GetTags(tags), i)
}

// Register the given metric under the given name. The name will be prefixed.
func (r *SegmentedTaggedRegistry) Register(name string, tags Tags, i interface{}) error {
	return r.GetRootRegistry().Register(r.GetName(name), r.GetTags(tags), i)
}

func (r *SegmentedTaggedRegistry) Add(name string, tags Tags, i interface{}) error {
	return r.GetRootRegistry().Add(r.GetName(name), r.GetTags(tags), i)
}

// Run all registered healthchecks.
func (r *SegmentedTaggedRegistry) RunHealthchecks() {
	r.parent.RunHealthchecks()
}

// Unregister the metric with the given name. The name will be prefixed.
func (r *SegmentedTaggedRegistry) Unregister(name string, tags Tags) {
	r.parent.Unregister(name, tags)
}

// Unregister the metric using the Metric object itself.
func (r *SegmentedTaggedRegistry) UnregisterMetric(i interface{}) {
	r.parent.UnregisterMetric(i)
}

// Unregister all metrics.  (Mostly for testing.)
func (r *SegmentedTaggedRegistry) UnregisterAll() {
	r.parent.UnregisterAll()
}
