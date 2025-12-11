package logparser

import (
	"sort"
	"time"
)

// AggregateMode defines how values are reduced within a bucket.
type AggregateMode int

const (
	// ModeCount counts the number of occurrences per metric in each bucket.
	ModeCount AggregateMode = iota
	// ModeSum sums the metric Value per bucket.
	ModeSum
	// ModeFirst picks the first metric value in the bucket (by earliest time).
	ModeFirst
	// ModeAvg averages the metric values in the bucket.
	ModeAvg
	// ModeDelta computes per-series increments (current - previous) and sums them per bucket.
	// For each metric series (by Name and optionally SourceType), points are time-sorted and
	// the first point has no increment (treated as 0). Subsequent points contribute their delta.
	ModeDelta
)

// BucketAggregator aggregates metrics into fixed time-step buckets.
// Grouping keys default to (Name, CF, SourceType). You can disable CF/SourceType grouping.
type BucketAggregator struct {
	Step           time.Duration
	Mode           AggregateMode
	GroupBySource  bool
}

func NewBucketAggregator(step time.Duration, mode AggregateMode) *BucketAggregator {
	return &BucketAggregator{
		Step:          step,
		Mode:          mode,
		GroupBySource: true,
	}
}

// Aggregate reduces the provided metrics into buckets and returns aggregated metrics.
// - Time is set to the bucket start (second precision).
// - Name uses a suffix when needed:
//   - ModeCount: "<Name>_Count"
//   - ModeSum:   "<Name>_Sum"
//   - ModeFirst: "<Name>_First"
//   - ModeAvg:   "<Name>_Avg"
// - CF/SourceType grouping depends on the aggregator flags.
func (a *BucketAggregator) Aggregate(metrics []Metric) []Metric {
	// Special handling for delta aggregation: we must respect temporal order
	// within each metric series to compute increments.
	if a.Mode == ModeDelta {
		// Group by series key (Name + optional SourceType)
		type series struct {
			st   LogType
			name string
			pts  []Metric
		}
		seriesMap := make(map[string]*series, len(metrics))
		for _, in := range metrics {
			if in.StartTime.IsZero() {
				continue
			}
			name := in.Name
			source := LogTypeOther
			if a.GroupBySource {
				source = in.SourceType
			}
			key := name + "|" + string(source)
			s := seriesMap[key]
			if s == nil {
				s = &series{st: source, name: name, pts: make([]Metric, 0, 32)}
				seriesMap[key] = s
			}
			s.pts = append(s.pts, in)
		}
		// Accumulate deltas into buckets
		type acc struct {
			sum float64
			st  LogType
			bkt time.Time
			nm  string
		}
		buckets := make(map[string]*acc, len(metrics))
		for _, s := range seriesMap {
			// sort by time
			sort.Slice(s.pts, func(i, j int) bool { return s.pts[i].StartTime.Before(s.pts[j].StartTime) })
			prevSet := false
			var prev float64
			for _, p := range s.pts {
				var delta float64
				if prevSet {
					delta = p.Value - prev
				} else {
					delta = 0
					prevSet = true
				}
				prev = p.Value
				bkt := alignToBucketStart(p.StartTime, a.Step)
				key := bkt.Format("2006/01/02-15:04:05") + "|" + s.name + "|" + string(s.st)
				ac := buckets[key]
				if ac == nil {
					ac = &acc{sum: 0, st: s.st, bkt: bkt, nm: s.name}
					buckets[key] = ac
				}
				ac.sum += delta
			}
		}
		out := make([]Metric, 0, len(buckets))
		for _, ac := range buckets {
			out = append(out, Metric{
				SourceType: ac.st,
				StartTime:  ac.bkt,
				Name:       ac.nm + "_Delta",
				Value:      ac.sum,
			})
		}
		return out
	}

	type acc struct {
		sum   float64
		count float64
		name  string
		st    LogType
		bkt   time.Time
		// track earliest value for ModeFirst
		firstVal  float64
		firstTime time.Time
		firstSet  bool
	}
	// bucket key: ts|name|source
	m := make(map[string]*acc, len(metrics))

	for _, in := range metrics {
		if in.StartTime.IsZero() {
			continue
		}
		bkt := alignToBucketStart(in.StartTime, a.Step)
		name := in.Name
		source := LogTypeOther
		if a.GroupBySource {
			source = in.SourceType
		}
		key := bkt.Format("2006/01/02-15:04:05") + "|" + name + "|" + string(source)
		ac := m[key]
		if ac == nil {
			ac = &acc{name: name, st: source, bkt: bkt}
			m[key] = ac
		}
		ac.count += 1
		ac.sum += in.Value
		// track earliest
		if !ac.firstSet || in.StartTime.Before(ac.firstTime) {
			ac.firstSet = true
			ac.firstTime = in.StartTime
			ac.firstVal = in.Value
		}
	}

	out := make([]Metric, 0, len(m))
	for _, ac := range m {
		var val float64
		var outName string
		switch a.Mode {
		case ModeCount:
			val = ac.count
			outName = ac.name + "_Count"
		case ModeSum:
			val = ac.sum
			outName = ac.name + "_Sum"
		case ModeFirst:
			// if no values somehow, default zero (but firstSet should be true if count>0)
			val = ac.firstVal
			outName = ac.name + "_First"
		case ModeAvg:
			if ac.count > 0 {
				val = ac.sum / ac.count
			} else {
				val = 0
			}
			outName = ac.name + "_Avg"
		default:
			val = ac.sum
			outName = ac.name + "_Sum"
		}
		out = append(out, Metric{
			SourceType: ac.st,
			StartTime:  ac.bkt,
			Name:       outName,
			Value:      val,
		})
	}
	return out
}

func alignToBucketStart(t time.Time, step time.Duration) time.Time {
	if step <= 0 {
		return t.Truncate(time.Second)
	}
	// Truncate aligns to floor of the duration from the zero time.
	aligned := t.Truncate(step)
	// Normalize to second precision for output consistency
	return aligned.Truncate(time.Second)
}

// (no string parsing needed; metrics carry time.Time)


