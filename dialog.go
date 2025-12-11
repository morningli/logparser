package logparser

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Dialog renders simple time-series charts from []Metric as SVG files.
// - Multiple metric names -> multiple series (one polyline per Name)
// - X axis: time
// - Y axis: value
type Dialog struct {
	Width      int
	Height     int
	Padding    int
	Background string
	Grid       bool
	Title      string
	TimeFormat string // for tick labels
}

func NewDialog() *Dialog {
	return &Dialog{
		Width:      1200,
		Height:     600,
		Padding:    60,
		Background: "#ffffff",
		Grid:       true,
		Title:      "",
		TimeFormat: "01-02 15:04",
	}
}

// Render writes an SVG chart to outPath.
// Series are grouped by metric Name; each Name is drawn as one colored line.
func (d *Dialog) Render(metrics []Metric, outPath string) error {
	if len(metrics) == 0 {
		return fmt.Errorf("no metrics to render")
	}

	// Group by Name
	nameToPoints := map[string][]Metric{}
	for _, m := range metrics {
		if m.StartTime.IsZero() {
			continue
		}
		nameToPoints[m.Name] = append(nameToPoints[m.Name], m)
	}
	if len(nameToPoints) == 0 {
		return fmt.Errorf("no valid metrics (missing StartTime)")
	}

	// Sort each series by time; also collect global min/max
	var minT, maxT time.Time
	minSet := false
	minY := 0.0
	maxY := 0.0
	allVals := make([]float64, 0, len(metrics))
	for name, pts := range nameToPoints {
		sort.Slice(pts, func(i, j int) bool { return pts[i].StartTime.Before(pts[j].StartTime) })
		nameToPoints[name] = pts
		for _, p := range pts {
			if !minSet {
				minT, maxT = p.StartTime, p.StartTime
				minY, maxY = p.Value, p.Value
				minSet = true
			} else {
				if p.StartTime.Before(minT) {
					minT = p.StartTime
				}
				if p.StartTime.After(maxT) {
					maxT = p.StartTime
				}
				if p.Value < minY {
					minY = p.Value
				}
				if p.Value > maxY {
					maxY = p.Value
				}
			}
			allVals = append(allVals, p.Value)
		}
	}
	if !minSet {
		return fmt.Errorf("no points after filtering")
	}
	if !maxT.After(minT) {
		// expand a tiny window to avoid divide-by-zero
		maxT = minT.Add(time.Minute)
	}

	// Use a robust Y-maximum: 95th percentile of all values, with headroom and nice rounding.
	if len(allVals) >= 3 {
		sort.Float64s(allVals)
		p95Idx := int(float64(len(allVals)-1) * 0.95)
		if p95Idx < 0 {
			p95Idx = 0
		}
		p95 := allVals[p95Idx]
		if p95 > minY {
			maxY = p95
		}
		// add 5% headroom
		head := (maxY - minY) * 0.05
		if head <= 0 {
			head = 1
		}
		maxY = maxY + head
		// nice rounding up
		maxY = niceUpper(maxY)
	}
	if maxY <= minY {
		// expand a tiny vertical range
		maxY = minY + 1
	}

	// Layout
	w := d.Width
	h := d.Height
	pad := d.Padding
	plotW := float64(w - 2*pad)
	plotH := float64(h - 2*pad)

	// Scales
	tRange := maxT.Sub(minT).Seconds()
	yRange := maxY - minY
	timeToX := func(t time.Time) float64 {
		return float64(pad) + (float64(t.Sub(minT).Seconds())/tRange)*plotW
	}
	valToY := func(v float64) float64 {
		// invert y: larger values higher
		return float64(h-pad) - ((v-minY)/yRange)*plotH
	}

	// Build SVG
	var b strings.Builder
	fmt.Fprintf(&b, "<svg xmlns='http://www.w3.org/2000/svg' width='%d' height='%d' viewBox='0 0 %d %d'>\n", w, h, w, h)
	fmt.Fprintf(&b, "<rect x='0' y='0' width='%d' height='%d' fill='%s'/>\n", w, h, d.Background)

	// Title
	if strings.TrimSpace(d.Title) != "" {
		fmt.Fprintf(&b, "<text x='%d' y='%d' text-anchor='middle' font-family='sans-serif' font-size='18' fill='#333'>%s</text>\n",
			w/2, pad/2, escapeXML(d.Title))
	}

	// Ticks/grid
	if d.Grid {
		gridColor := "#eee"
		// X ticks: 6
		for i := 0; i <= 6; i++ {
			ratio := float64(i) / 6.0
			x := float64(pad) + ratio*plotW
			fmt.Fprintf(&b, "<line x1='%.1f' y1='%d' x2='%.1f' y2='%d' stroke='%s' stroke-width='1'/>\n", x, pad, x, h-pad, gridColor)

			// time label
			tickSec := int64(ratio * tRange)
			tt := minT.Add(time.Duration(tickSec) * time.Second)
			label := tt.Format(d.TimeFormat)
			fmt.Fprintf(&b, "<text x='%.1f' y='%d' text-anchor='middle' font-family='sans-serif' font-size='11' fill='#555'>%s</text>\n", x, h-(pad/2), escapeXML(label))
		}
		// Y ticks: 6
		for i := 0; i <= 6; i++ {
			ratio := float64(i) / 6.0
			y := float64(h-pad) - ratio*plotH
			fmt.Fprintf(&b, "<line x1='%d' y1='%.1f' x2='%d' y2='%.1f' stroke='%s' stroke-width='1'/>\n", pad, y, w-pad, y, gridColor)
			val := minY + ratio*yRange
			fmt.Fprintf(&b, "<text x='%d' y='%.1f' text-anchor='end' font-family='sans-serif' font-size='11' fill='#555'>%.4g</text>\n", pad-8, y+4, val)
		}
	}

	// Axes (draw AFTER grid to avoid being overdrawn by the last grid line)
	axisColor := "#222"
	fmt.Fprintf(&b, "<line x1='%d' y1='%d' x2='%d' y2='%d' stroke='%s' stroke-width='1'/>\n", pad, h-pad, w-pad, h-pad, axisColor) // X
	fmt.Fprintf(&b, "<line x1='%d' y1='%d' x2='%d' y2='%d' stroke='%s' stroke-width='1'/>\n", pad, pad, pad, h-pad, axisColor)     // Y

	// Series colors
	colors := []string{
		"#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
		"#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
		"#bcbd22", "#17becf",
	}

	// Draw series
	seriesNames := make([]string, 0, len(nameToPoints))
	for k := range nameToPoints {
		seriesNames = append(seriesNames, k)
	}
	sort.Strings(seriesNames)

	for i, name := range seriesNames {
		pts := nameToPoints[name]
		if len(pts) == 0 {
			continue
		}
		color := colors[i%len(colors)]
		// Build polyline points
		var psb strings.Builder
		for _, p := range pts {
			x := timeToX(p.StartTime)
			y := valToY(p.Value)
			fmt.Fprintf(&psb, "%.2f,%.2f ", x, y)
		}
		fmt.Fprintf(&b, "<polyline fill='none' stroke='%s' stroke-width='2' points='%s'/>\n", color, strings.TrimSpace(psb.String()))

		// Annotate top-3 maximum values for non-zero series
		type idxVal struct {
			idx int
			val float64
		}
		candidates := make([]idxVal, 0, len(pts))
		for idx, p := range pts {
			if p.Value > 0 {
				candidates = append(candidates, idxVal{idx: idx, val: p.Value})
			}
		}
		if len(candidates) > 0 {
			sort.Slice(candidates, func(i, j int) bool { return candidates[i].val > candidates[j].val })
			n := 3
			if len(candidates) < n {
				n = len(candidates)
			}
			for k := 0; k < n; k++ {
				p := pts[candidates[k].idx]
				x := timeToX(p.StartTime)
				y := valToY(p.Value)
				// point marker
				fmt.Fprintf(&b, "<circle cx='%.2f' cy='%.2f' r='3' fill='%s' stroke='#ffffff' stroke-width='1'/>\n", x, y, color)
				// value label slightly above
				fmt.Fprintf(&b, "<text x='%.2f' y='%.2f' text-anchor='middle' font-family='sans-serif' font-size='11' fill='%s'>%.4g</text>\n", x, y-6, color, p.Value)
			}
		}
	}

	// Legend (top-right)
	legendX := w - pad - 200
	if legendX < pad {
		legendX = pad
	}
	legendY := pad
	lineH := 18
	fmt.Fprintf(&b, "<rect x='%d' y='%d' width='200' height='%d' fill='#ffffff' stroke='#ddd'/>\n",
		legendX, legendY-14, 14+len(seriesNames)*lineH)
	for i, name := range seriesNames {
		color := colors[i%len(colors)]
		y := legendY + i*lineH
		fmt.Fprintf(&b, "<line x1='%d' y1='%d' x2='%d' y2='%d' stroke='%s' stroke-width='3'/>\n", legendX+10, y, legendX+40, y, color)
		fmt.Fprintf(&b, "<text x='%d' y='%d' font-family='sans-serif' font-size='12' fill='#333'>%s</text>\n", legendX+48, y+4, escapeXML(name))
	}

	fmt.Fprintln(&b, "</svg>")

	// Write file
	if err := os.WriteFile(outPath, []byte(b.String()), 0644); err != nil {
		return err
	}
	return nil
}

// niceUpper rounds up v to a "nice" number (1, 2, 5) * 10^k
func niceUpper(v float64) float64 {
	if v <= 0 {
		return 1
	}
	// determine exponent
	exp := 0
	pow10 := 1.0
	for v/pow10 >= 10 {
		pow10 *= 10
		exp++
	}
	for v/pow10 < 1 {
		pow10 /= 10
		exp--
	}
	n := v / pow10
	var nice float64
	switch {
	case n <= 1:
		nice = 1
	case n <= 2:
		nice = 2
	case n <= 5:
		nice = 5
	default:
		nice = 10
	}
	return nice * pow10
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}


