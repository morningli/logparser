package logparser

import (
	"errors"
	"fmt"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"bytes"
	"strconv"
	"time"
)

// simple glob matcher for names (case-insensitive)
func matchNameGlob(pattern, name string) bool {
	p := strings.ToLower(pattern)
	n := strings.ToLower(name)
	ok, _ := filepath.Match(p, n)
	return ok
}

// ChartGroup defines one chart: output file, optional title and the metric names to include.
type ChartGroup struct {
	Out    string
	Title  string
	Names  []string
	// Optional: abstract file type key (resolved via config FileTypes)
	Type   string
	Agg    string
	// Optional computed series within this group; evaluated after aggregation on the group's metric set.
	Exprs  []ExprSpec `json:"exprs"`
}

// ExprSpec defines a computed metric series Name = Formula
// Formula supports + - * / and parentheses over metric names present in the aggregated set for this group.
type ExprSpec struct {
	Name    string `json:"name"`
	Formula string `json:"formula"`
}

// ChartOrchestrator renders multiple charts from a single metric stream based on groups.
type ChartOrchestrator struct {
	Groups []ChartGroup
}

// RenderAll renders each group to its Out path using Dialog, filtering metrics by Names (exact match).
func (o *ChartOrchestrator) RenderAll(metrics []Metric) error {
	for _, g := range o.Groups {
		if g.Out == "" {
			return errors.New("chart group missing Out path")
		}
		nameSet := make(map[string]struct{}, len(g.Names))
		var patterns []string
		for _, n := range g.Names {
			n = strings.TrimSpace(n)
			if n != "" {
				if strings.ContainsAny(n, "*?[]") {
					patterns = append(patterns, n)
				} else {
					nameSet[n] = struct{}{}
				}
			}
		}
		selected := make([]Metric, 0, len(metrics))
		for _, m := range metrics {
			if _, ok := nameSet[m.Name]; ok {
				selected = append(selected, m)
				continue
			}
			matched := false
			for _, pat := range patterns {
				if matchNameGlob(pat, m.Name) {
					matched = true
					break
				}
			}
			if matched {
				selected = append(selected, m)
			}
		}
		// Inject computed expressions if any (after filtering base series)
		if len(g.Exprs) > 0 {
			selected = append(selected, computeExpressions(selected, g.Exprs)...)
		}
		dlg := NewDialog()
		if g.Title != "" {
			dlg.Title = g.Title
		} else {
			dlg.Title = fmt.Sprintf("Metrics: %s", strings.Join(g.Names, ", "))
		}
		// Ensure folder exists (best-effort)
		if dir := filepath.Dir(g.Out); dir != "" && dir != "." {
			_ = ensureDir(dir)
		}
		if err := dlg.Render(selected, g.Out); err != nil {
			return err
		}
	}
	return nil
}

// RenderAllWithAgg renders each group with its own aggregation mode (if provided), otherwise defaultMode.
// If bucketStep <= 0, no aggregation is applied.
func (o *ChartOrchestrator) RenderAllWithAgg(metrics []Metric, bucketStep time.Duration, defaultMode AggregateMode, groupBySource bool) error {
	for _, g := range o.Groups {
		if g.Out == "" {
			return errors.New("chart group missing Out path")
		}
		exprMode := strings.ToLower(strings.TrimSpace(g.Agg)) == "expr" || strings.ToLower(strings.TrimSpace(g.Agg)) == "expression"
		// First aggregate (so names carry suffix _Sum/_Avg/...),
		// then filter by the configured names.
		selected := metrics
		if bucketStep > 0 {
			mode := PickAggMode(strings.TrimSpace(g.Agg), defaultMode)
			agg := NewBucketAggregator(bucketStep, mode)
			agg.GroupBySource = groupBySource
			selected = agg.Aggregate(selected)
		}
		// For expr mode: if expressions specified, replace selection with computed series
		if exprMode && len(g.Exprs) > 0 {
			comp := computeExpressions(selected, g.Exprs)
			if len(comp) > 0 {
				selected = comp
			}
		} else if len(g.Exprs) > 0 {
			// Non-expr mode: append computed series in addition to base selection
			selected = append(selected, computeExpressions(selected, g.Exprs)...)
		}
		nameSet := make(map[string]struct{}, len(g.Names))
		var patterns []string
		for _, n := range g.Names {
			n = strings.TrimSpace(n)
			if n != "" {
				if strings.ContainsAny(n, "*?[]") {
					patterns = append(patterns, n)
				} else {
					nameSet[n] = struct{}{}
				}
			}
		}
		filtered := make([]Metric, 0, len(selected))
		for _, m := range selected {
			if _, ok := nameSet[m.Name]; ok {
				filtered = append(filtered, m)
				continue
			}
			matched := false
			for _, pat := range patterns {
				if matchNameGlob(pat, m.Name) {
					matched = true
					break
				}
			}
			if matched {
				filtered = append(filtered, m)
			}
		}
		if bucketStep > 0 {
			// already aggregated above
		}
		dlg := NewDialog()
		if g.Title != "" {
			dlg.Title = g.Title
		} else {
			dlg.Title = fmt.Sprintf("Metrics: %s", strings.Join(g.Names, ", "))
		}
		if dir := filepath.Dir(g.Out); dir != "" && dir != "." {
			_ = ensureDir(dir)
		}
		if err := dlg.Render(filtered, g.Out); err != nil {
			return err
		}
	}
	return nil
}

// ParseChartsSpec parses a semicolon-separated spec of groups:
//   "out1.svg:Title A:Name1,Name2; out2.svg:Title B:Name3,Name4"
// Title can be omitted: "out.svg:Name1,Name2"
func ParseChartsSpec(spec string) ([]ChartGroup, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	segs := strings.Split(spec, ";")
	out := make([]ChartGroup, 0, len(segs))
	for _, seg := range segs {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		parts := strings.SplitN(seg, ":", 3)
		switch len(parts) {
		case 3:
			names := splitCSV(parts[2])
			out = append(out, ChartGroup{
				Out:   strings.TrimSpace(parts[0]),
				Title: strings.TrimSpace(parts[1]),
				Names: names,
			})
		case 2:
			// treat second part as names list
			names := splitCSV(parts[1])
			out = append(out, ChartGroup{
				Out:   strings.TrimSpace(parts[0]),
				Title: "",
				Names: names,
			})
		default:
			return nil, fmt.Errorf("bad charts spec segment: %q", seg)
		}
	}
	return out, nil
}

func splitCSV(s string) []string {
	raw := strings.Split(s, ",")
	res := make([]string, 0, len(raw))
	for _, r := range raw {
		t := strings.TrimSpace(r)
		if t != "" {
			res = append(res, t)
		}
	}
	return res
}

// ensureDir is a minimal directory ensure util to avoid adding new deps.
// It tries to create the directory path if missing.
func ensureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// RenderAllSingle stacks all groups vertically into a single SVG file.
// It renders each group using Dialog into a temporary SVG, extracts its inner content and dimensions,
// then composes a parent SVG with each panel stacked vertically.
func (o *ChartOrchestrator) RenderAllSingle(metrics []Metric, out string) error {
	if len(o.Groups) == 0 {
		return errors.New("no chart groups")
	}
	type panel struct {
		inner  string
		width  int
		height int
	}
	var panels []panel
	maxW := 0
	totalH := 0
	for _, g := range o.Groups {
		nameSet := make(map[string]struct{}, len(g.Names))
		var patterns []string
		for _, n := range g.Names {
			n = strings.TrimSpace(n)
			if n != "" {
				if strings.ContainsAny(n, "*?[]") {
					patterns = append(patterns, n)
				} else {
					nameSet[n] = struct{}{}
				}
			}
		}
		selected := make([]Metric, 0, len(metrics))
		for _, m := range metrics {
			if _, ok := nameSet[m.Name]; ok {
				selected = append(selected, m)
				continue
			}
			matched := false
			for _, pat := range patterns {
				if matchNameGlob(pat, m.Name) {
					matched = true
					break
				}
			}
			if matched {
				selected = append(selected, m)
			}
		}
		// skip empty groups to avoid aborting stacked render
		if len(selected) == 0 {
			continue
		}
		dlg := NewDialog()
		if g.Title != "" {
			dlg.Title = g.Title
		} else {
			dlg.Title = fmt.Sprintf("Metrics: %s", strings.Join(g.Names, ", "))
		}
		tmp, err := os.CreateTemp("", "chart_*.svg")
		if err != nil {
			return err
		}
		tmpPath := tmp.Name()
		_ = tmp.Close()
		// render to tmp
		if err := dlg.Render(selected, tmpPath); err != nil {
			return err
		}
		// read and extract
		data, err := os.ReadFile(tmpPath)
		_ = os.Remove(tmpPath)
		if err != nil {
			return err
		}
		inner, w, h := extractSVGInner(data)
		if inner == "" || w <= 0 || h <= 0 {
			// fallback default panel dimensions if not found
			if inner == "" {
				inner = string(data)
			}
			if w <= 0 {
				w = 1200
			}
			if h <= 0 {
				h = 600
			}
		}
		panels = append(panels, panel{inner: inner, width: w, height: h})
		if w > maxW {
			maxW = w
		}
		totalH += h
	}
	if len(panels) == 0 {
		return errors.New("no panels to render")
	}
	if dir := filepath.Dir(out); dir != "" && dir != "." {
		_ = ensureDir(dir)
	}
	var buf bytes.Buffer
	// Layout panels in two columns
	cols := 2
	// compute row heights
	rowCount := (len(panels) + cols - 1) / cols
	rowHeights := make([]int, rowCount)
	totalH = 0
	for r := 0; r < rowCount; r++ {
		left := panels[r*cols]
		rh := left.height
		if r*cols+1 < len(panels) {
			right := panels[r*cols+1]
			if right.height > rh {
				rh = right.height
			}
		}
		rowHeights[r] = rh
		totalH += rh
	}
	// ensure maxW sane before computing total width
	if maxW <= 0 {
		for _, p := range panels {
			if p.width > maxW {
				maxW = p.width
			}
		}
		if maxW <= 0 {
			maxW = 1200
		}
	}
	totalW := maxW * 2 // two-column layout
	buf.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, totalW, totalH, totalW, totalH))
	y := 0
	for r := 0; r < rowCount; r++ {
		// left
		idx := r * cols
		buf.WriteString(fmt.Sprintf(`<g transform="translate(%d,%d)">`, 0, y))
		buf.WriteString(panels[idx].inner)
		buf.WriteString(`</g>`)
		// right if present
		if idx+1 < len(panels) {
			buf.WriteString(fmt.Sprintf(`<g transform="translate(%d,%d)">`, maxW, y))
			buf.WriteString(panels[idx+1].inner)
			buf.WriteString(`</g>`)
		}
		y += rowHeights[r]
	}
	buf.WriteString(`</svg>`)
	return os.WriteFile(out, buf.Bytes(), 0644)
}

// RenderAllSingleWithAgg stacks panels after optional per-group aggregation.
func (o *ChartOrchestrator) RenderAllSingleWithAgg(metrics []Metric, out string, bucketStep time.Duration, defaultMode AggregateMode, groupBySource bool) error {
	if len(o.Groups) == 0 {
		return errors.New("no chart groups")
	}
	type panel struct {
		inner  string
		width  int
		height int
	}
	var panels []panel
	maxW := 0
	totalH := 0
	for _, g := range o.Groups {
		// Aggregate first so names have suffixes, then filter.
		selected := metrics
		if bucketStep > 0 {
			mode := PickAggMode(strings.TrimSpace(g.Agg), defaultMode)
			agg := NewBucketAggregator(bucketStep, mode)
			agg.GroupBySource = groupBySource
			selected = agg.Aggregate(selected)
		}
		// Expression mode: compute expressions and replace selection
		exprMode := strings.ToLower(strings.TrimSpace(g.Agg)) == "expr" || strings.ToLower(strings.TrimSpace(g.Agg)) == "expression"
		if exprMode && len(g.Exprs) > 0 {
			comp := computeExpressions(selected, g.Exprs)
			if len(comp) > 0 {
				selected = comp
			}
		} else if len(g.Exprs) > 0 {
			// Non-expr: append computed series alongside base selection
			selected = append(selected, computeExpressions(selected, g.Exprs)...)
		}
		nameSet := make(map[string]struct{}, len(g.Names))
		var patterns []string
		for _, n := range g.Names {
			n = strings.TrimSpace(n)
			if n != "" {
				if strings.ContainsAny(n, "*?[]") {
					patterns = append(patterns, n)
				} else {
					nameSet[n] = struct{}{}
				}
			}
		}
		filtered := make([]Metric, 0, len(selected))
		for _, m := range selected {
			if _, ok := nameSet[m.Name]; ok {
				filtered = append(filtered, m)
				continue
			}
			matched := false
			for _, pat := range patterns {
				if matchNameGlob(pat, m.Name) {
					matched = true
					break
				}
			}
			if matched {
				filtered = append(filtered, m)
			}
		}
		dlg := NewDialog()
		if g.Title != "" {
			dlg.Title = g.Title
		} else {
			dlg.Title = fmt.Sprintf("Metrics: %s", strings.Join(g.Names, ", "))
		}
		// skip empty groups to avoid aborting stacked render
		if len(filtered) == 0 {
			continue
		}
		tmp, err := os.CreateTemp("", "chart_*.svg")
		if err != nil {
			return err
		}
		tmpPath := tmp.Name()
		_ = tmp.Close()
		if err := dlg.Render(filtered, tmpPath); err != nil {
			return err
		}
		data, err := os.ReadFile(tmpPath)
		_ = os.Remove(tmpPath)
		if err != nil {
			return err
		}
		inner, w, h := extractSVGInner(data)
		if inner == "" || w <= 0 || h <= 0 {
			if inner == "" {
				inner = string(data)
			}
			if w <= 0 {
				w = 1200
			}
			if h <= 0 {
				h = 600
			}
		}
		panels = append(panels, panel{inner: inner, width: w, height: h})
		if w > maxW {
			maxW = w
		}
		totalH += h
	}
	if len(panels) == 0 {
		return errors.New("no panels to render")
	}
	if dir := filepath.Dir(out); dir != "" && dir != "." {
		_ = ensureDir(dir)
	}
	var buf bytes.Buffer
	// Layout panels in two columns
	cols := 2
	rowCount := (len(panels) + cols - 1) / cols
	rowHeights := make([]int, rowCount)
	totalH = 0
	for r := 0; r < rowCount; r++ {
		left := panels[r*cols]
		rh := left.height
		if r*cols+1 < len(panels) {
			right := panels[r*cols+1]
			if right.height > rh {
				rh = right.height
			}
		}
		rowHeights[r] = rh
		totalH += rh
	}
	// ensure maxW sane before computing total width
	if maxW <= 0 {
		for _, p := range panels {
			if p.width > maxW {
				maxW = p.width
			}
		}
		if maxW <= 0 {
			maxW = 1200
		}
	}
	totalW := maxW * 2 // two-column layout
	buf.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, totalW, totalH, totalW, totalH))
	y := 0
	for r := 0; r < rowCount; r++ {
		idx := r * cols
		buf.WriteString(fmt.Sprintf(`<g transform="translate(%d,%d)">`, 0, y))
		buf.WriteString(panels[idx].inner)
		buf.WriteString(`</g>`)
		if idx+1 < len(panels) {
			buf.WriteString(fmt.Sprintf(`<g transform="translate(%d,%d)">`, maxW, y))
			buf.WriteString(panels[idx+1].inner)
			buf.WriteString(`</g>`)
		}
		y += rowHeights[r]
	}
	buf.WriteString(`</svg>`)
	return os.WriteFile(out, buf.Bytes(), 0644)
}

// reWH matches width/height in either single or double quotes, e.g. width="1200" or height='600'
var reWH = regexp.MustCompile(`\b(width|height)\s*=\s*['"]([0-9]+)['"]`)

// extractSVGInner tries to parse width/height and inner content of an SVG
func extractSVGInner(data []byte) (inner string, w int, h int) {
	s := data
	// find <svg ...>
	svgIdx := bytes.Index(s, []byte("<svg"))
	if svgIdx < 0 {
		return "", 0, 0
	}
	gt := bytes.IndexByte(s[svgIdx:], '>')
	if gt < 0 {
		return "", 0, 0
	}
	header := s[svgIdx : svgIdx+gt+1]
	// parse width/height
	matches := reWH.FindAllSubmatch(header, -1)
	for _, m := range matches {
		if len(m) == 3 {
			val, _ := strconv.Atoi(string(m[2]))
			if string(m[1]) == "width" {
				w = val
			} else if string(m[1]) == "height" {
				h = val
			}
		}
	}
	// find closing </svg>
	endIdx := bytes.LastIndex(s, []byte("</svg>"))
	if endIdx < 0 {
		return "", w, h
	}
	innerBytes := s[svgIdx+gt+1 : endIdx]
	return string(innerBytes), w, h
}

// ParseChartsConfig loads groups from a JSON file.
// Accepted formats:
// 1) Raw array:
//    [ {"out":"a.svg","title":"A","names":["N1","N2"]}, ... ]
// 2) Object with groups field:
//    { "groups": [ {"out":"...","names":[...]} ] }
func ParseChartsConfig(path string) ([]ChartGroup, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// try raw array
	var rawArr []ChartGroup
	if err := json.Unmarshal(data, &rawArr); err == nil && len(rawArr) > 0 {
		return rawArr, nil
	}
	// try object
	var obj struct {
		Groups []ChartGroup `json:"groups"`
	}
	if err := json.Unmarshal(data, &obj); err == nil && len(obj.Groups) > 0 {
		return obj.Groups, nil
	}
	// empty array is also valid
	if err := json.Unmarshal(data, &rawArr); err == nil && len(rawArr) == 0 {
		return rawArr, nil
	}
	return nil, fmt.Errorf("unrecognized charts config format: %s", path)
}

// ChartsConfigFull supports top-level file type mapping and optional bucket.
type ChartsConfigFull struct {
	Groups      []ChartGroup      `json:"groups"`
	FileTypes   map[string]string `json:"fileTypes"`
	// Optional global bucket step like "10m"; CLI may override if not set
	Bucket      string            `json:"bucket"`
}

// ParseChartsConfigFull returns groups, file type mapping, and optional bucket (string).
func ParseChartsConfigFull(path string) ([]ChartGroup, map[string]string, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, "", err
	}
	// Try full object first
	var full ChartsConfigFull
	if err := json.Unmarshal(data, &full); err == nil && (len(full.Groups) > 0 || len(full.FileTypes) > 0) {
		return full.Groups, full.FileTypes, full.Bucket, nil
	}
	// Fallback to raw array or {groups:[]}
	if groups, err := ParseChartsConfig(path); err == nil {
		return groups, map[string]string{}, "", nil
	} else {
		return nil, nil, "", err
	}
}

func PickAggMode(s string, def AggregateMode) AggregateMode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "count":
		return ModeCount
	case "sum":
		return ModeSum
	case "expr", "expression":
		// Expressions are computed at orchestrator level; use SUM as base aggregation.
		return ModeSum
	case "first":
		return ModeFirst
	case "avg", "average":
		return ModeAvg
	case "delta", "diff", "incr", "increment", "incremental":
		return ModeDelta
	default:
		return def
	}
}

// computeExpressions evaluates group-level expressions over the provided metrics (already aggregated if bucketStep>0).
func computeExpressions(selected []Metric, exprs []ExprSpec) []Metric {
	out := make([]Metric, 0, len(exprs)*4)
	for _, es := range exprs {
		name := strings.TrimSpace(es.Name)
		formula := strings.TrimSpace(es.Formula)
		if name == "" || formula == "" {
			continue
		}
		series, err := ComputeExpression(selected, formula, name)
		if err != nil {
			// Skip bad expression silently to avoid aborting the whole render; alternatively log to stderr if available
			continue
		}
		out = append(out, series...)
	}
	return out
}


