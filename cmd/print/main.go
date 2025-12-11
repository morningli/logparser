package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	lp "tools/logparser"
)

type itParser interface {
	Seek(time.Time) error
	Next() bool
	Value() (lp.LogItem, error)
	Close() error
}

type mode int

const (
	modeItems mode = iota
	modeMetrics
)

func parseTimeFlexible(s string) (time.Time, error) {
	formats := []string{
		"2006/01/02-15:04:05.000000",
		"2006/01/02-15:04:05",
		"2006/01/02-15:04",
		time.RFC3339,
	}
	var last error
	for _, f := range formats {
		if t, err := time.ParseInLocation(f, s, time.Local); err == nil {
			return t, nil
		} else {
			last = err
		}
	}
	return time.Time{}, last
}

// computeDerivedExpressions builds common derived series and returns them to be appended:
// - Compaction_Eff_default: Compaction_Write_GB_default_Sum / (Flush_GB_default_Sum + Add_GB_default_Sum)
// - Compaction_Eff_data_cf: Compaction_Write_GB_data_cf_Sum / (Flush_GB_data_cf_Sum + Add_GB_data_cf_Sum)
// - BC_Hit_Ratio:          BC_Hit_Cum_Delta / (BC_Hit_Cum_Delta + BC_Miss_Cum_Delta)
func computeDerivedExpressions(all []lp.Metric, step time.Duration) []lp.Metric {
	out := make([]lp.Metric, 0, 256)
	if step <= 0 {
		return out
	}
	sumAgg := lp.NewBucketAggregator(step, lp.ModeSum)
	sumAgg.GroupBySource = false
	sumMetrics := sumAgg.Aggregate(all)

	// Compaction efficiency (default/data_cf) based on SUM bucket metrics
	if ms, err := lp.ComputeExpression(sumMetrics,
		"Compaction_Write_GB_default_Sum / (Flush_GB_default_Sum + Add_GB_default_Sum)",
		"Compaction_Eff_default"); err == nil {
		out = append(out, ms...)
	}
	if ms, err := lp.ComputeExpression(sumMetrics,
		"Compaction_Write_GB_data_cf_Sum / (Flush_GB_data_cf_Sum + Add_GB_data_cf_Sum)",
		"Compaction_Eff_data_cf"); err == nil {
		out = append(out, ms...)
	}

	// Block Cache Hit Ratio based on DELTA bucket metrics
	deltaAgg := lp.NewBucketAggregator(step, lp.ModeDelta)
	deltaAgg.GroupBySource = false
	deltaMetrics := deltaAgg.Aggregate(all)
	if ms, err := lp.ComputeExpression(deltaMetrics,
		"BC_Hit_Cum_Delta / (BC_Hit_Cum_Delta + BC_Miss_Cum_Delta)",
		"BC_Hit_Ratio"); err == nil {
		out = append(out, ms...)
	}
	return out
}

func printItem(it lp.LogItem) {
	fmt.Printf("Type: %s\n", it.Type)
	fmt.Printf("Time: %s\n", it.StartTime.Format("2006/01/02-15:04:05.000000"))
	fmt.Println("Content:")
	for _, l := range it.Lines {
		fmt.Println(l)
	}
	fmt.Println()
}

func matchGlob(pattern, path string) bool {
	pb := strings.ToLower(pattern)
	fb := strings.ToLower(path)
	if ok, _ := filepath.Match(pb, fb); ok {
		return true
	}
	// also try on basename
	if ok, _ := filepath.Match(pb, strings.ToLower(filepath.Base(path))); ok {
		return true
	}
	return false
}

func extraFilepath(pattern string) (fs []string) {
	// 匹配当前目录下所有 .txt 文件
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range matches {
		fs = append(fs, file)
	}
	return
}

func main() {
	var startStr, endStr string
	var chartsConfig string
	var chartsOutOne string
	flag.StringVar(&startStr, "start", "", "start time (e.g., 2025/11/30-03:16:58.152255)")
	flag.StringVar(&endStr, "end", "", "end time (e.g., 2025/11/30-08:23)")
	flag.StringVar(&chartsConfig, "charts-config", "", "load chart groups from JSON (raw array or {\"groups\": [...]})")
	flag.StringVar(&chartsOutOne, "charts-out-one", "", "if set, compose all -charts groups into a single stacked SVG output")
	flag.Parse()

	if startStr == "" || endStr == "" {
		fmt.Fprintln(os.Stderr, "missing -start or -end")
		os.Exit(2)
	}
	start, err := parseTimeFlexible(startStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad -start:", err)
		os.Exit(2)
	}
	end, err := parseTimeFlexible(endStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad -end:", err)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "bad -bucket:", err)
		os.Exit(2)
	}

	if chartsConfig == "" {
		fmt.Fprintln(os.Stderr, "bad -charts-config:")
		os.Exit(2)
	}

	groups, typesMap, bucketCfg, err := lp.ParseChartsConfigFull(chartsConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad -charts-config:", err)
		os.Exit(2)
	}

	var allMetrics []lp.Metric
	for t, f := range typesMap {
		switch t {
		case "LOG":
			mp := lp.NewRocksDMetricParser()
			ps := extraFilepath(f)
			for _, p := range ps {
				parser, err := lp.NewRocksDLogParser(p)
				if err != nil {
					fmt.Fprintf(os.Stderr, "cannot open filepath:%s err:%s", p, err.Error())
					os.Exit(2)
				}
				err = parser.Seek(start)
				if err == io.EOF {
					_ = parser.Close()
					continue
				}
				for {
					i, err := parser.Value()
					if err == io.EOF || i.StartTime.After(end) {
						break
					}
					allMetrics = append(allMetrics, mp.Parse(i)...)
					ok := parser.Next()
					if !ok {
						break
					}
				}
				_ = parser.Close()
			}
		case "SLOWLOG":
			mp := lp.NewPikaSlowMetricParser()
			ps := extraFilepath(f)
			for _, p := range ps {
				parser, err := lp.NewPikaSlowLogItemParser(p)
				if err != nil {
					fmt.Fprintf(os.Stderr, "cannot open filepath:%s err:%s", p, err.Error())
					os.Exit(2)
				}
				err = parser.Seek(start)
				if err == io.EOF {
					_ = parser.Close()
					continue
				}
				for {
					i, err := parser.Value()
					if err == io.EOF || i.StartTime.After(end) {
						break
					}
					allMetrics = append(allMetrics, mp.Parse(i)...)
					ok := parser.Next()
					if !ok {
						break
					}
				}
				_ = parser.Close()
			}
		}
	}

	// Prefer config options over CLI when using charts-config
	bucketStep := 10 * time.Minute
	if strings.TrimSpace(bucketCfg) != "" {
		if d, e := time.ParseDuration(strings.TrimSpace(bucketCfg)); e == nil {
			bucketStep = d
		}
	}
	// Ignore CLI -agg; per-group agg from config is used. Default fallback is SUM only if a group omits agg.
	defaultMode := lp.ModeSum

	// Optional chart from raw metrics
	if chartsConfig != "" && chartsOutOne != "" {
		orch := lp.ChartOrchestrator{Groups: groups}
		if err := orch.RenderAllSingleWithAgg(allMetrics, chartsOutOne, bucketStep, defaultMode, false); err != nil {
			fmt.Fprintln(os.Stderr, "render charts (single):", err)
			os.Exit(1)
		}
	} else if chartsConfig != "" {
		orch := lp.ChartOrchestrator{Groups: groups}
		if err := orch.RenderAllWithAgg(allMetrics, bucketStep, defaultMode, false); err != nil {
			fmt.Fprintln(os.Stderr, "render charts:", err)
			os.Exit(1)
		}
	}
}
