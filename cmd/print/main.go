package main

import (
	"flag"
	"fmt"
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

func filterGroupsByFile(groups []lp.ChartGroup, currentFile string, types map[string]string) []lp.ChartGroup {
	if currentFile == "" {
		return groups
	}
	cur := currentFile
	out := make([]lp.ChartGroup, 0, len(groups))
	for _, g := range groups {
		var want string
		if g.Type != "" && types != nil {
			if p, ok := types[g.Type]; ok {
				want = p
			}
		}
		// legacy "file" field support via reflection is not present; we rely on Type+fileTypes
		if want == "" {
			// no constraint, keep
			out = append(out, g)
			continue
		}
		if strings.ContainsAny(want, "*?[]") {
			if matchGlob(want, cur) {
				out = append(out, g)
			}
		} else {
			// plain path or basename
			if strings.EqualFold(filepath.Base(want), filepath.Base(cur)) || strings.EqualFold(want, cur) {
				out = append(out, g)
			}
		}
	}
	return out
}

func main() {
	var file, startStr, endStr string
	var metrics bool
	var metricsOut string
	var pika bool
	var bucketStr string
	var aggStr string
	var metricName string
	var chartOut string
	var chartNamesCSV string
	var chartTitle string
		var chartsSpec string
		var chartsConfig string
		var chartsOutOne string
	flag.StringVar(&file, "file", "LOG", "path to LOG file (RocksDB LOG or pika.ERROR when -pika)")
	flag.StringVar(&startStr, "start", "", "start time (e.g., 2025/11/30-03:16:58.152255)")
	flag.StringVar(&endStr, "end", "", "end time (e.g., 2025/11/30-08:23)")
	flag.BoolVar(&metrics, "metrics", false, "print metrics instead of raw log items")
	flag.StringVar(&metricsOut, "metrics-out", "", "write metrics CSV to file (Time,SourceType,Name,Value,CF)")
	flag.BoolVar(&pika, "pika", false, "parse pika.ERROR slowlog (use PikaSlowLogItemParser)")
	flag.StringVar(&bucketStr, "bucket", "", "aggregate metrics into fixed time buckets (e.g., 10m, 5m)")
	flag.StringVar(&aggStr, "agg", "sum", "aggregation mode: sum|count|first|avg (default: sum)")
	flag.StringVar(&metricName, "metric", "", "only aggregate/print this metric name (exact match; name may already include CF suffix)")
	flag.StringVar(&chartOut, "chart-out", "", "output SVG chart to file (renders selected metrics over time)")
	flag.StringVar(&chartNamesCSV, "chart-names", "", "comma-separated metric names to chart (exact match; multiple series)")
	flag.StringVar(&chartTitle, "chart-title", "", "optional chart title")
		flag.StringVar(&chartsSpec, "charts", "", "multi-chart spec: 'out1.svg:Title1:NameA,NameB; out2.svg:Title2:NameC,NameD' (exact name match)")
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

	var p itParser
	var errOpen error
	if pika {
		p, errOpen = lp.NewPikaSlowLogItemParser(file)
	} else {
		p, errOpen = lp.NewRocksDLogParser(file)
	}
	if errOpen != nil {
		fmt.Fprintln(os.Stderr, "open:", errOpen)
		os.Exit(1)
	}
	defer p.Close()

	if err := p.Seek(start); err != nil {
		if err.Error() == "EOF" {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "seek:", err)
		os.Exit(1)
	}
	item, err := p.Value()
	if err != nil {
		fmt.Fprintln(os.Stderr, "value:", err)
		os.Exit(1)
	}

	printMode := modeItems
	if metrics {
		printMode = modeMetrics
	}

	// Choose metric parser based on input type
	var parseMetricsFn func(lp.LogItem) []lp.Metric
	if pika {
		sp := lp.NewPikaSlowMetricParser()
		parseMetricsFn = sp.Parse
	} else {
		mp := lp.NewRocksDMetricParser()
		parseMetricsFn = mp.Parse
	}

	printedHeader := false
	var allMetrics []lp.Metric
	printMetrics := func(ms []lp.Metric) {
		// optional filtering by metric name
		if metricName != "" {
			filtered := make([]lp.Metric, 0, len(ms))
			for _, m := range ms {
				if m.Name == metricName {
					filtered = append(filtered, m)
				}
			}
			ms = filtered
		}
		if !printedHeader {
			fmt.Println("Time,SourceType,Name,Value")
			printedHeader = true
		}
		for _, m := range ms {
			// CSV without quoting as fields do not include commas by our definitions
			fmt.Printf("%s,%s,%s,%g\n", m.StartTime.Format("2006/01/02-15:04:05.000000"), m.SourceType, m.Name, m.Value)
			allMetrics = append(allMetrics, m)
		}
	}

	doAggregate := bucketStr != ""
	var bucketStep time.Duration
	var aggMode lp.AggregateMode
	if doAggregate {
		var err error
		bucketStep, err = time.ParseDuration(bucketStr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad -bucket:", err)
			os.Exit(2)
		}
		switch aggStr {
		case "count", "COUNT":
			aggMode = lp.ModeCount
		case "sum", "SUM":
			aggMode = lp.ModeSum
		case "first", "FIRST":
			aggMode = lp.ModeFirst
		case "avg", "average", "AVG", "AVERAGE":
			aggMode = lp.ModeAvg
		default:
			switch strings.ToLower(aggStr) {
			case "delta", "diff", "incr", "increment", "incremental":
				aggMode = lp.ModeDelta
			default:
				fmt.Fprintln(os.Stderr, "bad -agg: use count|sum|first|avg|delta")
				os.Exit(2)
			}
		}
	}

	for {
		if item.StartTime.After(end) {
			break
		}
		if printMode == modeMetrics && !doAggregate {
			ms := parseMetricsFn(item)
			printMetrics(ms)
		} else {
			ms := parseMetricsFn(item)
			// filter if needed
			if metricName != "" {
				filtered := make([]lp.Metric, 0, len(ms))
				for _, m := range ms {
					if m.Name == metricName {
						filtered = append(filtered, m)
					}
				}
				ms = filtered
			}
			if doAggregate {
				allMetrics = append(allMetrics, ms...)
			} else if metricsOut != "" || chartOut != "" {
				allMetrics = append(allMetrics, ms...)
			}
			printItem(item)
		}
		if !p.Next() {
			break
		}
		item, err = p.Value()
		if err != nil {
			break
		}
	}

	if doAggregate {
		agg := lp.NewBucketAggregator(bucketStep, aggMode)
		agg.GroupBySource = false
		aggMetrics := agg.Aggregate(allMetrics)
		if metricsOut != "" {
			writer := lp.NewMetric2CSV()
			writer.IncludeHeader = true
			writer.Append = false
			if err := writer.WriteFile(aggMetrics, metricsOut); err != nil {
				fmt.Fprintln(os.Stderr, "write metrics csv:", err)
				os.Exit(1)
			}
		} else {
			// print aggregated to stdout
			fmt.Println("Time,SourceType,Name,Value")
			for _, m := range aggMetrics {
				fmt.Printf("%s,%s,%s,%g\n", m.StartTime.Format("2006/01/02-15:04:05.000000"), m.SourceType, m.Name, m.Value)
			}
		}
		// Optional chart from aggregated metrics
			if chartsConfig != "" && chartsOutOne != "" {
				groups, typesMap, bucketCfg, err := lp.ParseChartsConfigFull(chartsConfig)
				if err != nil {
					fmt.Fprintln(os.Stderr, "bad -charts-config:", err)
					os.Exit(2)
				}
				fgroups := filterGroupsByFile(groups, file, typesMap)
				// Prefer config options over CLI when using charts-config
				if strings.TrimSpace(bucketCfg) != "" {
					if d, e := time.ParseDuration(strings.TrimSpace(bucketCfg)); e == nil {
						bucketStep = d
					}
				}
				// Ignore CLI -agg; per-group agg from config is used. Default fallback is SUM only if a group omits agg.
				defaultMode := lp.ModeSum
				// Compute derived expressions (compaction efficiency, BC hit ratio) and append
				allMetrics = append(allMetrics, computeDerivedExpressions(allMetrics, bucketStep)...)
				orch := lp.ChartOrchestrator{Groups: fgroups}
				if err := orch.RenderAllSingleWithAgg(allMetrics, chartsOutOne, bucketStep, defaultMode, false); err != nil {
					fmt.Fprintln(os.Stderr, "render charts (single):", err)
					os.Exit(1)
				}
			} else if chartsConfig != "" {
				groups, typesMap, bucketCfg, err := lp.ParseChartsConfigFull(chartsConfig)
				if err != nil {
					fmt.Fprintln(os.Stderr, "bad -charts-config:", err)
					os.Exit(2)
				}
				fgroups := filterGroupsByFile(groups, file, typesMap)
				if strings.TrimSpace(bucketCfg) != "" {
					if d, e := time.ParseDuration(strings.TrimSpace(bucketCfg)); e == nil {
						bucketStep = d
					}
				}
				// Ignore CLI -agg; per-group agg from config is used. Default fallback is SUM only if a group omits agg.
				defaultMode := lp.ModeSum
				allMetrics = append(allMetrics, computeDerivedExpressions(allMetrics, bucketStep)...)
				orch := lp.ChartOrchestrator{Groups: fgroups}
				if err := orch.RenderAllWithAgg(allMetrics, bucketStep, defaultMode, false); err != nil {
					fmt.Fprintln(os.Stderr, "render charts:", err)
					os.Exit(1)
				}
			} else if chartsSpec != "" && chartsOutOne != "" {
				groups, err := lp.ParseChartsSpec(chartsSpec)
				if err != nil {
					fmt.Fprintln(os.Stderr, "bad -charts:", err)
					os.Exit(2)
				}
				orch := lp.ChartOrchestrator{Groups: groups}
				if err := orch.RenderAllSingle(aggMetrics, chartsOutOne); err != nil {
					fmt.Fprintln(os.Stderr, "render charts (single):", err)
					os.Exit(1)
				}
			} else if chartsSpec != "" {
				groups, err := lp.ParseChartsSpec(chartsSpec)
				if err != nil {
					fmt.Fprintln(os.Stderr, "bad -charts:", err)
					os.Exit(2)
				}
				orch := lp.ChartOrchestrator{Groups: groups}
				if err := orch.RenderAll(aggMetrics); err != nil {
					fmt.Fprintln(os.Stderr, "render charts:", err)
					os.Exit(1)
				}
			} else if chartOut != "" {
			if chartNamesCSV == "" {
				fmt.Fprintln(os.Stderr, "-chart-names required when -chart-out is set")
				os.Exit(2)
			}
			nameSet := map[string]struct{}{}
			for _, s := range strings.Split(chartNamesCSV, ",") {
				t := strings.TrimSpace(s)
				if t != "" {
					nameSet[t] = struct{}{}
				}
			}
			selected := make([]lp.Metric, 0, len(aggMetrics))
			for _, m := range aggMetrics {
				if _, ok := nameSet[m.Name]; ok {
					selected = append(selected, m)
				}
			}
			dlg := lp.NewDialog()
			if chartTitle != "" {
				dlg.Title = chartTitle
			} else {
				dlg.Title = fmt.Sprintf("Metrics (%s, %s)", bucketStr, strings.ToUpper(aggStr))
			}
			if err := dlg.Render(selected, chartOut); err != nil {
				fmt.Fprintln(os.Stderr, "render chart:", err)
				os.Exit(1)
			}
		}
	} else {
		// If metrics-out is specified, persist all collected metrics as CSV
		if metricsOut != "" {
			writer := lp.NewMetric2CSV()
			writer.IncludeHeader = true
			writer.Append = false
			if err := writer.WriteFile(allMetrics, metricsOut); err != nil {
				fmt.Fprintln(os.Stderr, "write metrics csv:", err)
				os.Exit(1)
			}
		}
		// Optional chart from raw metrics
			if chartsConfig != "" && chartsOutOne != "" {
				groups, typesMap, bucketCfg, err := lp.ParseChartsConfigFull(chartsConfig)
				if err != nil {
					fmt.Fprintln(os.Stderr, "bad -charts-config:", err)
					os.Exit(2)
				}
				fgroups := filterGroupsByFile(groups, file, typesMap)
				if strings.TrimSpace(bucketCfg) != "" {
					if d, e := time.ParseDuration(strings.TrimSpace(bucketCfg)); e == nil {
						bucketStep = d
					}
				}
				// Ignore CLI -agg; per-group agg from config is used. Default fallback is SUM only if a group omits agg.
				defaultMode := lp.ModeSum
				allMetrics = append(allMetrics, computeDerivedExpressions(allMetrics, bucketStep)...)
				orch := lp.ChartOrchestrator{Groups: fgroups}
				if err := orch.RenderAllSingleWithAgg(allMetrics, chartsOutOne, bucketStep, defaultMode, false); err != nil {
					fmt.Fprintln(os.Stderr, "render charts (single):", err)
					os.Exit(1)
				}
			} else if chartsConfig != "" {
				groups, typesMap, bucketCfg, err := lp.ParseChartsConfigFull(chartsConfig)
				if err != nil {
					fmt.Fprintln(os.Stderr, "bad -charts-config:", err)
					os.Exit(2)
				}
				fgroups := filterGroupsByFile(groups, file, typesMap)
				if strings.TrimSpace(bucketCfg) != "" {
					if d, e := time.ParseDuration(strings.TrimSpace(bucketCfg)); e == nil {
						bucketStep = d
					}
				}
				// Ignore CLI -agg; per-group agg from config is used. Default fallback is SUM only if a group omits agg.
				defaultMode := lp.ModeSum
				allMetrics = append(allMetrics, computeDerivedExpressions(allMetrics, bucketStep)...)
				orch := lp.ChartOrchestrator{Groups: fgroups}
				if err := orch.RenderAllWithAgg(allMetrics, bucketStep, defaultMode, false); err != nil {
					fmt.Fprintln(os.Stderr, "render charts:", err)
					os.Exit(1)
				}
			} else if chartsSpec != "" && chartsOutOne != "" {
				groups, err := lp.ParseChartsSpec(chartsSpec)
				if err != nil {
					fmt.Fprintln(os.Stderr, "bad -charts:", err)
					os.Exit(2)
				}
				orch := lp.ChartOrchestrator{Groups: groups}
				if err := orch.RenderAllSingle(allMetrics, chartsOutOne); err != nil {
					fmt.Fprintln(os.Stderr, "render charts (single):", err)
					os.Exit(1)
				}
			} else if chartsSpec != "" {
				groups, err := lp.ParseChartsSpec(chartsSpec)
				if err != nil {
					fmt.Fprintln(os.Stderr, "bad -charts:", err)
					os.Exit(2)
				}
				orch := lp.ChartOrchestrator{Groups: groups}
				if err := orch.RenderAll(allMetrics); err != nil {
					fmt.Fprintln(os.Stderr, "render charts:", err)
					os.Exit(1)
				}
			} else if chartOut != "" {
			if chartNamesCSV == "" {
				fmt.Fprintln(os.Stderr, "-chart-names required when -chart-out is set")
				os.Exit(2)
			}
			nameSet := map[string]struct{}{}
			for _, s := range strings.Split(chartNamesCSV, ",") {
				t := strings.TrimSpace(s)
				if t != "" {
					nameSet[t] = struct{}{}
				}
			}
			selected := make([]lp.Metric, 0, len(allMetrics))
			for _, m := range allMetrics {
				if _, ok := nameSet[m.Name]; ok {
					selected = append(selected, m)
				}
			}
			dlg := lp.NewDialog()
			if chartTitle != "" {
				dlg.Title = chartTitle
			} else {
				dlg.Title = "Metrics"
			}
			if err := dlg.Render(selected, chartOut); err != nil {
				fmt.Fprintln(os.Stderr, "render chart:", err)
				os.Exit(1)
			}
		}
	}
}


