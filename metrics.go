package logparser

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Metric represents one extracted metric datum from a LogItem.
// - SourceType: the LogItem type the metric came from
// - StartTime: the LogItem start time
// - Name: metric name (e.g., "DB_Ingest_MB", "BC_Hit_Cum", "Level0_Files")
// - Value: numeric value
type Metric struct {
	SourceType LogType
	StartTime  time.Time
	Name       string
	Value      float64
}

// RocksDMetricParser extracts useful metrics from a LogItem.
// Provide Parse(item) to get all metrics for that item.
type RocksDMetricParser struct{}

func NewRocksDMetricParser() *RocksDMetricParser { return &RocksDMetricParser{} }

// Parse returns all metrics extracted from the given item.
func (mp *RocksDMetricParser) Parse(item LogItem) []Metric {
	switch item.Type {
	case LogTypeStatistics:
		return mp.parseStatistics(item)
	case LogTypeDump:
		return mp.parseDump(item)
	case LogTypeEvents:
		return mp.parseEvents(item)
	default:
		return nil
	}
}

// ===== STATISTICS parsing (counts + P99) =====
var (
	reCountStat = map[string]*regexp.Regexp{
		"BC_Hit_Cum":       regexp.MustCompile(`^rocksdb\.block\.cache\.hit\s+COUNT\s*:\s*([0-9]+)`),
		"BC_Miss_Cum":      regexp.MustCompile(`^rocksdb\.block\.cache\.miss\s+COUNT\s*:\s*([0-9]+)`),
		"Bloom_Useful_Cum": regexp.MustCompile(`^rocksdb\.bloom\.filter\.useful\s+COUNT\s*:\s*([0-9]+)`),
		"File_Opens_Cum":   regexp.MustCompile(`^rocksdb\.no\.file\.opens\s+COUNT\s*:\s*([0-9]+)`),
		"DB_Seek_Cum":      regexp.MustCompile(`^rocksdb\.number\.db\.seek\s+COUNT\s*:\s*([0-9]+)`),
		"DB_Next_Cum":      regexp.MustCompile(`^rocksdb\.number\.db\.next\s+COUNT\s*:\s*([0-9]+)`),
	}
	reP99Num = regexp.MustCompile(`P99\s*:\s*([0-9.]+)`)
)

func (mp *RocksDMetricParser) parseStatistics(item LogItem) []Metric {
	var out []Metric
	seen := map[string]struct{}{} // key: name|cf
	add := func(name string, v float64) {
		key := name
		if _, ok := seen[key]; ok {
			return // keep first occurrence
		}
		seen[key] = struct{}{}
		out = append(out, Metric{SourceType: item.Type, StartTime: item.StartTime, Name: name, Value: v})
	}
	for _, line := range item.Lines {
		s := strings.TrimSpace(line)
		// counts
		for name, re := range reCountStat {
			if m := re.FindStringSubmatch(s); len(m) == 2 {
				if v, err := strconv.ParseFloat(m[1], 64); err == nil {
					add(name, v)
				}
			}
		}
		// P99 families
		switch {
		case strings.HasPrefix(s, "rocksdb.table.open.io.micros"):
			if v, ok := pickP99(s); ok {
				add("TableOpenIO_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.bytes.per.read"):
			if v, ok := pickP99(s); ok {
				add("BytesPerRead_P99", v)
			}
		case strings.HasPrefix(s, "rocksdb.db.get.micros"):
			if v, ok := pickP99(s); ok {
				add("DB_Get_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.db.write.micros"):
			if v, ok := pickP99(s); ok {
				add("DB_Write_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.compaction.times.micros"):
			if v, ok := pickP99(s); ok {
				add("Compaction_Times_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.table.sync.micros"):
			if v, ok := pickP99(s); ok {
				add("Table_Sync_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.compaction.outfile.sync.micros"):
			if v, ok := pickP99(s); ok {
				add("Compaction_Outfile_Sync_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.manifest.file.sync.micros"):
			if v, ok := pickP99(s); ok {
				add("Manifest_Sync_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.read.block.get.micros"):
			if v, ok := pickP99(s); ok {
				add("Read_Block_Get_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.sst.read.micros"):
			if v, ok := pickP99(s); ok {
				add("SST_Read_P99_us", v)
			}
		case strings.HasPrefix(s, "rocksdb.db.seek.micros"):
			if v, ok := pickP99(s); ok {
				add("DB_Seek_P99_us", v)
			}
		}
	}
	return out
}

func pickP99(line string) (float64, bool) {
	m := reP99Num.FindStringSubmatch(line)
	if len(m) != 2 {
		return 0, false
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// ===== DUMP parsing (Interval/CF summaries/Level lines) =====
var (
	// Interval writes: ... ingest: 0.02 MB, 0.00 MB/s
	reIntervalWrites = regexp.MustCompile(`^Interval writes:.*ingest:\s*([0-9.]+)\s*(KB|MB|GB),\s*([0-9.]+)\s*MB/s`)
	// Interval WAL: ... written: 0.00 MB, 0.00 MB/s
	reIntervalWAL = regexp.MustCompile(`^Interval WAL:.*written:\s*([0-9.]+)\s*(KB|MB|GB),\s*([0-9.]+)\s*MB/s`)
	// Uptime(secs): total, interval
	reUptime = regexp.MustCompile(`^Uptime\(secs\):\s*([0-9.]+)\s*total,\s*([0-9.]+)\s*interval`)
	// Flush(GB): cumulative X, interval Y
	reFlushGB = regexp.MustCompile(`^Flush\(GB\):\s*cumulative\s*([0-9.]+),\s*interval\s*([0-9.]+)`)
	// AddFile(GB): cumulative X, interval Y
	reAddFileGB = regexp.MustCompile(`^AddFile\(GB\):\s*cumulative\s*([0-9.]+),\s*interval\s*([0-9.]+)`)
	// AddFile(Total Files): cumulative X, interval Y
	reAddTotalFiles = regexp.MustCompile(`^AddFile\(Total Files\):\s*cumulative\s*([0-9]+),\s*interval\s*([0-9]+)`)
	// AddFile(L0 Files): cumulative X, interval Y
	reAddL0Files = regexp.MustCompile(`^AddFile\(L0 Files\):\s*cumulative\s*([0-9]+),\s*interval\s*([0-9]+)`)
	// Cumulative compaction: ... GB write, ... MB/s write, ... GB read, ... MB/s read, ... seconds
	reCumComp = regexp.MustCompile(`^Cumulative compaction:\s*([0-9.]+)\s*GB write,\s*([0-9.]+)\s*MB/s write,\s*([0-9.]+)\s*GB read,\s*([0-9.]+)\s*MB/s read,\s*([0-9.]+)\s*seconds`)
	// Interval compaction: similar pattern
	reIntComp = regexp.MustCompile(`^Interval compaction:\s*([0-9.]+)\s*GB write,\s*([0-9.]+)\s*MB/s write,\s*([0-9.]+)\s*GB read,\s*([0-9.]+)\s*MB/s read,\s*([0-9.]+)\s*seconds`)
	// Per-level line: Lx a/b Size Unit Score Read(GB) Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
	reLevel = regexp.MustCompile(`^L([0-6])\s+([0-9]+)/([0-9]+)\s+([0-9.]+)\s+(KB|MB|GB)`)
	// Compaction stats header: ** Compaction Stats [cf] **
	reCompStatsHdr = regexp.MustCompile(`^\*\* Compaction Stats \[([^\]]+)\] \*\*`)
	// Histogram header: ** File Read Latency Histogram By Level [cf] **
	reHistHdr = regexp.MustCompile(`^\*\* File Read Latency Histogram By Level \[([^\]]+)\] \*\*`)
)

func (mp *RocksDMetricParser) parseDump(item LogItem) []Metric {
	var out []Metric
	seen := map[string]struct{}{} // key: name|cf
	add := func(name string, v float64, cf string) {
		suffix := ""
		if cf != "" {
			suffix = "_" + cf
		}
		fullName := name + suffix
		key := fullName
		if _, ok := seen[key]; ok {
			return // keep first occurrence
		}
		seen[key] = struct{}{}
		out = append(out, Metric{SourceType: item.Type, StartTime: item.StartTime, Name: fullName, Value: v})
	}
	currentCF := "" // "", "default", "data_cf", etc.
	for _, line := range item.Lines {
		s := strings.TrimSpace(line)
		// CF context detection
		if m := reCompStatsHdr.FindStringSubmatch(s); len(m) == 2 {
			currentCF = strings.ToLower(m[1])
			continue
		}
		if m := reHistHdr.FindStringSubmatch(s); len(m) == 2 {
			// histogram starts; treat as CF context end for summary parsing
			currentCF = ""
			continue
		}
		// Interval writes
		if m := reIntervalWrites.FindStringSubmatch(s); len(m) == 4 {
			ingMB := toMB(m[1], m[2])
			ingMBps, _ := strconv.ParseFloat(m[3], 64)
			add("DB_Ingest_MB", ingMB, "")
			add("DB_Ingest_MBps", ingMBps, "")
			continue
		}
		// Interval WAL
		if m := reIntervalWAL.FindStringSubmatch(s); len(m) == 4 {
			wMB := toMB(m[1], m[2])
			wMBps, _ := strconv.ParseFloat(m[3], 64)
			add("WAL_Written_MB", wMB, "")
			add("WAL_MBps", wMBps, "")
			continue
		}
		// Uptime(secs)
		if m := reUptime.FindStringSubmatch(s); len(m) == 3 {
			intv, _ := strconv.ParseFloat(m[2], 64)
			add("Uptime_Sec", intv, currentCF)
			continue
		}
		// Flush/AddFile counters (interval)
		if m := reFlushGB.FindStringSubmatch(s); len(m) == 3 {
			v, _ := strconv.ParseFloat(m[2], 64)
			add("Flush_GB", v, currentCF)
			continue
		}
		if m := reAddFileGB.FindStringSubmatch(s); len(m) == 3 {
			v, _ := strconv.ParseFloat(m[2], 64)
			add("Add_GB", v, currentCF)
			continue
		}
		if m := reAddTotalFiles.FindStringSubmatch(s); len(m) == 3 {
			v, _ := strconv.ParseFloat(m[2], 64)
			add("Add_TotalFiles", v, currentCF)
			continue
		}
		if m := reAddL0Files.FindStringSubmatch(s); len(m) == 3 {
			v, _ := strconv.ParseFloat(m[2], 64)
			add("Add_L0Files", v, currentCF)
			continue
		}
		// Compaction summaries
		if m := reCumComp.FindStringSubmatch(s); len(m) == 6 {
			wgb, _ := strconv.ParseFloat(m[1], 64)
			wmbps, _ := strconv.ParseFloat(m[2], 64)
			rgb, _ := strconv.ParseFloat(m[3], 64)
			rmbps, _ := strconv.ParseFloat(m[4], 64)
			sec, _ := strconv.ParseFloat(m[5], 64)
			add("Cum_Compaction_Write_GB", wgb, currentCF)
			add("Cum_Compaction_Write_MBps", wmbps, currentCF)
			add("Cum_Compaction_Read_GB", rgb, currentCF)
			add("Cum_Compaction_Read_MBps", rmbps, currentCF)
			add("Cum_Compaction_Sec", sec, currentCF)
			continue
		}
		if m := reIntComp.FindStringSubmatch(s); len(m) == 6 {
			wgb, _ := strconv.ParseFloat(m[1], 64)
			wmbps, _ := strconv.ParseFloat(m[2], 64)
			rgb, _ := strconv.ParseFloat(m[3], 64)
			rmbps, _ := strconv.ParseFloat(m[4], 64)
			sec, _ := strconv.ParseFloat(m[5], 64)
			add("Compaction_Write_GB", wgb, currentCF)
			add("Compaction_Write_MBps", wmbps, currentCF)
			add("Compaction_Read_GB", rgb, currentCF)
			add("Compaction_Read_MBps", rmbps, currentCF)
			add("Compaction_Sec", sec, currentCF)
			continue
		}
		// Per-level key metrics (files/size)
		if m := reLevel.FindStringSubmatch(s); len(m) == 6 {
			lvl := m[1]
			files, _ := strconv.ParseFloat(m[2], 64)
			sizeMB := toMB(m[4], m[5])
			add("Level"+lvl+"_Files", files, currentCF)
			add("Level"+lvl+"_Size_MB", sizeMB, currentCF)
			// Additional columns (W-Amp, Rd(MB/s), etc.) can be parsed if needed with a richer regex.
			continue
		}
	}
	return out
}

// ===== Events parsing =====
var (
	reEventName = regexp.MustCompile(`"event"\s*:\s*"([^"]+)"`)
	reCFName    = regexp.MustCompile(`"cf_name"\s*:\s*"([^"]+)"`)
	// Common numeric fields in EVENT_LOG
	reNumFields = map[string]*regexp.Regexp{
		"bytes_written":  regexp.MustCompile(`"bytes_written"\s*:\s*([0-9]+)`),
		"file_size":      regexp.MustCompile(`"file_size"\s*:\s*([0-9]+)`),
		"bytes":          regexp.MustCompile(`"bytes"\s*:\s*([0-9]+)`),
		"micros":         regexp.MustCompile(`"micros"\s*:\s*([0-9]+)`),
		"size":           regexp.MustCompile(`"size"\s*:\s*([0-9]+)`),
		"data_size":      regexp.MustCompile(`"data_size"\s*:\s*([0-9]+)`),
		"wal_file_bytes": regexp.MustCompile(`"wal_file_bytes"\s*:\s*([0-9]+)`),
		"tables":         regexp.MustCompile(`"tables"\s*:\s*([0-9]+)`),
		"files":          regexp.MustCompile(`"files"\s*:\s*([0-9]+)`),
	}
	// Non-JSON stall notices
	rePendingStall = regexp.MustCompile(`(?i)(Stalling|Stopping) writes because of estimated pending compaction bytes`)
)

func (mp *RocksDMetricParser) parseEvents(item LogItem) []Metric {
	var out []Metric
	seen := map[string]struct{}{} // key: name|cf
	add := func(name string, v float64, cf string) {
		if cf != "" {
			name = name + "_" + strings.ToLower(cf)
		}
		key := name
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, Metric{SourceType: item.Type, StartTime: item.StartTime, Name: name, Value: v})
	}

	for _, line := range item.Lines {
		s := strings.TrimSpace(line)
		cf := ""
		if m := reCFName.FindStringSubmatch(s); len(m) == 2 {
			cf = strings.ToLower(m[1])
		}
		// Count the event
		if m := reEventName.FindStringSubmatch(s); len(m) == 2 {
			ev := m[1]
			add("Event_"+canonicalizeMetricName(ev)+"_Count", 1, cf)
			// Extract common numeric fields for this event
			for fname, re := range reNumFields {
				if n := re.FindStringSubmatch(s); len(n) == 2 {
					if v, err := strconv.ParseFloat(n[1], 64); err == nil {
						add("Event_"+canonicalizeMetricName(ev)+"_"+canonicalizeFieldName(fname), v, cf)
					}
				}
			}
			continue
		}
		// Non-JSON stall events
		if rePendingStall.MatchString(s) {
			add("Event_PendingCompactionBytes_Stall_Count", 1, cf)
			continue
		}
	}
	return out
}

func canonicalizeMetricName(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return s
}

func canonicalizeFieldName(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return s
}

// ===== Compaction stats parsing (subset) =====
func (mp *RocksDMetricParser) parseCompactionStats(item LogItem) []Metric {
	// For now, rely on Dump parser handling when compaction stats appear in DUMP.
	// If a standalone compaction stats item appears, we can reuse parseDump to extract per-level metrics.
	return mp.parseDump(item)
}

// ===== Read latency histogram parsing (subset: levels + P99 captured as content) =====
func (mp *RocksDMetricParser) parseReadLatency(item LogItem) []Metric {
	// Histograms are primarily informational; aggregate P99 per level would require extra stateful parsing.
	// Keep minimal here; DUMP/STATISTICS parsers already cover P99 series where needed.
	return nil
}

func toMB(vs string, unit string) float64 {
	v, _ := strconv.ParseFloat(vs, 64)
	switch strings.ToUpper(unit) {
	case "GB":
		return v * 1024.0
	case "KB":
		return v / 1024.0
	default:
		return v
	}
}

// ===== PIKA SLOWLOG metrics from LogItem =====
type PikaSlowMetricParser struct{}

func NewPikaSlowMetricParser() *PikaSlowMetricParser { return &PikaSlowMetricParser{} }

var (
	reSlowCmdQuoted = regexp.MustCompile(`(?i)\bcommand\s*:\s*\"([^\"]+)\"`)
	reSlowCmdShort  = regexp.MustCompile(`(?i)\bcmd\s*:\s*([a-z_]+)`)
	reSlowCmdWord   = regexp.MustCompile(`(?i)\bcommand\s*:\s*([A-Za-z_]+)\b`)
)

// Parse converts a SLOWLOG LogItem into one or more metrics.
// Current rule: emit a single count metric per item: Slow_Command_<CMD>=1.
func (sp *PikaSlowMetricParser) Parse(item LogItem) []Metric {
	if item.Type != LogTypeSlowLog {
		return nil
	}
	cmd := ""
	for _, line := range item.Lines {
		s := strings.TrimSpace(line)
		if m := reSlowCmdQuoted.FindStringSubmatch(s); len(m) == 2 {
			cmd = strings.ToUpper(strings.TrimSpace(m[1]))
			break
		}
		if m := reSlowCmdShort.FindStringSubmatch(strings.ToLower(s)); len(m) == 2 {
			cmd = strings.ToUpper(strings.TrimSpace(m[1]))
			break
		}
		if m := reSlowCmdWord.FindStringSubmatch(s); len(m) == 2 {
			cmd = strings.ToUpper(strings.TrimSpace(m[1]))
			break
		}
	}
	if cmd == "" {
		return nil
	}
	name := "Slow_Command_" + cmd
	return []Metric{{
		SourceType: item.Type,
		StartTime:  item.StartTime,
		Name:       name,
		Value:      1,
	}}
}


