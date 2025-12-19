package logparser

import (
	"bufio"
	"errors"
	"os"
	"regexp"
	"strings"
	"time"
)

// LogType enumerates recognized RocksDB LOG item categories.
type LogType string

const (
	// DUMPING STATS + [/db_impl.cc:670] + "** DB Stats **" 合并后的整体块
	LogTypeDump LogType = "DUMP"
	// STATISTICS 块（带头行 STATISTICS: 及其续行）
	LogTypeStatistics LogType = "STATISTICS"
	// 事件类：compaction/flush/table_file_* 等事件与 pending compaction bytes 提示
	LogTypeEvents LogType = "EVENTS"
	// Pika Slowlog 指标来源
	LogTypeSlowLog LogType = "SLOWLOG"
	// 其他未归类
	LogTypeOther LogType = "OTHER"
)

// LogItem represents one logical log item: starts at a timestamped line, includes
// all following non-timestamp lines; for DUMPING STATS, it also includes the immediate
// next timestamped DB Stats header ([/db_impl.cc:670]) and its continuation.
type LogItem struct {
	StartTime time.Time // head timestamp
	Lines     []string  // all lines belonging to this logical item (without trimming)
	Type      LogType
}

// RocksDLogParser parses RocksDB LOG files item-by-item.
type RocksDLogParser struct {
	path    string
	file    *os.File
	sc      *bufio.Scanner
	reTs    *regexp.Regexp // timestamp-only: YYYY/MM/DD-HH:MM:SS.micros
	reHdr   *regexp.Regexp // strict header (thread, [LEVEL], [/file:line])
	cur     *LogItem
	peekBuf *string
}

// NewRocksDLogParser creates a new RocksDLogParser. Use Close when done.
func NewRocksDLogParser(path string) (*RocksDLogParser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	p := &RocksDLogParser{
		path: path,
		file: f,
		sc:   bufio.NewScanner(f),
		// timestamp-only head
		reTs:  regexp.MustCompile(`^[0-9]{4}/[0-9]{2}/[0-9]{2}-[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+`),
		reHdr: regexp.MustCompile(`^[0-9]{4}/[0-9]{2}/[0-9]{2}-[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+\s+[0-9A-Fa-f]+\s+\[[A-Z]+\]\s+\[/[^]]+:[0-9]+\]`),
	}
	return p, nil
}

// Close releases file handle.
func (p *RocksDLogParser) Close() error {
	if p.file != nil {
		err := p.file.Close()
		p.file = nil
		return err
	}
	return nil
}

// Seek positions to the first log item whose start timestamp >= at.
// After Seek, the matched item is available via Value(). On EOF returns error.
func (p *RocksDLogParser) Seek(at time.Time) error {
	if p.file == nil {
		return errors.New("parser closed")
	}
	// Fast path: if the file's last head timestamp is not after 'at', return EOF quickly.
	if ok, _ := p.fastHasAnyAfter(at); !ok {
		return ioEOF()
	}
	// scan until we find a head with ts >= at
	for {
		line, ok := p.nextLine()
		if !ok {
			return ioEOF()
		}
		lineStripped := stripLOGPrefix(line)
		if !p.reTs.MatchString(lineStripped) {
			continue
		}
		if ht, ok := headTime(line); ok && (ht.Equal(at) || ht.After(at)) {
			// build item starting at current head
			_ = p.buildItemFromHead(line)
			return nil
		}
		// otherwise skip this item quickly: consume continuation lines until next timestamp
		for {
			l2, ok2 := p.nextLine()
			if !ok2 {
				return ioEOF()
			}
			if p.reTs.MatchString(stripLOGPrefix(l2)) {
				p.unread(l2)
				break
			}
		}
	}
}

// Next advances to the next log item.
// It returns true if a next item is available; false on EOF or closed parser.
func (p *RocksDLogParser) Next() bool {
	if p.file == nil {
		return false
	}
	// find next head
	for {
		line, ok := p.nextLine()
		if !ok {
			p.cur = nil
			return false
		}
		if p.reTs.MatchString(stripLOGPrefix(line)) {
			_ = p.buildItemFromHead(line)
			return true
		}
	}
}

// Value returns the last built item (after Seek). Returns error if none.
func (p *RocksDLogParser) Value() (LogItem, error) {
	if p.cur == nil {
		return LogItem{}, errors.New("no current item")
	}
	return *p.cur, nil
}

func (p *RocksDLogParser) buildItemFromHead(head string) LogItem {
	item := LogItem{
		StartTime: func() time.Time { t, _ := headTime(head); return t }(),
		Lines:     []string{head},
		Type:      classifyHead(head),
	}
	// Gather continuation lines until next timestamp (timestamp-only)
	for {
		line, ok := p.nextLine()
		if !ok {
			break
		}
		if p.reTs.MatchString(stripLOGPrefix(line)) {
			// Potential special rule: If this item is a DUMPING STATS item, and the next head
			// is a DB Stats header ([/db_impl.cc:670]), include that head and its continuations,
			// then stop at the subsequent timestamp head.
			if item.Type == LogTypeDump && isDBStatsHead(line) {
				item.Lines = append(item.Lines, line)
				for {
					l2, ok2 := p.nextLine()
					if !ok2 {
						break
					}
					if p.reTs.MatchString(stripLOGPrefix(l2)) {
						p.unread(l2)
						break
					}
					item.Lines = append(item.Lines, l2)
				}
				break
			}
			// Otherwise, next head belongs to the next item
			p.unread(line)
			break
		}
		item.Lines = append(item.Lines, line)
	}
	// If not dump/stat, re-classify by content heuristics
	if item.Type == LogTypeOther {
		item.Type = classifyByContent(item.Lines)
	}
	p.cur = &item
	return item
}

// fastHasAnyAfter checks the tail of the RocksDB LOG file to see if any head timestamp > at exists.
func (p *RocksDLogParser) fastHasAnyAfter(at time.Time) (bool, error) {
	if p.file == nil {
		return false, errors.New("parser closed")
	}
	stat, err := p.file.Stat()
	if err != nil {
		return true, nil
	}
	size := stat.Size()
	if size <= 0 {
		return false, nil
	}
	const tailReadBytes int64 = 1024 * 1024
	start := size - tailReadBytes
	if start < 0 {
		start = 0
	}
	buf := make([]byte, int(size-start))
	_, err = p.file.ReadAt(buf, start)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "eof") {
		// Ignore read-at EOF; otherwise, bail to normal path
		return true, nil
	}
	lastTs := time.Time{}
	lines := strings.Split(string(buf), "\n")
	for _, ln := range lines {
		if t, ok := headTime(ln); ok {
			if t.After(lastTs) {
				lastTs = t
			}
		}
	}
	if lastTs.IsZero() {
		// Not found: fall back to normal Seek scanning.
		return true, nil
	}
	return lastTs.After(at), nil
}

func (p *RocksDLogParser) nextLine() (string, bool) {
	if p.peekBuf != nil {
		s := *p.peekBuf
		p.peekBuf = nil
		return s, true
	}
	if p.sc.Scan() {
		return p.sc.Text(), true
	}
	return "", false
}

func (p *RocksDLogParser) unread(s string) {
	if p.peekBuf != nil {
		panic("unread buffer already occupied")
	}
	p.peekBuf = &s
}

func classifyHead(line string) LogType {
	s := stripLOGPrefix(line)
	if strings.Contains(s, "STATISTICS") {
		return LogTypeStatistics
	}
	if strings.Contains(s, "DUMPING STATS") {
		return LogTypeDump
	}
	// If DB Stats header directly encountered, treat as dump (paired half)
	if isDBStatsHead(line) {
		return LogTypeDump
	}
	return LogTypeOther
}

func classifyByContent(lines []string) LogType {
	// Strongest signals first
	for _, l := range lines {
		s := strings.TrimSpace(l)
		// Events and pending compaction notices
		if strings.Contains(s, `"event": "compaction_started"`) ||
			strings.Contains(s, `"event": "compaction_finished"`) ||
			strings.Contains(s, `"event": "flush_started"`) ||
			strings.Contains(s, `"event": "flush_finished"`) ||
			strings.Contains(s, `"event": "trival_move"`) ||
			strings.Contains(s, `"event": "trivial_move"`) ||
			strings.Contains(s, `"event": "table_file_creation"`) ||
			strings.Contains(s, `"event": "table_file_deletion"`) ||
			strings.Contains(strings.ToLower(s), "table_file_creation") ||
			strings.Contains(strings.ToLower(s), "table_file_deletion") ||
			strings.Contains(s, "Stalling writes because of estimated pending compaction bytes") ||
			strings.Contains(s, "Stopping writes because of estimated pending compaction bytes") {
			return LogTypeEvents
		}
	}
	for _, l := range lines {
		if strings.Contains(l, "STATISTICS") {
			return LogTypeStatistics
		}
		if strings.Contains(l, "DUMPING STATS") || strings.HasPrefix(strings.TrimSpace(l), "** DB Stats **") {
			return LogTypeDump
		}
	}
	return LogTypeOther
}

func isDBStatsHead(line string) bool {
	s := stripLOGPrefix(line)
	// Strict head containing [/db_impl.cc:670]
	return strings.Contains(s, "[/db_impl.cc:670]")
}

func stripLOGPrefix(s string) string {
	return strings.TrimLeft(strings.TrimPrefix(s, "LOG:"), " ")
}

func headTime(s string) (time.Time, bool) {
	s2 := stripLOGPrefix(s)
	// take token up to first space
	ts := s2
	for i := 0; i < len(s2); i++ {
		if s2[i] == ' ' {
			ts = s2[:i]
			break
		}
	}
	// try micros then seconds
	if t, err := time.ParseInLocation("2006/01/02-15:04:05.000000", ts, time.Local); err == nil {
		return t, true
	}
	if t, err := time.ParseInLocation("2006/01/02-15:04:05", ts, time.Local); err == nil {
		return t, true
	}
	return time.Time{}, false
}

func normalizeKey(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", errors.New("empty key")
	}
	// Accept minute-level
	if len(s) == len("2006/01/02-15:04") {
		_, err := time.Parse("2006/01/02-15:04", s)
		if err != nil {
			return "", err
		}
		return s, nil
	}
	// Accept full with microseconds
	// Format like 2006/01/02-15:04:05.000000
	if len(s) >= len("2006/01/02-15:04:05.000000") {
		// We use lexicographic compare, still validate prefix
		if !regexp.MustCompile(`^[0-9]{4}/[0-9]{2}/[0-9]{2}-[0-9]{2}:[0-9]{2}`).MatchString(s) {
			return "", errors.New("bad timestamp format")
		}
		return s, nil
	}
	return "", errors.New("unsupported time format")
}

func ioEOF() error { return errors.New("EOF") }

// PikaSlowLogItemParser groups Pika ERROR slowlog lines into LogItems.
// Each LogItem corresponds to one request (same command + start_time(s)),
// and contains the head "command: ..." line and its related NET_DEBUG line(s).
type PikaSlowLogItemParser struct {
	path        string
	file        *os.File
	sc          *bufio.Scanner
	reGlogTs    *regexp.Regexp
	reCreated   *regexp.Regexp
	reCmdQuoted *regexp.Regexp
	reCmdShort  *regexp.Regexp
	reStartSec  *regexp.Regexp
	curYear     string
	cur         *LogItem
	peekBuf     *string
}

func NewPikaSlowLogItemParser(path string) (*PikaSlowLogItemParser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &PikaSlowLogItemParser{
		path:        path,
		file:        f,
		sc:          bufio.NewScanner(f),
		reGlogTs:    regexp.MustCompile(`^[IWEF]([0-9]{2})([0-9]{2})\s([0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]+))?`),
		reCreated:   regexp.MustCompile(`^Log file created at:\s*([0-9]{4})/([0-9]{2})/([0-9]{2})\s+([0-9]{2}:[0-9]{2}:[0-9]{2})`),
		reCmdQuoted: regexp.MustCompile(`(?i)\bcommand\s*:\s*\"([^\"]+)\"`),
		reCmdShort:  regexp.MustCompile(`(?i)\bcmd\s*:\s*([a-z_]+)`),
		reStartSec:  regexp.MustCompile(`\bstart_time\(s\)\s*:\s*([0-9]+)\b`),
	}, nil
}

func (p *PikaSlowLogItemParser) Close() error {
	if p.file != nil {
		err := p.file.Close()
		p.file = nil
		return err
	}
	return nil
}

// Seek positions to the first slowlog item whose head timestamp >= at.
func (p *PikaSlowLogItemParser) Seek(at time.Time) error {
	if p.file == nil {
		return errors.New("parser closed")
	}
	// Fast path: if the file's last head timestamp is not after 'at', return EOF quickly.
	if ok, _ := p.fastHasAnyAfter(at); !ok {
		return errors.New("EOF")
	}
	for {
		line, ok := p.nextLine()
		if !ok {
			return errors.New("EOF")
		}
		ts, isHead := p.parseGlogTs(line)
		if !isHead {
			p.tryUpdateCreated(line)
			continue
		}
		// Fast-forward: if this head is earlier than target time, skip to next head
		// (avoid spending time on non-head lines between items)
		if ts.Before(at) {
			for {
				l2, ok2 := p.nextLine()
				if !ok2 {
					return errors.New("EOF")
				}
				if t2, isHead2 := p.parseGlogTs(l2); isHead2 {
					// If still before target, keep skipping; else evaluate this head
					if t2.Before(at) {
						continue
					}
					// We found a head at/after target; reuse it
					line = l2
					isHead = true
					break
				} else {
					p.tryUpdateCreated(l2)
				}
			}
		}
		// at this point, line is a head with ts >= at; advance until we find a command head
		if p.isCommandHead(line) {
			_ = p.buildItemFromHead(line)
			return nil
		}
		// otherwise continue scanning
	}
}

// fastHasAnyAfter checks the tail of the file to see if there exists any head timestamp > at.
// It avoids full-file scanning when the target time is beyond the file's last entry.
func (p *PikaSlowLogItemParser) fastHasAnyAfter(at time.Time) (bool, error) {
	if p.file == nil {
		return false, errors.New("parser closed")
	}
	stat, err := p.file.Stat()
	if err != nil {
		return true, nil
	}
	size := stat.Size()
	if size <= 0 {
		return false, nil
	}
	// Best-effort: try to detect year from file head if not known.
	year := p.curYear
	if year == "" {
		if y, ok := p.scanYearFromHead(); ok {
			year = y
		}
	}
	if year == "" {
		year = "2025"
	}
	// Read last chunk of the file (up to 1MB) and find the last head timestamp.
	const tailReadBytes int64 = 1024 * 1024
	start := size - tailReadBytes
	if start < 0 {
		start = 0
	}
	buf := make([]byte, int(size-start))
	_, err = p.file.ReadAt(buf, start)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "eof") {
		// Ignore read-at EOF; otherwise, bail to normal path
		return true, nil
	}
	lastTs := time.Time{}
	lines := strings.Split(string(buf), "\n")
	for _, ln := range lines {
		// parseGlogTs requires full line; reuse logic with temporary year
		t, ok := p.parseGlogTsWithYear(ln, year)
		if ok {
			if t.After(lastTs) {
				lastTs = t
			}
		}
	}
	if lastTs.IsZero() {
		// Not found: fall back to normal Seek to be safe.
		return true, nil
	}
	return lastTs.After(at), nil
}

// scanYearFromHead reads a small prefix of the file and attempts to capture the year from
// "Log file created at: YYYY/MM/DD ..." lines.
func (p *PikaSlowLogItemParser) scanYearFromHead() (string, bool) {
	if p.file == nil {
		return "", false
	}
	const headReadBytes int64 = 128 * 1024
	buf := make([]byte, headReadBytes)
	n, err := p.file.ReadAt(buf, 0)
	if err != nil && n <= 0 {
		return "", false
	}
	data := string(buf[:n])
	for _, ln := range strings.Split(data, "\n") {
		s := strings.TrimLeft(strings.TrimPrefix(ln, "LOG:"), " ")
		if c := p.reCreated.FindStringSubmatch(s); len(c) == 5 {
			return c[1], true
		}
	}
	return "", false
}

// parseGlogTsWithYear parses a glog-style head timestamp using a provided year fallback.
func (p *PikaSlowLogItemParser) parseGlogTsWithYear(line string, year string) (time.Time, bool) {
	s := strings.TrimLeft(strings.TrimPrefix(line, "LOG:"), " ")
	m := p.reGlogTs.FindStringSubmatch(s)
	if len(m) < 4 {
		return time.Time{}, false
	}
	mon, day, hms := m[1], m[2], m[3]
	mic := ""
	if len(m) >= 5 {
		mic = m[4]
	}
	if year == "" {
		year = "2025"
	}
	if mic != "" {
		if t, err := time.ParseInLocation("2006/01/02-15:04:05.000000", year+"/"+mon+"/"+day+"-"+hms+"."+mic, time.Local); err == nil {
			return t, true
		}
	} else {
		if t, err := time.ParseInLocation("2006/01/02-15:04:05", year+"/"+mon+"/"+day+"-"+hms, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// Next advances to the next slowlog item.
func (p *PikaSlowLogItemParser) Next() bool {
	if p.file == nil {
		return false
	}
	for {
		line, ok := p.nextLine()
		if !ok {
			p.cur = nil
			return false
		}
		_, isHead := p.parseGlogTs(line)
		if !isHead {
			p.tryUpdateCreated(line)
			continue
		}
		if p.isCommandHead(line) {
			_ = p.buildItemFromHead(line)
			return true
		}
	}
}

// Value returns the current LogItem.
func (p *PikaSlowLogItemParser) Value() (LogItem, error) {
	if p.cur == nil {
		return LogItem{}, errors.New("no current item")
	}
	return *p.cur, nil
}

func (p *PikaSlowLogItemParser) buildItemFromHead(head string) LogItem {
	ts, _ := p.parseGlogTs(head)
	item := LogItem{
		StartTime: ts,
		Lines:     []string{head},
		Type:      LogTypeSlowLog,
	}
	cmd, startSec := p.extractCommandAndStart(head)
	// collect continuation lines until next command head
	for {
		line, ok := p.nextLine()
		if !ok {
			break
		}
		if p.isCommandHead(line) {
			p.unread(line)
			break
		}
		// include NET_DEBUG line for same command
		if p.isNetDebugForCmd(line, cmd) {
			item.Lines = append(item.Lines, line)
			continue
		}
		// include lines that mention the same start_time(s) (rare)
		if startSec != "" && p.hasStartSec(line, startSec) {
			item.Lines = append(item.Lines, line)
			continue
		}
		// otherwise ignore unrelated noise
	}
	p.cur = &item
	return item
}

func (p *PikaSlowLogItemParser) parseGlogTs(line string) (time.Time, bool) {
	s := strings.TrimLeft(strings.TrimPrefix(line, "LOG:"), " ")
	m := p.reGlogTs.FindStringSubmatch(s)
	if len(m) < 4 {
		return time.Time{}, false
	}
	mon, day, hms := m[1], m[2], m[3]
	mic := ""
	if len(m) >= 5 {
		mic = m[4]
	}
	year := p.curYear
	if year == "" {
		year = "2025"
	}
	if mic != "" {
		if t, err := time.ParseInLocation("2006/01/02-15:04:05.000000", year+"/"+mon+"/"+day+"-"+hms+"."+mic, time.Local); err == nil {
			return t, true
		}
	} else {
		if t, err := time.ParseInLocation("2006/01/02-15:04:05", year+"/"+mon+"/"+day+"-"+hms, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func (p *PikaSlowLogItemParser) tryUpdateCreated(line string) {
	s := strings.TrimLeft(strings.TrimPrefix(line, "LOG:"), " ")
	if c := p.reCreated.FindStringSubmatch(s); len(c) == 5 {
		p.curYear = c[1]
	}
}

func (p *PikaSlowLogItemParser) isCommandHead(line string) bool {
	s := strings.TrimSpace(strings.TrimLeft(strings.TrimPrefix(line, "LOG:"), " "))
	if p.reCmdQuoted.MatchString(s) {
		return true
	}
	// replica bgworker slow line: command: pkbulkload ...
	if strings.Contains(s, "pika_repl_bgworker.cc") && strings.Contains(strings.ToLower(s), "command:") {
		return true
	}
	// some heads may include start_time(s)
	if p.reStartSec.MatchString(s) && (strings.Contains(strings.ToLower(s), "command:") || strings.Contains(strings.ToLower(s), "cmd:")) {
		return true
	}
	return false
}

func (p *PikaSlowLogItemParser) isNetDebugForCmd(line string, cmdUpper string) bool {
	if cmdUpper == "" {
		return false
	}
	s := strings.TrimSpace(strings.ToLower(line))
	// NET_DEBUG ... cmd: <lowercase>
	m := p.reCmdShort.FindStringSubmatch(s)
	if len(m) == 2 {
		return strings.ToUpper(strings.TrimSpace(m[1])) == cmdUpper
	}
	return false
}

func (p *PikaSlowLogItemParser) extractCommandAndStart(line string) (string, string) {
	s := strings.TrimSpace(strings.TrimLeft(strings.TrimPrefix(line, "LOG:"), " "))
	// command quoted
	if m := p.reCmdQuoted.FindStringSubmatch(s); len(m) == 2 {
		cmd := strings.ToUpper(strings.TrimSpace(m[1]))
		st := ""
		if m2 := p.reStartSec.FindStringSubmatch(s); len(m2) == 2 {
			st = m2[1]
		}
		return cmd, st
	}
	// repl bgworker
	idx := strings.Index(strings.ToLower(s), "command:")
	if idx >= 0 {
		rest := strings.TrimSpace(s[idx+len("command:"):])
		fields := strings.Fields(rest)
		if len(fields) > 0 {
			cmd := strings.ToUpper(strings.Trim(fields[0], `",`))
			st := ""
			if m2 := p.reStartSec.FindStringSubmatch(s); len(m2) == 2 {
				st = m2[1]
			}
			return cmd, st
		}
	}
	return "", ""
}

func (p *PikaSlowLogItemParser) hasStartSec(line string, st string) bool {
	s := strings.TrimSpace(strings.TrimLeft(strings.TrimPrefix(line, "LOG:"), " "))
	if m := p.reStartSec.FindStringSubmatch(s); len(m) == 2 {
		return m[1] == st
	}
	return false
}

func (p *PikaSlowLogItemParser) nextLine() (string, bool) {
	if p.peekBuf != nil {
		s := *p.peekBuf
		p.peekBuf = nil
		return s, true
	}
	if p.sc.Scan() {
		return p.sc.Text(), true
	}
	return "", false
}

func (p *PikaSlowLogItemParser) unread(s string) {
	if p.peekBuf != nil {
		panic("unread buffer already occupied")
	}
	p.peekBuf = &s
}
