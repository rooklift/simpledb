package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	filepathpkg "path/filepath"
	"sort"
	"strings"
)

var (
	records    []map[string]string
	fields     []string
	filepath   string
	loaded     bool
	sortField  string
	deleteHint int
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		handleCommand(line)
	}
}

func respond(v interface{}) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func respondError(msg string) {
	respond(map[string]interface{}{"error": msg})
}

func respondOK(extra map[string]interface{}) {
	r := map[string]interface{}{"ok": true}
	for k, v := range extra {
		r[k] = v
	}
	respond(r)
}

func handleCommand(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}

	if strings.HasPrefix(line, "expect ") {
		cmdExpect(line[7:])
	} else if strings.HasPrefix(line, "load ") {
		cmdLoad(strings.TrimSpace(line[5:]))
	} else if line == "save" {
		cmdSave()
	} else if line == "quit" {
		os.Exit(0)
	} else if strings.HasPrefix(line, "add ") {
		cmdAdd(line[4:])
	} else if line == "count" {
		cmdCount()
	} else if line == "clear" {
		cmdClear()
	} else if strings.HasPrefix(line, "sort ") {
		cmdSort(strings.TrimSpace(line[5:]))
	} else if strings.HasPrefix(line, "select ") {
		cmdSelect(line[7:])
	} else if strings.HasPrefix(line, "deleteone ") {
		cmdDeleteOne(line[10:])
	} else if strings.HasPrefix(line, "delete ") {
		cmdDelete(line[7:])
	} else {
		respondError("unknown command")
	}
}

func cmdExpect(payload string) {
	if fields != nil {
		respondError("fields already set")
		return
	}

	var newFields []string
	if err := json.Unmarshal([]byte(payload), &newFields); err != nil {
		respondError(fmt.Sprintf("bad field list: %v", err))
		return
	}
	if len(newFields) == 0 {
		respondError("field list cannot be empty")
		return
	}

	fields = newFields
	respondOK(nil)
}

func cmdLoad(path string) {
	if fields == nil {
		respondError("must call expect first")
		return
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			filepath = path
			records = nil
			loaded = true
			sortField = ""
			deleteHint = 0
			respondOK(map[string]interface{}{"count": 0})
			return
		}
		respondError(fmt.Sprintf("cannot open file: %v", err))
		return
	}
	defer f.Close()

	var temp []map[string]string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		var rec map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			respondError(fmt.Sprintf("bad record on line %d: %v", lineNum, err))
			return
		}
		if msg := validateRecord(rec); msg != "" {
			respondError(fmt.Sprintf("%s on line %d", msg, lineNum))
			return
		}
		temp = append(temp, rec)
	}

	if err := scanner.Err(); err != nil {
		respondError(fmt.Sprintf("read error: %v", err))
		return
	}

	filepath = path
	records = temp
	loaded = true
	sortField = ""
	deleteHint = 0
	respondOK(map[string]interface{}{"count": len(records)})
}

func cmdSave() {
	if !loaded {
		respondError("no file loaded")
		return
	}

	dir := filepathpkg.Dir(filepath)

	f, err := os.CreateTemp(dir, "simpledb-*.jsonl")
	if err != nil {
		respondError(fmt.Sprintf("cannot create temp file: %v", err))
		return
	}
	tempPath := f.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tempPath)
		}
	}()

	w := bufio.NewWriter(f)
	var writeErr error
	for _, rec := range records {
		b, err := json.Marshal(rec)
		if err != nil {
			writeErr = err
			break
		}
		if _, err := w.Write(b); err != nil {
			writeErr = err
			break
		}
		if err := w.WriteByte('\n'); err != nil {
			writeErr = err
			break
		}
	}
	if writeErr == nil {
		writeErr = w.Flush()
	}
	if err := f.Close(); err != nil && writeErr == nil {
		writeErr = err
	}

	if writeErr != nil {
		respondError(fmt.Sprintf("write error: %v", writeErr))
		return
	}

	backupPath := ""
	if _, err := os.Stat(filepath); err == nil {
		backupPath = tempPath + ".bak"
		if err := os.Rename(filepath, backupPath); err != nil {
			respondError(fmt.Sprintf("cannot stage old file: %v", err))
			return
		}
	} else if !os.IsNotExist(err) {
		respondError(fmt.Sprintf("cannot access file: %v", err))
		return
	}

	if err := os.Rename(tempPath, filepath); err != nil {
		if backupPath != "" {
			_ = os.Rename(backupPath, filepath)
		}
		respondError(fmt.Sprintf("cannot replace file: %v", err))
		return
	}
	cleanupTemp = false
	if backupPath != "" {
		_ = os.Remove(backupPath)
	}

	respondOK(nil)
}

func cmdAdd(payload string) {
	if !loaded {
		respondError("no file loaded")
		return
	}

	var rec map[string]string
	if err := json.Unmarshal([]byte(payload), &rec); err != nil {
		respondError(fmt.Sprintf("bad json: %v", err))
		return
	}

	if msg := validateRecord(rec); msg != "" {
		respondError(msg)
		return
	}

	if sortField != "" {
		val := strings.ToLower(rec[sortField])
		i := sort.Search(len(records), func(i int) bool {
			return strings.ToLower(records[i][sortField]) >= val
		})
		records = append(records, nil)
		copy(records[i+1:], records[i:])
		records[i] = rec
	} else {
		records = append(records, rec)
	}
	respondOK(nil)
}

func validateRecord(rec map[string]string) string {
	for _, f := range fields {
		if _, ok := rec[f]; !ok {
			return fmt.Sprintf("missing field: %s", f)
		}
	}
	for k := range rec {
		if !isExpectedField(k) {
			return fmt.Sprintf("unexpected field: %s", k)
		}
	}
	return ""
}

func isExpectedField(k string) bool {
	for _, f := range fields {
		if k == f {
			return true
		}
	}
	return false
}

type parsedFilter struct {
	fields     map[string]string
	pairKeys   []string
	pairValues []string
}

func parseFilter(payload string) (*parsedFilter, string) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		return nil, fmt.Sprintf("bad json: %v", err)
	}

	pf := &parsedFilter{fields: make(map[string]string)}

	for k, v := range raw {
		if k == "__pair__" {
			var pair [2][]string
			if err := json.Unmarshal(v, &pair); err != nil {
				return nil, fmt.Sprintf("bad __pair__: %v", err)
			}
			if len(pair[0]) != 2 {
				return nil, "__pair__ must have exactly 2 field names"
			}
			for _, fk := range pair[0] {
				if !isExpectedField(fk) {
					return nil, fmt.Sprintf("unexpected field in __pair__: %s", fk)
				}
			}
			pf.pairKeys = pair[0]
			pf.pairValues = pair[1]
			if len(pf.pairValues) > 2 {
				return nil, "__pair__ must have 0, 1, or 2 values"
			}
			continue
		}
		if !isExpectedField(k) {
			return nil, fmt.Sprintf("unexpected field: %s", k)
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return nil, fmt.Sprintf("bad value for field %q: %v", k, err)
		}
		pf.fields[k] = s
	}

	return pf, ""
}

func likeMatch(haystack, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(needle))
}

func matchRecord(rec map[string]string, pf *parsedFilter) bool {
	for k, v := range pf.fields {
		rv, ok := rec[k]
		if !ok {
			return false
		}
		if !likeMatch(rv, v) {
			return false
		}
	}

	if pf.pairKeys != nil && len(pf.pairValues) > 0 {
		f0 := rec[pf.pairKeys[0]]
		f1 := rec[pf.pairKeys[1]]

		if len(pf.pairValues) == 1 {
			// One value: match if either field contains it
			v := pf.pairValues[0]
			if !likeMatch(f0, v) && !likeMatch(f1, v) {
				return false
			}
		} else {
			// Two values: match either assignment
			v0 := pf.pairValues[0]
			v1 := pf.pairValues[1]
			fwd := likeMatch(f0, v0) && likeMatch(f1, v1)
			rev := likeMatch(f0, v1) && likeMatch(f1, v0)
			if !fwd && !rev {
				return false
			}
		}
	}

	return true
}

func cmdCount() {
	if !loaded {
		respondError("no file loaded")
		return
	}
	respondOK(map[string]interface{}{"count": len(records)})
}

func cmdClear() {
	if !loaded {
		respondError("no file loaded")
		return
	}
	records = nil
	deleteHint = 0
	respondOK(nil)
}

func cmdSort(field string) {
	if !loaded {
		respondError("no file loaded")
		return
	}

	// Strip quotes if present
	if len(field) >= 2 && field[0] == '"' && field[len(field)-1] == '"' {
		field = field[1 : len(field)-1]
	}

	// Verify field exists in schema
	found := false
	for _, f := range fields {
		if f == field {
			found = true
			break
		}
	}
	if !found {
		respondError(fmt.Sprintf("unknown field: %s", field))
		return
	}

	sortField = field
	deleteHint = 0

	sort.SliceStable(records, func(i, j int) bool {
		return strings.ToLower(records[i][sortField]) < strings.ToLower(records[j][sortField])
	})

	respondOK(nil)
}

func cmdDeleteOne(payload string) {
	if !loaded {
		respondError("no file loaded")
		return
	}

	pf, errMsg := parseFilter(payload)
	if errMsg != "" {
		respondError(errMsg)
		return
	}

	n := len(records)
	if n == 0 {
		respondOK(map[string]interface{}{"count": 0})
		return
	}

	// Search forward from hint, then wrap around
	if deleteHint >= n {
		deleteHint = 0
	}

	idx := -1
	for i := 0; i < n; i++ {
		j := (deleteHint + i) % n
		if matchRecord(records[j], pf) {
			idx = j
			break
		}
	}

	if idx < 0 {
		respondOK(map[string]interface{}{"count": 0})
		return
	}

	records = append(records[:idx], records[idx+1:]...)
	deleteHint = idx
	respondOK(map[string]interface{}{"count": 1})
}

func cmdSelect(payload string) {
	if !loaded {
		respondError("no file loaded")
		return
	}

	pf, errMsg := parseFilter(payload)
	if errMsg != "" {
		respondError(errMsg)
		return
	}

	var results []map[string]string
	for _, rec := range records {
		if matchRecord(rec, pf) {
			results = append(results, rec)
		}
	}

	respondOK(map[string]interface{}{"count": len(results)})
	for _, rec := range results {
		b, _ := json.Marshal(rec)
		fmt.Println(string(b))
	}
}

func cmdDelete(payload string) {
	if !loaded {
		respondError("no file loaded")
		return
	}

	pf, errMsg := parseFilter(payload)
	if errMsg != "" {
		respondError(errMsg)
		return
	}

	count := 0
	kept := make([]map[string]string, 0, len(records))
	for _, rec := range records {
		if matchRecord(rec, pf) {
			count++
		} else {
			kept = append(kept, rec)
		}
	}
	records = kept
	deleteHint = 0

	respondOK(map[string]interface{}{"count": count})
}