// Package config handles parsing and validation of alert/recording rule definitions.
//
// # Design Decisions
//
// Rule definitions use the same YAML structure as vmalert for familiarity, with the
// key difference that `expr` contains ClickHouse SQL instead of PromQL.
//
// We keep the same Group/Rule hierarchy so the evaluation engine can be reused:
// a Group is a set of rules sharing an evaluation interval and concurrency setting,
// and each Rule is either an alerting rule or a recording rule.
//
// # Assumptions
//
//   - SQL expressions are validated syntactically at parse time (basic checks), but
//     full validation requires a ClickHouse connection (column type checking, table existence).
//   - Environment variable substitution uses %{ENV_VAR} syntax (same as vmalert).
//   - Rule identity is computed via FNV-64a hash of expr + name + labels, same as vmalert,
//     so rules can be matched across config reloads for state preservation.
package config

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Group contains a list of rules grouped with a shared evaluation interval.
type Group struct {
	Name     string            `yaml:"name"`
	Interval Duration          `yaml:"interval,omitempty"`
	Rules    []Rule            `yaml:"rules"`
	Labels   map[string]string `yaml:"labels,omitempty"`

	// Concurrency controls how many rules in this group evaluate in parallel.
	// Defaults to 1 (sequential).
	Concurrency int `yaml:"concurrency,omitempty"`

	// EvalDelay adjusts the query timestamp to compensate for ingestion lag.
	// For example, if ClickHouse data is typically 30s behind real time,
	// set eval_delay: 30s so queries look at data that has actually landed.
	EvalDelay *Duration `yaml:"eval_delay,omitempty"`

	// Limit caps the number of alert instances or recording results a single
	// rule can produce. 0 means no limit.
	Limit *int `yaml:"limit,omitempty"`

	// Connection optionally overrides the default ClickHouse connection for
	// this group. Useful when different groups query different CH clusters.
	Connection string `yaml:"connection,omitempty"`

	// Checksum is computed during parsing for change detection on reload.
	Checksum string `yaml:"-"`

	// File records which file this group was loaded from.
	File string `yaml:"-"`
}

// Rule describes either an alerting rule or a recording rule.
type Rule struct {
	// Alerting rule fields
	Alert       string            `yaml:"alert,omitempty"`
	Expr        string            `yaml:"expr"`
	For         Duration          `yaml:"for,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`

	// KeepFiringFor keeps the alert firing for this duration after the
	// expression stops matching. Useful for flapping alerts.
	KeepFiringFor Duration `yaml:"keep_firing_for,omitempty"`

	// Recording rule fields
	Record string `yaml:"record,omitempty"`

	// Debug enables verbose logging for this rule.
	Debug *bool `yaml:"debug,omitempty"`

	// ID is computed during parsing.
	ID uint64 `yaml:"-"`
}

// Name returns the rule name based on type.
func (r *Rule) Name() string {
	if r.Record != "" {
		return r.Record
	}
	return r.Alert
}

// IsAlerting returns true if this is an alerting rule.
func (r *Rule) IsAlerting() bool {
	return r.Alert != ""
}

// IsRecording returns true if this is a recording rule.
func (r *Rule) IsRecording() bool {
	return r.Record != ""
}

// Validate checks for configuration errors.
func (r *Rule) Validate() error {
	if r.Alert == "" && r.Record == "" {
		return fmt.Errorf("either 'alert' or 'record' must be set")
	}
	if r.Alert != "" && r.Record != "" {
		return fmt.Errorf("'alert' and 'record' are mutually exclusive")
	}
	if r.Expr == "" {
		return fmt.Errorf("'expr' cannot be empty")
	}
	if r.IsRecording() && len(r.Annotations) > 0 {
		return fmt.Errorf("recording rules cannot have annotations")
	}
	if r.IsRecording() && r.For.Duration() > 0 {
		return fmt.Errorf("recording rules cannot have 'for' duration")
	}

	// Basic SQL sanity checks. Full validation requires a CH connection.
	exprUpper := strings.ToUpper(strings.TrimSpace(r.Expr))
	if !strings.HasPrefix(exprUpper, "SELECT") && !strings.HasPrefix(exprUpper, "WITH") {
		return fmt.Errorf("'expr' must be a SELECT or WITH statement, got: %.40s", r.Expr)
	}

	return nil
}

// normalizeExpr collapses whitespace in an expression so that cosmetic
// reformatting (e.g. YAML block scalar differences) does not change the hash.
func normalizeExpr(expr string) string {
	return strings.Join(strings.Fields(expr), " ")
}

// HashRule computes a unique identity hash for a rule.
func HashRule(r Rule) uint64 {
	h := fnv.New64a()
	h.Write([]byte(normalizeExpr(r.Expr)))
	if r.Record != "" {
		h.Write([]byte("recording"))
		h.Write([]byte(r.Record))
	} else {
		h.Write([]byte("alerting"))
		h.Write([]byte(r.Alert))
	}
	kv := sortedMapEntries(r.Labels)
	for _, e := range kv {
		h.Write([]byte(e.key))
		h.Write([]byte(e.value))
		h.Write([]byte("\xff"))
	}
	return h.Sum64()
}

// NormalizeRuleIDs replaces parse-time rule IDs with IDs derived from
// ClickHouse's normalizedQueryHashKeepNames function for the expression
// component. This provides CH-native query normalization (whitespace,
// comments, literal placeholding) so that cosmetic edits to expressions
// don't change rule identity.
//
// The queryHash function should call:
//
//	SELECT normalizedQueryHashKeepNames(expr)
//
// and return the UInt64 result.
//
// NormalizeRuleIDs also re-validates uniqueness after normalization,
// since CH normalization may merge previously-distinct expressions.
func NormalizeRuleIDs(groups []Group, queryHash func(expr string) (uint64, error)) error {
	cache := make(map[string]uint64)
	for i := range groups {
		seen := make(map[uint64]string)
		for j := range groups[i].Rules {
			r := &groups[i].Rules[j]
			chHash, ok := cache[r.Expr]
			if !ok {
				var err error
				chHash, err = queryHash(r.Expr)
				if err != nil {
					return fmt.Errorf("rule %q in group %q: failed to normalize expr: %w",
						r.Name(), groups[i].Name, err)
				}
				cache[r.Expr] = chHash
			}
			r.ID = hashRuleWithExprHash(chHash, *r)
			if prev, dup := seen[r.ID]; dup {
				return fmt.Errorf("group %q: rules %q and %q have the same normalized identity",
					groups[i].Name, prev, r.Name())
			}
			seen[r.ID] = r.Name()
		}
	}
	return nil
}

// hashRuleWithExprHash computes a rule ID using a pre-computed expression
// hash (from ClickHouse) combined with the rule name and labels.
func hashRuleWithExprHash(exprHash uint64, r Rule) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], exprHash)
	h.Write(buf[:])
	if r.Record != "" {
		h.Write([]byte("recording"))
		h.Write([]byte(r.Record))
	} else {
		h.Write([]byte("alerting"))
		h.Write([]byte(r.Alert))
	}
	kv := sortedMapEntries(r.Labels)
	for _, e := range kv {
		h.Write([]byte(e.key))
		h.Write([]byte(e.value))
		h.Write([]byte("\xff"))
	}
	return h.Sum64()
}

// Validate checks the group for configuration errors.
func (g *Group) Validate() error {
	if g.Name == "" {
		return fmt.Errorf("group name must be set")
	}
	if g.Interval.Duration() < 0 {
		return fmt.Errorf("interval must be >= 0")
	}
	if g.Concurrency < 0 {
		return fmt.Errorf("concurrency must be >= 0")
	}
	if g.Limit != nil && *g.Limit < 0 {
		return fmt.Errorf("limit must be >= 0")
	}

	uniqueRules := make(map[uint64]struct{})
	for i := range g.Rules {
		r := &g.Rules[i]
		if err := r.Validate(); err != nil {
			return fmt.Errorf("invalid rule %q: %w", r.Name(), err)
		}
		if _, ok := uniqueRules[r.ID]; ok {
			return fmt.Errorf("duplicate rule %q in group", r.Name())
		}
		uniqueRules[r.ID] = struct{}{}
	}
	return nil
}

// Parse reads rule configuration from the given file paths (supports globs).
func Parse(pathPatterns []string) ([]Group, error) {
	snap, err := ReadSnapshot(pathPatterns)
	if err != nil {
		return nil, err
	}
	return snap.Parse()
}

func parse(files map[string][]byte) ([]Group, error) {
	var groups []Group
	for file, data := range files {
		data = envSubstitute(data)
		uniqueGroups := make(map[string]struct{})

		decoder := yaml.NewDecoder(bytes.NewReader(data))
		for {
			var doc struct {
				Groups []Group `yaml:"groups"`
			}
			if err := decoder.Decode(&doc); err != nil {
				if err == io.EOF {
					break
				}
				return nil, fmt.Errorf("failed to parse %q: %w", file, err)
			}
			for i := range doc.Groups {
				g := &doc.Groups[i]
				g.File = file

				// Compute rule IDs
				for j := range g.Rules {
					g.Rules[j].ID = HashRule(g.Rules[j])
				}

				// Compute group checksum
				b, _ := yaml.Marshal(g)
				h := fnv.New64a()
				h.Write(b)
				g.Checksum = fmt.Sprintf("%x", h.Sum(nil))

				if err := g.Validate(); err != nil {
					return nil, fmt.Errorf("invalid group %q in %q: %w", g.Name, file, err)
				}
				if _, ok := uniqueGroups[g.Name]; ok {
					return nil, fmt.Errorf("duplicate group %q in %q", g.Name, file)
				}
				uniqueGroups[g.Name] = struct{}{}
				groups = append(groups, *g)
			}
		}
	}
	return groups, nil
}

// Snapshot is a point-in-time read of rule files, so fingerprinting and
// parsing operate on the same content.
type Snapshot struct {
	files map[string][]byte
}

// ReadSnapshot reads all rule files matching the given path patterns (supports globs).
func ReadSnapshot(pathPatterns []string) (*Snapshot, error) {
	files, err := readFiles(pathPatterns)
	if err != nil {
		return nil, err
	}
	return &Snapshot{files: files}, nil
}

// Fingerprint hashes the snapshot's raw content, as a cheap reload pre-check.
func (s *Snapshot) Fingerprint() uint64 {
	paths := make([]string, 0, len(s.files))
	for path := range s.files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	h := fnv.New64a()
	for _, path := range paths {
		h.Write([]byte(path))
		h.Write([]byte{0})
		h.Write(s.files[path])
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// Parse parses the snapshot's files into rule groups.
func (s *Snapshot) Parse() ([]Group, error) {
	return parse(s.files)
}

func readFiles(patterns []string) (map[string][]byte, error) {
	files := make(map[string][]byte)
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid glob pattern %q: %w", pattern, err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no files match pattern %q", pattern)
		}
		for _, path := range matches {
			info, err := os.Stat(path)
			if err != nil {
				return nil, fmt.Errorf("failed to stat %q: %w", path, err)
			}
			if info.IsDir() {
				continue
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("failed to read %q: %w", path, err)
			}
			files[path] = data
		}
	}
	return files, nil
}

func envSubstitute(data []byte) []byte {
	s := string(data)
	// Replace %{ENV_VAR} with environment variable values
	for {
		start := strings.Index(s, "%{")
		if start < 0 {
			break
		}
		end := strings.Index(s[start:], "}")
		if end < 0 {
			break
		}
		end += start
		envVar := s[start+2 : end]
		s = s[:start] + os.Getenv(envVar) + s[end+1:]
	}
	return []byte(s)
}

// Duration wraps time.Duration for YAML unmarshaling.
type Duration struct {
	D time.Duration
}

func (d Duration) Duration() time.Duration { return d.D }

func (d Duration) MarshalYAML() (any, error) {
	return d.D.String(), nil
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("cannot parse duration %q: %w", s, err)
	}
	d.D = dur
	return nil
}

type mapEntry struct {
	key, value string
}

func sortedMapEntries(m map[string]string) []mapEntry {
	entries := make([]mapEntry, 0, len(m))
	for k, v := range m {
		entries = append(entries, mapEntry{k, v})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})
	return entries
}
