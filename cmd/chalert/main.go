// chalert evaluates alerting rules against ClickHouse and sends notifications.
//
// # Quick Start
//
//	chalert \
//	  -clickhouse.dsn="clickhouse://default:@localhost:9000/default" \
//	  -rule="rules/*.yaml" \
//	  -notifier.url="http://localhost:9093" \
//	  -evaluationInterval=1m
//
// # Architecture
//
// chalert follows vmalert's proven architecture:
//
//  1. Parse YAML rule files into Groups of alerting rules
//  2. Each Group runs an evaluation loop on its configured interval
//  3. Each evaluation executes the rule's SQL expression against ClickHouse
//  4. Query results are compared to in-memory alert state (Inactive → Pending → Firing)
//  5. Firing/resolved alerts are sent to Alertmanager
//  6. Alert state is persisted to ClickHouse tables for restart recovery
//
// Key differences from vmalert:
//   - Expressions are ClickHouse SQL instead of PromQL
//   - Alert state is stored in ClickHouse tables, not written as time series
//   - No prompb/protobuf dependency
//   - No recording rules (use ClickHouse materialized views directly)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/garbett1/chalert/chclient"
	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
	"github.com/garbett1/chalert/metrics"
	"github.com/garbett1/chalert/notifier"
	"github.com/garbett1/chalert/rule"
	"github.com/garbett1/chalert/statestore"
	"github.com/garbett1/chalert/web"
)

// arrayString implements flag.Value for repeatable string flags.
// Usage: -flag=value1 -flag=value2
type arrayString []string

func (a *arrayString) String() string { return strings.Join(*a, ",") }
func (a *arrayString) Set(v string) error {
	*a = append(*a, v)
	return nil
}

var (
	rulePaths           = flag.String("rule", "", "Path to rule files (supports globs). Multiple paths separated by ';'.")
	evaluationInterval  = flag.Duration("evaluationInterval", time.Minute, "Default evaluation interval for rule groups.")
	clickhouseDSN       = flag.String("clickhouse.dsn", "", "ClickHouse connection DSN (required).")
	clickhouseReadDSN   = flag.String("clickhouse.read-dsn", "", "Optional ClickHouse read replica DSN for query evaluation.")
	clickhouseDB        = flag.String("clickhouse.database", "default", "ClickHouse database for alert state tables.")
	clickhouseUser      = flag.String("clickhouse.username", "", "ClickHouse username. Overrides any user in the DSN.")
	clickhousePass      = flag.String("clickhouse.password", "", "ClickHouse password. Overrides any password in the DSN.")
	clickhousePassFile  = flag.String("clickhouse.passwordFile", "", "Path to file containing the ClickHouse password (e.g. a mounted k8s secret). Overrides -clickhouse.password.")
	maxQueryTime        = flag.Duration("clickhouse.maxQueryTime", 30*time.Second, "Max execution time per alert query.")
	notifierURLs        = flag.String("notifier.url", "", "Alertmanager URL(s), comma-separated for HA.")
	externalURL         = flag.String("external.url", "", "External URL for alert source links.")
	maxRowsToRead       = flag.Int64("clickhouse.maxRowsToRead", 0, "Max rows ClickHouse may read per alert query. 0 means unlimited.")
	maxThreads          = flag.Int("clickhouse.maxThreads", 0, "Max threads per alert query on ClickHouse. 0 means ClickHouse default.")
	maxIdleConns        = flag.Int("clickhouse.maxIdleConns", 0, "Max idle connections to ClickHouse. 0 means the clickhouse-go default of 5.")
	maxOpenConns        = flag.Int("clickhouse.maxOpenConns", 0, "Max open connections to ClickHouse. 0 means the clickhouse-go default of maxIdleConns+5.")
	defaultLimit        = flag.Int("rule.defaultLimit", 10000, "Default max alert instances per rule when group config omits 'limit'. 0 means unlimited.")
	resendDelay         = flag.Duration("rule.resendDelay", time.Minute, "Minimum interval between re-sending a firing alert to the notifier.")
	configCheckInterval = flag.Duration("rule.configCheckInterval", 0, "Interval for re-reading rule files and hot-reloading on content changes (e.g. updated ConfigMap mounts). 0 disables periodic checks; SIGHUP and POST /-/reload always work.")
	dryRun              = flag.Bool("dryRun", false, "Parse and validate rules without starting evaluation.")
	httpAddr            = flag.String("httpListenAddr", ":8880", "Address for the HTTP API and UI.")
	shutdownTimeout     = flag.Duration("shutdownTimeout", 30*time.Second, "Maximum time to wait for graceful shutdown.")
	externalLabelsRaw   arrayString

	// version is set by -ldflags at build time.
	version = "dev"

	// TLS flags for ClickHouse connections
	tlsEnabled            = flag.Bool("clickhouse.tls", false, "Enable TLS for ClickHouse connections.")
	tlsCAFile             = flag.String("clickhouse.tls.caFile", "", "Path to PEM-encoded CA certificate for verifying the ClickHouse server.")
	tlsCertFile           = flag.String("clickhouse.tls.certFile", "", "Path to PEM-encoded client certificate for mTLS.")
	tlsKeyFile            = flag.String("clickhouse.tls.keyFile", "", "Path to PEM-encoded client private key for mTLS.")
	tlsServerName         = flag.String("clickhouse.tls.serverName", "", "Override server name for TLS certificate verification.")
	tlsInsecureSkipVerify = flag.Bool("clickhouse.tls.insecureSkipVerify", false, "Skip server certificate verification (insecure, for testing only).")
)

func main() {
	flag.Var(&externalLabelsRaw, "external.label", `External label in the form "Name=value". Can be specified multiple times.`)
	flag.Parse()

	if *rulePaths == "" {
		fatalf("'-rule' flag is required")
	}
	if *clickhouseDSN == "" && !*dryRun {
		fatalf("'-clickhouse.dsn' flag is required")
	}

	// Apply resend delay
	rule.ResendDelay = *resendDelay

	// Parse external labels
	externalLabels, err := parseExternalLabels(externalLabelsRaw)
	if err != nil {
		fatalf("invalid -external.label: %s", err)
	}

	// Parse rules
	paths := strings.Split(*rulePaths, ";")
	snap, err := config.ReadSnapshot(paths)
	if err != nil {
		fatalf("failed to read rules: %s", err)
	}
	groups, err := snap.Parse()
	if err != nil {
		fatalf("failed to parse rules: %s", err)
	}
	slog.Info("parsed rule groups", "count", len(groups))

	if *dryRun {
		fmt.Printf("Validation successful: %d groups, %d total rules\n",
			len(groups), countRules(groups))
		return
	}

	// Start HTTP server for health checks, metrics, and version info.
	httpSrv := web.New(*httpAddr, version)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()

	// Resolve password: file takes precedence over flag.
	password := *clickhousePass
	if *clickhousePassFile != "" {
		data, err := os.ReadFile(*clickhousePassFile)
		if err != nil {
			fatalf("failed to read -clickhouse.passwordFile: %s", err)
		}
		password = strings.TrimRight(string(data), "\n\r")
	}

	// Connect to ClickHouse
	ch, err := chclient.New(chclient.Config{
		DSN:      *clickhouseDSN,
		ReadDSN:  *clickhouseReadDSN,
		Username: *clickhouseUser,
		Password: password,
		TLS: chclient.TLSConfig{
			Enabled:            *tlsEnabled,
			CAFile:             *tlsCAFile,
			CertFile:           *tlsCertFile,
			KeyFile:            *tlsKeyFile,
			ServerName:         *tlsServerName,
			InsecureSkipVerify: *tlsInsecureSkipVerify,
		},
		MaxQueryTime:  *maxQueryTime,
		MaxRowsToRead: *maxRowsToRead,
		MaxThreads:    *maxThreads,
		MaxOpenConns:  *maxOpenConns,
		MaxIdleConns:  *maxIdleConns,
	})
	if err != nil {
		fatalf("failed to connect to ClickHouse: %s", err)
	}
	defer func() { _ = ch.Close() }()

	// Ensure state tables exist
	store := statestore.New(ch.WriteConn(), *clickhouseDB)
	ctx := context.Background()
	if err := store.EnsureTables(ctx); err != nil {
		fatalf("failed to create state tables: %s", err)
	}

	// Normalize rule IDs using ClickHouse's normalizedQueryHashKeepNames.
	// This replaces the Go-side whitespace normalization with CH-native
	// query normalization so cosmetic expr edits don't change rule identity.
	queryHash := func(expr string) (uint64, error) {
		var hash uint64
		if err := ch.ReadConn().QueryRow(context.Background(),
			"SELECT normalizedQueryHashKeepNames(?)", expr).Scan(&hash); err != nil {
			return 0, err
		}
		return hash, nil
	}
	if err := config.NormalizeRuleIDs(groups, queryHash); err != nil {
		fatalf("failed to normalize rule IDs: %s", err)
	}

	// Load previously active alerts for state restoration
	activeAlerts, err := store.LoadActive(ctx)
	if err != nil {
		slog.Warn("failed to load active alerts for restoration", "error", err)
	}

	// Set up notifier
	var notify rule.Notifier
	if *notifierURLs != "" {
		urls := strings.Split(*notifierURLs, ",")
		notify = notifier.New(notifier.Config{
			URLs:        urls,
			ExternalURL: *externalURL,
		})
	} else {
		slog.Warn("no notifier.url configured — alerts will be evaluated but not sent")
	}

	// Build querier
	qb := datasource.NewQuerierBuilder(ch.ReadConn())

	// Start evaluation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	ruleGroups := make(map[uint64]*rule.Group)

	groupOpts := rule.GroupOptions{
		DefaultInterval: *evaluationInterval,
		ExternalLabels:  externalLabels,
		DefaultLimit:    *defaultLimit,
	}

	for _, cfg := range groups {
		g := rule.NewGroup(cfg, qb, groupOpts)
		g.RestoreState(activeAlerts)
		ruleGroups[g.ID()] = g

		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Start(ctx, notify, store)
		}()
	}

	httpSrv.SetReady(true)
	slog.Info("chalert started",
		"version", version,
		"groups", len(ruleGroups),
		"http", *httpAddr,
		"configCheckInterval", configCheckInterval.String(),
		"clickhouse", redactDSN(*clickhouseDSN))

	// Periodic checks skip the reload while this fingerprint is unchanged; SIGHUP and /-/reload force it.
	// Taken from the startup snapshot so it describes exactly the content that was loaded.
	lastFP := snap.Fingerprint()
	metrics.ConfigLastReloadSuccessful.Set(1)

	reload := func(trigger string, force bool) {
		snap, err := config.ReadSnapshot(paths)
		if err != nil {
			slog.Error("failed to read rule files for reload", "trigger", trigger, "error", err)
			metrics.ConfigReloads.WithLabelValues("error").Inc()
			metrics.ConfigLastReloadSuccessful.Set(0)
			return
		}
		fp := snap.Fingerprint()
		if !force && fp == lastFP {
			return
		}
		slog.Info("reloading rules", "trigger", trigger)
		newGroups, err := snap.Parse()
		if err != nil {
			slog.Error("failed to reload rules", "trigger", trigger, "error", err)
			metrics.ConfigReloads.WithLabelValues("error").Inc()
			metrics.ConfigLastReloadSuccessful.Set(0)
			return
		}
		if err := config.NormalizeRuleIDs(newGroups, queryHash); err != nil {
			slog.Error("failed to normalize rule IDs on reload", "trigger", trigger, "error", err)
			metrics.ConfigReloads.WithLabelValues("error").Inc()
			metrics.ConfigLastReloadSuccessful.Set(0)
			return
		}
		reloadGroups(ctx, ruleGroups, newGroups, qb, notify, store, &wg, groupOpts)
		lastFP = fp
		metrics.ConfigReloads.WithLabelValues("success").Inc()
		metrics.ConfigLastReloadSuccess.SetToCurrentTime()
		metrics.ConfigLastReloadSuccessful.Set(1)
		slog.Info("rules reloaded", "trigger", trigger, "groups", len(ruleGroups))
	}

	// Buffered so requests during an in-flight reload coalesce instead of blocking.
	reloadRequests := make(chan struct{}, 1)
	httpSrv.SetReloadFunc(func() {
		select {
		case reloadRequests <- struct{}{}:
		default:
		}
	})

	var configCheckCh <-chan time.Time
	if *configCheckInterval > 0 {
		ticker := time.NewTicker(*configCheckInterval)
		defer ticker.Stop()
		configCheckCh = ticker.C
	}

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for {
		select {
		case <-configCheckCh:
			reload("interval", false)

		case <-reloadRequests:
			reload("http", true)

		case sig := <-sigCh:
			switch sig {
			case syscall.SIGHUP:
				reload("sighup", true)

			case syscall.SIGINT, syscall.SIGTERM:
				slog.Info("shutdown signal received", "signal", sig)
				httpSrv.SetReady(false)
				cancel()
				for _, g := range ruleGroups {
					g.Close()
				}

				// Wait for groups with a timeout to prevent hanging during rolling restarts.
				done := make(chan struct{})
				go func() {
					wg.Wait()
					close(done)
				}()
				select {
				case <-done:
				case <-time.After(*shutdownTimeout):
					slog.Error("shutdown timeout exceeded, some goroutines did not exit")
				}

				// Final state persistence
				persistAllState(context.Background(), ruleGroups, store)

				// Shut down HTTP server
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = httpSrv.Shutdown(shutCtx)
				shutCancel()

				slog.Info("chalert stopped")
				return
			}
		}
	}
}

func reloadGroups(
	ctx context.Context,
	existing map[uint64]*rule.Group,
	newCfgs []config.Group,
	qb datasource.QuerierBuilder,
	notify rule.Notifier,
	store rule.StateStore,
	wg *sync.WaitGroup,
	opts rule.GroupOptions,
) {
	newRegistry := make(map[uint64]*rule.Group)
	for _, cfg := range newCfgs {
		ng := rule.NewGroup(cfg, qb, opts)
		newRegistry[ng.ID()] = ng
	}

	// Stop removed groups
	for id, og := range existing {
		if _, ok := newRegistry[id]; !ok {
			og.Close()
			delete(existing, id)
		}
	}

	// Update or start groups
	for id, ng := range newRegistry {
		if og, ok := existing[id]; ok {
			if og.Checksum() != ng.Checksum() {
				og.UpdateWith(ng)
			}
		} else {
			existing[id] = ng
			wg.Add(1)
			go func() {
				defer wg.Done()
				ng.Start(ctx, notify, store)
			}()
		}
	}
}

func persistAllState(ctx context.Context, groups map[uint64]*rule.Group, store rule.StateStore) {
	var all []rule.AlertInstance
	for _, g := range groups {
		for _, r := range g.Rules {
			all = append(all, r.GetAlerts()...)
		}
	}
	if len(all) > 0 {
		if err := store.Save(ctx, all); err != nil {
			slog.Error("failed to persist final alert state", "error", err)
		} else {
			slog.Info("persisted alert state", "count", len(all))
		}
	}
}

func countRules(groups []config.Group) int {
	n := 0
	for _, g := range groups {
		n += len(g.Rules)
	}
	return n
}

func redactDSN(dsn string) string {
	// Simple redaction: hide password if present
	if i := strings.Index(dsn, "@"); i > 0 {
		prefix := dsn[:strings.Index(dsn, "://")+3]
		return prefix + "***@" + dsn[i+1:]
	}
	return dsn
}

// parseExternalLabels parses repeatable -external.label=Name=value flags into a map.
func parseExternalLabels(raw []string) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	labels := make(map[string]string, len(raw))
	for _, pair := range raw {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 || kv[0] == "" {
			return nil, fmt.Errorf("invalid external label %q: expected Name=value", pair)
		}
		labels[kv[0]] = kv[1]
	}
	return labels, nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "chalert: "+format+"\n", args...)
	os.Exit(1)
}
