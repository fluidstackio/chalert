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
	"errors"
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
	dropLabelsRaw       arrayString

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
	flag.Var(&dropLabelsRaw, "notifier.dropLabel", `Do not send an alert whose label Name equals value, in the form "Name=value". Can be specified multiple times; one match drops the alert. chalert still evaluates the alert and writes its alert_history rows.`)
	flag.Parse()

	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "chalert: %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	if *rulePaths == "" {
		return errors.New("'-rule' flag is required")
	}
	if *clickhouseDSN == "" && !*dryRun {
		return errors.New("'-clickhouse.dsn' flag is required")
	}

	rule.ResendDelay = *resendDelay

	externalLabels, err := parseExternalLabels(externalLabelsRaw)
	if err != nil {
		return fmt.Errorf("invalid -external.label: %w", err)
	}
	dropLabels, err := parseDropLabels(dropLabelsRaw)
	if err != nil {
		return fmt.Errorf("invalid -notifier.dropLabel: %w", err)
	}
	if err := checkDropLabels(dropLabels, externalLabels); err != nil {
		return err
	}

	paths := strings.Split(*rulePaths, ";")
	snap, err := config.ReadSnapshot(paths)
	if err != nil {
		return fmt.Errorf("failed to read rules: %w", err)
	}
	groups, err := snap.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse rules: %w", err)
	}
	slog.Info("parsed rule groups", "count", len(groups))

	if *dryRun {
		_, _ = fmt.Fprintf(os.Stdout, "Validation successful: %d groups, %d total rules\n",
			len(groups), countRules(groups))
		return nil
	}

	// Start HTTP server for health checks, metrics, and version info.
	httpSrv := web.New(*httpAddr, version)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server error", "error", err)
		}
	}()

	ch, err := connectClickHouse()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	store := statestore.New(ch.WriteConn(), *clickhouseDB)
	ctx := context.Background()
	if err := store.EnsureTables(ctx); err != nil {
		return fmt.Errorf("failed to create state tables: %w", err)
	}

	queryHash := normalizedQueryHash(ch)
	if err := config.NormalizeRuleIDs(groups, queryHash); err != nil {
		return fmt.Errorf("failed to normalize rule IDs: %w", err)
	}

	activeAlerts, err := store.LoadActive(ctx)
	if err != nil {
		slog.Warn("failed to load active alerts for restoration", "error", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	retirements := rule.NewRetirements(store)
	a := &app{
		ctx:       ctx,
		paths:     paths,
		queryHash: queryHash,
		qb:        datasource.NewQuerierBuilder(ch.ReadConn()),
		notify:    newNotifier(dropLabels),
		store:     store,
		// The evaluation log rides chalert's log stream (stderr, as slog's
		// default), which the log collector ships to ClickHouse.
		evalLog:     rule.NewEvaluationLog(os.Stderr),
		retirements: retirements,
		groupOpts: rule.GroupOptions{
			DefaultInterval: *evaluationInterval,
			ExternalLabels:  externalLabels,
			DefaultLimit:    *defaultLimit,
			Retirements:     retirements,
		},
		httpSrv: httpSrv,
		groups:  make(map[uint64]*rule.Group),
	}
	a.start(groups, activeAlerts)

	httpSrv.SetReady(true)
	slog.Info("chalert started",
		"version", version,
		"groups", len(a.groups),
		"http", *httpAddr,
		"configCheckInterval", configCheckInterval.String(),
		"clickhouse", redactDSN(*clickhouseDSN))

	// Periodic checks skip the reload while this fingerprint is unchanged; SIGHUP and /-/reload force it.
	// Taken from the startup snapshot so it describes exactly the content that was loaded.
	a.lastFP = snap.Fingerprint()
	metrics.ConfigLastReloadSuccessful.Set(1)

	a.serve(cancel)
	return nil
}

func connectClickHouse() (*chclient.Client, error) {
	// The password file takes precedence over the flag.
	password := *clickhousePass
	if *clickhousePassFile != "" {
		data, err := os.ReadFile(*clickhousePassFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read -clickhouse.passwordFile: %w", err)
		}
		password = strings.TrimRight(string(data), "\n\r")
	}

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
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	return ch, nil
}

// normalizedQueryHash hashes rule exprs with ClickHouse's own query
// normalization, so cosmetic expr edits don't change rule identity.
func normalizedQueryHash(ch *chclient.Client) func(string) (uint64, error) {
	return func(expr string) (uint64, error) {
		var hash uint64
		row := ch.ReadConn().QueryRow(context.Background(), "SELECT normalizedQueryHashKeepNames(?)", expr)
		if err := row.Scan(&hash); err != nil {
			return 0, err
		}
		return hash, nil
	}
}

func newNotifier(dropLabels map[string]string) rule.Notifier {
	if *notifierURLs == "" {
		slog.Warn("no notifier.url configured — alerts will be evaluated but not sent")
		if len(dropLabels) > 0 {
			slog.Warn("-notifier.dropLabel has no effect without -notifier.url")
		}
		return nil
	}
	return notifier.New(notifier.Config{
		URLs:        strings.Split(*notifierURLs, ","),
		ExternalURL: *externalURL,
		DropLabels:  dropLabels,
	})
}

// app owns the running rule groups between startup and shutdown.
type app struct {
	ctx         context.Context
	paths       []string
	queryHash   func(string) (uint64, error)
	qb          datasource.QuerierBuilder
	notify      rule.Notifier
	store       *statestore.Store
	evalLog     rule.EvaluationRecorder
	retirements *rule.Retirements
	groupOpts   rule.GroupOptions
	httpSrv     *web.Server

	groups map[uint64]*rule.Group
	wg     sync.WaitGroup
	lastFP uint64
}

func (a *app) start(cfgs []config.Group, activeAlerts []rule.AlertInstance) {
	for _, cfg := range cfgs {
		g := rule.NewGroup(cfg, a.qb, a.groupOpts)
		g.RestoreState(activeAlerts)
		a.groups[g.ID()] = g
	}
	// A rule edited or removed while chalert was down left its restored
	// instances to no rule: resolve them, as a reload does.
	a.retirements.Resolve(a.ctx, time.Now(), rule.Unclaimed(activeAlerts, a.groups))
	for _, g := range a.groups {
		a.wg.Go(func() {
			g.Start(a.ctx, a.notify, a.store, a.evalLog)
		})
	}
}

// serve handles reload triggers until a shutdown signal arrives.
func (a *app) serve(cancel context.CancelFunc) {
	// Buffered so requests during an in-flight reload coalesce instead of blocking.
	reloadRequests := make(chan struct{}, 1)
	a.httpSrv.SetReloadFunc(func() {
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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for {
		select {
		case <-configCheckCh:
			a.reload("interval", false)
		case <-reloadRequests:
			a.reload("http", true)
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				a.reload("sighup", true)
				continue
			}
			slog.Info("shutdown signal received", "signal", sig)
			a.shutdown(cancel)
			return
		}
	}
}

func (a *app) reload(trigger string, force bool) {
	snap, err := config.ReadSnapshot(a.paths)
	if err != nil {
		reloadFailed("failed to read rule files for reload", trigger, err)
		return
	}
	fp := snap.Fingerprint()
	if !force && fp == a.lastFP {
		return
	}
	slog.Info("reloading rules", "trigger", trigger)
	newGroups, err := snap.Parse()
	if err != nil {
		reloadFailed("failed to reload rules", trigger, err)
		return
	}
	if err := config.NormalizeRuleIDs(newGroups, a.queryHash); err != nil {
		reloadFailed("failed to normalize rule IDs on reload", trigger, err)
		return
	}
	reloadGroups(a.ctx, a.groups, newGroups, a.qb, a.notify, a.store, a.evalLog, &a.wg, a.groupOpts)
	a.lastFP = fp
	metrics.ConfigReloads.WithLabelValues("success").Inc()
	metrics.ConfigLastReloadSuccess.SetToCurrentTime()
	metrics.ConfigLastReloadSuccessful.Set(1)
	slog.Info("rules reloaded", "trigger", trigger, "groups", len(a.groups))
}

func reloadFailed(msg, trigger string, err error) {
	slog.Error(msg, "trigger", trigger, "error", err)
	metrics.ConfigReloads.WithLabelValues("error").Inc()
	metrics.ConfigLastReloadSuccessful.Set(0)
}

func (a *app) shutdown(cancel context.CancelFunc) {
	a.httpSrv.SetReady(false)
	cancel()
	for _, g := range a.groups {
		g.Close()
	}

	// Wait for groups with a timeout to prevent hanging during rolling restarts.
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(*shutdownTimeout):
		slog.Error("shutdown timeout exceeded, some goroutines did not exit")
	}

	persistAllState(context.Background(), a.groups, a.store, a.retirements)

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = a.httpSrv.Shutdown(shutCtx)
	shutCancel()

	slog.Info("chalert stopped")
}

func reloadGroups(
	ctx context.Context,
	existing map[uint64]*rule.Group,
	newCfgs []config.Group,
	qb datasource.QuerierBuilder,
	notify rule.Notifier,
	store rule.StateStore,
	evalLog rule.EvaluationRecorder,
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
			og.Retire(ctx)
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
			wg.Go(func() {
				ng.Start(ctx, notify, store, evalLog)
			})
		}
	}
}

func persistAllState(ctx context.Context, groups map[uint64]*rule.Group, store rule.StateStore, retirements *rule.Retirements) {
	var all []rule.AlertInstance
	for _, g := range groups {
		for _, r := range g.Rules {
			all = append(all, r.GetAlerts()...)
		}
	}
	// Retired instances whose resolved rows are unwritten are saved as
	// firing, so the restart resolves them (rule.Retirements).
	all = append(all, retirements.Unwritten()...)
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

// parseDropLabels parses repeatable -notifier.dropLabel=Name=value flags into
// a map. The value must not be empty: chalert drops a query column whose value
// is empty, so such a flag would match nothing a query returns.
func parseDropLabels(raw []string) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	labels := make(map[string]string, len(raw))
	for _, pair := range raw {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return nil, fmt.Errorf("invalid drop label %q: expected Name=value with a non-empty value", pair)
		}
		if prev, ok := labels[kv[0]]; ok {
			return nil, fmt.Errorf("drop label %q is given twice (%q and %q): one value per name", kv[0], prev, kv[1])
		}
		labels[kv[0]] = kv[1]
	}
	return labels, nil
}

// checkDropLabels refuses a drop label that an external label sets to the same
// value: every alert carries the external labels, so chalert would send none.
func checkDropLabels(drop, external map[string]string) error {
	for name, value := range drop {
		if external[name] == value {
			return fmt.Errorf("-notifier.dropLabel %s=%s matches -external.label %s=%s, so no alert would be sent", name, value, name, value)
		}
	}
	return nil
}
