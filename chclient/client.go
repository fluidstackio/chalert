// Package chclient provides the ClickHouse connection pool and query execution layer.
//
// # Design Decisions
//
//   - Uses the official clickhouse-go v2 driver with native protocol (not HTTP).
//     The native protocol is more efficient for the kind of queries we run (aggregations
//     returning moderate result sets).
//
//   - Supports separate read and write connections. Alert evaluation queries should hit
//     a read replica to avoid competing with ingestion. State writes (alert_state,
//     alert_history) are low volume and go to the primary.
//
//   - Connection-level settings (max_execution_time, max_memory_usage) are configured
//     per-pool to prevent runaway alert queries from impacting the cluster.
//
// # Assumptions
//
//   - ClickHouse is accessible via native protocol (default port 9000/9440 for TLS).
//   - Read replicas (if used) are eventually consistent — this is fine for alert evaluation
//     since we already account for ingestion delay via eval_delay.
//   - Connection strings follow the clickhouse-go DSN format:
//     clickhouse://user:pass@host:9000/database?dial_timeout=5s&max_execution_time=30
package chclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Config holds ClickHouse connection configuration.
type Config struct {
	// DSN is the primary ClickHouse connection string used for writes and
	// default reads. Required.
	DSN string

	// ReadDSN is an optional separate connection string for alert evaluation
	// queries. If empty, DSN is used for reads too. Point this at a read
	// replica to isolate alert query load from ingestion.
	ReadDSN string

	// Username overrides any username in the DSN. If set, it is applied to
	// the parsed connection options after DSN parsing.
	Username string

	// Password overrides any password in the DSN. Only used when Username is set.
	Password string

	// TLS configures TLS for the ClickHouse connection. When any TLS field
	// is set, a crypto/tls.Config is built and applied to the connection.
	TLS TLSConfig

	// MaxQueryTime limits the maximum execution time for any single alert
	// evaluation query. Prevents runaway queries from consuming cluster
	// resources. Default: 30s.
	MaxQueryTime time.Duration

	// MaxIdleConns limits the number of idle connections in each pool.
	// Default: 5.
	MaxIdleConns int

	// MaxOpenConns limits the number of open connections in each pool.
	// Default: MaxIdleConns + 5.
	MaxOpenConns int

	// MaxRowsToRead limits rows ClickHouse may read per query (server-side
	// max_rows_to_read setting). Guards against full table scans from poorly
	// written alert expressions. 0 means unlimited.
	MaxRowsToRead int64

	// MaxThreads limits threads per query on the ClickHouse side (server-side
	// max_threads setting). 0 means use ClickHouse default.
	MaxThreads int
}

// TLSConfig holds TLS settings for the ClickHouse connection.
// Follows the same pattern as the OpenTelemetry Collector's ClickHouse exporter.
type TLSConfig struct {
	// Enabled forces TLS on the connection even if the DSN scheme is clickhouse://.
	Enabled bool

	// CAFile is the path to a PEM-encoded CA certificate file for verifying
	// the server's certificate.
	CAFile string

	// CertFile is the path to a PEM-encoded client certificate file for mTLS.
	CertFile string

	// KeyFile is the path to a PEM-encoded client private key file for mTLS.
	KeyFile string

	// ServerName overrides the server name used for certificate verification.
	ServerName string

	// InsecureSkipVerify disables server certificate verification. For testing only.
	InsecureSkipVerify bool
}

// hasTLS returns true if any TLS field is explicitly configured.
func (t TLSConfig) hasTLS() bool {
	return t.Enabled || t.CAFile != "" || t.CertFile != "" || t.KeyFile != "" || t.ServerName != "" || t.InsecureSkipVerify
}

// buildTLSConfig creates a *tls.Config from the TLS settings.
func (t TLSConfig) buildTLSConfig() (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: t.InsecureSkipVerify,
		ServerName:         t.ServerName,
	}

	if t.CAFile != "" {
		caPEM, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("CA file %s contains no valid certificates", t.CAFile)
		}
		tlsCfg.RootCAs = pool
	}

	if t.CertFile != "" || t.KeyFile != "" {
		if t.CertFile == "" || t.KeyFile == "" {
			return nil, fmt.Errorf("both tls.certFile and tls.keyFile must be set for mTLS")
		}
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// Client provides ClickHouse connectivity with separate read/write pools.
type Client struct {
	write driver.Conn
	read  driver.Conn

	maxQueryTime time.Duration
}

func New(cfg Config) (*Client, error) {
	if cfg.MaxQueryTime == 0 {
		cfg.MaxQueryTime = 30 * time.Second
	}
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = 5
	}
	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = cfg.MaxIdleConns + 5
	}

	// Build guard rail settings for the read connection.
	// These are applied at the ClickHouse connection level so every alert
	// evaluation query inherits them automatically.
	readSettings := make(clickhouse.Settings)
	if secs := int(cfg.MaxQueryTime.Seconds()); secs > 0 {
		readSettings["max_execution_time"] = secs
	}
	if cfg.MaxRowsToRead > 0 {
		readSettings["max_rows_to_read"] = cfg.MaxRowsToRead
	}
	if cfg.MaxThreads > 0 {
		readSettings["max_threads"] = cfg.MaxThreads
	}

	// Build TLS config once, shared across connections.
	var tlsCfg *tls.Config
	if cfg.TLS.hasTLS() {
		var err error
		tlsCfg, err = cfg.TLS.buildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
	}

	cOpts := connOptions{
		maxOpenConns: cfg.MaxOpenConns,
		username:     cfg.Username,
		password:     cfg.Password,
		tlsConfig:    tlsCfg,
	}

	writeConn, err := openConn(cfg.DSN, cOpts, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open write connection: %w", err)
	}

	readConn := writeConn
	if cfg.ReadDSN != "" {
		readConn, err = openConn(cfg.ReadDSN, cOpts, readSettings)
		if err != nil {
			_ = writeConn.Close()
			return nil, fmt.Errorf("failed to open read connection: %w", err)
		}
	} else if len(readSettings) > 0 {
		// Same DSN but with guard rail settings for reads.
		readConn, err = openConn(cfg.DSN, cOpts, readSettings)
		if err != nil {
			_ = writeConn.Close()
			return nil, fmt.Errorf("failed to open read connection: %w", err)
		}
	}

	return &Client{
		write:        writeConn,
		read:         readConn,
		maxQueryTime: cfg.MaxQueryTime,
	}, nil
}

type connOptions struct {
	maxOpenConns int
	username     string
	password     string
	tlsConfig    *tls.Config
}

func openConn(dsn string, opts connOptions, settings clickhouse.Settings) (driver.Conn, error) {
	chOpts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}
	chOpts.MaxOpenConns = opts.maxOpenConns

	// Override auth if explicit username was provided, following the OTel
	// collector pattern where discrete fields take precedence over the DSN.
	if opts.username != "" {
		chOpts.Auth.Username = opts.username
		chOpts.Auth.Password = opts.password
	}

	// Apply TLS if configured.
	if opts.tlsConfig != nil {
		chOpts.TLS = opts.tlsConfig
	}

	if len(settings) > 0 {
		if chOpts.Settings == nil {
			chOpts.Settings = make(clickhouse.Settings)
		}
		for k, v := range settings {
			chOpts.Settings[k] = v
		}
	}

	conn, err := clickhouse.Open(chOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping failed: %w", err)
	}
	return conn, nil
}

// ReadConn returns the read connection (may be same as write if no read replica configured).
func (c *Client) ReadConn() driver.Conn {
	return c.read
}

// WriteConn returns the write connection.
func (c *Client) WriteConn() driver.Conn {
	return c.write
}

// MaxQueryTime returns the configured max query execution time.
func (c *Client) MaxQueryTime() time.Duration {
	return c.maxQueryTime
}

// Close closes both connection pools.
func (c *Client) Close() error {
	var errs []error
	if err := c.write.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing write conn: %w", err))
	}
	if c.read != c.write {
		if err := c.read.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing read conn: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}
