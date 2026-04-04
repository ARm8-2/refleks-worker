package supabase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Client wraps a shared Supabase Postgres connection pool.
type Client struct {
	pool *pgxpool.Pool
}

// NewClient creates a Supabase connection pool.
func NewClient(ctx context.Context, databaseURL string) (*Client, error) {
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return nil, fmt.Errorf("supabase database url is required")
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse supabase database url: %w", err)
	}

	cfg.MaxConnIdleTime = 30 * time.Second
	cfg.MaxConnLifetime = 45 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open supabase pool: %w", err)
	}

	return &Client{pool: pool}, nil
}

// Pool returns the underlying pgx pool.
func (c *Client) Pool() *pgxpool.Pool {
	if c == nil {
		return nil
	}
	return c.pool
}

// Ping checks database reachability.
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.pool == nil {
		return fmt.Errorf("supabase client is not configured")
	}
	return c.pool.Ping(ctx)
}

// Close releases resources.
func (c *Client) Close() {
	if c == nil || c.pool == nil {
		return
	}
	c.pool.Close()
}
