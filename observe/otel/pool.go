package otel

import (
	"context"
	"database/sql"

	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/g8rswimmer/go-shard/observe"
)

// PoolStatter is what RegisterPoolMetrics reads; *shard.DB is one.
type PoolStatter interface {
	PoolStats() map[observe.ShardID]sql.DBStats
}

// RegisterPoolMetrics reports each shard's connection pool, read when the
// metrics are collected (the shards are not contacted):
//
//	go_shard.pool.open            gauge:   connections open
//	go_shard.pool.in_use          gauge:   connections running a statement
//	go_shard.pool.idle            gauge:   idle connections
//	go_shard.pool.wait_count      counter: times a statement waited for a connection
//	go_shard.pool.wait_duration   counter: seconds spent waiting for one
//
// all with the attribute shard.id. Unregister the result when the DB closes.
// A nil mp uses the global meter provider.
func RegisterPoolMetrics(mp metric.MeterProvider, src PoolStatter) (metric.Registration, error) {
	if mp == nil {
		mp = otelapi.GetMeterProvider()
	}
	m := mp.Meter(instrumentation)
	open, err := m.Int64ObservableGauge("go_shard.pool.open", metric.WithUnit("{connection}"), metric.WithDescription("Connections open to a shard."))
	if err != nil {
		return nil, err
	}
	inUse, err := m.Int64ObservableGauge("go_shard.pool.in_use", metric.WithUnit("{connection}"), metric.WithDescription("Connections to a shard running a statement."))
	if err != nil {
		return nil, err
	}
	idle, err := m.Int64ObservableGauge("go_shard.pool.idle", metric.WithUnit("{connection}"), metric.WithDescription("Idle connections to a shard."))
	if err != nil {
		return nil, err
	}
	waits, err := m.Int64ObservableCounter("go_shard.pool.wait_count", metric.WithDescription("Times a statement waited for a connection to a shard."))
	if err != nil {
		return nil, err
	}
	waited, err := m.Float64ObservableCounter("go_shard.pool.wait_duration", metric.WithUnit("s"), metric.WithDescription("Time statements spent waiting for a connection to a shard."))
	if err != nil {
		return nil, err
	}
	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for id, s := range src.PoolStats() {
			at := metric.WithAttributes(attribute.String("shard.id", string(id)))
			o.ObserveInt64(open, int64(s.OpenConnections), at)
			o.ObserveInt64(inUse, int64(s.InUse), at)
			o.ObserveInt64(idle, int64(s.Idle), at)
			o.ObserveInt64(waits, s.WaitCount, at)
			o.ObserveFloat64(waited, s.WaitDuration.Seconds(), at)
		}
		return nil
	}, open, inUse, idle, waits, waited)
}
