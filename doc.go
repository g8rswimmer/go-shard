// Package shard is a library for maintaining a sharded PostgreSQL database.
//
// It routes queries to the right shard from table metadata, fans out and
// merges results when more than one shard is needed, and applies migrations
// to every shard. See docs/REQUIREMENTS.md and docs/ARCHITECTURE.md.
package shard
