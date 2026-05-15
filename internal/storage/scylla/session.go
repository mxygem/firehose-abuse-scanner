package scylla

import (
	"context"

	"github.com/gocql/gocql"
)

// cqlSession is the subset of gocql.Session that Store uses.
type cqlSession interface {
	QueryContext(ctx context.Context, stmt string, args ...interface{}) cqlQuery
	IterContext(ctx context.Context, stmt string, args ...interface{}) cqlIter
	Close()
}

// cqlQuery is the subset of gocql.Query that Store uses for single-row /
// fire-and-forget statements.
type cqlQuery interface {
	Exec() error
	Scan(dest ...interface{}) error
}

// cqlIter is the subset of gocql.Iter that Store uses for multi-row reads.
// Scan returns false when iteration is done; Close surfaces any error from
// the underlying iteration (driver decode errors, transport errors, etc).
type cqlIter interface {
	Scan(dest ...interface{}) bool
	Close() error
}

// realSession wraps *gocql.Session. WithContext is folded into QueryContext
// so call sites don't have to chain it every time.
type realSession struct {
	s *gocql.Session
}

func (r *realSession) QueryContext(ctx context.Context, stmt string, args ...interface{}) cqlQuery {
	return r.s.Query(stmt, args...).WithContext(ctx)
}

func (r *realSession) IterContext(ctx context.Context, stmt string, args ...interface{}) cqlIter {
	return r.s.Query(stmt, args...).WithContext(ctx).Iter()
}

func (r *realSession) Close() { r.s.Close() }
