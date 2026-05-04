package scylla

import (
	"context"

	"github.com/gocql/gocql"
)

// cqlSession is the subset of gocql.Session that Store uses.
type cqlSession interface {
	QueryContext(ctx context.Context, stmt string, args ...interface{}) cqlQuery
	Close()
}

// cqlQuery is the subset of gocql.Query that Store uses.
type cqlQuery interface {
	Exec() error
	Scan(dest ...interface{}) error
}

// realSession wraps *gocql.Session. WithContext is folded into QueryContext
// so call sites don't have to chain it every time.
type realSession struct {
	s *gocql.Session
}

func (r *realSession) QueryContext(ctx context.Context, stmt string, args ...interface{}) cqlQuery {
	return r.s.Query(stmt, args...).WithContext(ctx)
}

func (r *realSession) Close() { r.s.Close() }
