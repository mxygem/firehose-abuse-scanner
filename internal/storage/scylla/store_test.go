package scylla

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func TestSplitStatements(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		expected []string
	}{
		{
			desc:     "empty input",
			input:    "",
			expected: []string{},
		},
		{
			desc:     "single statement no terminator",
			input:    "CREATE KEYSPACE foo",
			expected: []string{"CREATE KEYSPACE foo"},
		},
		{
			desc:     "single statement with terminator",
			input:    "CREATE KEYSPACE foo;",
			expected: []string{"CREATE KEYSPACE foo"},
		},
		{
			desc:     "two statements",
			input:    "USE foo; CREATE TABLE bar (id text PRIMARY KEY);",
			expected: []string{"USE foo", "CREATE TABLE bar (id text PRIMARY KEY)"},
		},
		{
			desc:     "line comments stripped",
			input:    "-- this creates a keyspace\nCREATE KEYSPACE foo;",
			expected: []string{"CREATE KEYSPACE foo"},
		},
		{
			desc:     "trailing whitespace and only-comment chunks dropped",
			input:    "-- only a comment\n\n   \n;",
			expected: []string{},
		},
		{
			desc: "multi-line statement preserved",
			input: `CREATE TABLE foo (
    id text PRIMARY KEY,
    name text
);`,
			expected: []string{"CREATE TABLE foo (\n    id text PRIMARY KEY,\n    name text\n)"},
		},
		{
			desc: "inline comment trimmed mid-line",
			input: `CREATE TABLE foo ( -- the foo table
    id text PRIMARY KEY
);`,
			expected: []string{"CREATE TABLE foo ( \n    id text PRIMARY KEY\n)"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.expected, splitStatements(tc.input))
		})
	}
}

func TestParseConsistency(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		expected gocql.Consistency
	}{
		{"empty defaults to ONE", "", gocql.One},
		{"unknown falls back to ONE", "BANANA", gocql.One},
		{"uppercase ONE", "ONE", gocql.One},
		{"lowercase one", "one", gocql.One},
		{"surrounding whitespace tolerated", "  QUORUM  ", gocql.Quorum},
		{"ANY", "ANY", gocql.Any},
		{"TWO", "TWO", gocql.Two},
		{"THREE", "THREE", gocql.Three},
		{"QUORUM", "QUORUM", gocql.Quorum},
		{"ALL", "ALL", gocql.All},
		{"LOCAL_QUORUM", "LOCAL_QUORUM", gocql.LocalQuorum},
		{"EACH_QUORUM", "EACH_QUORUM", gocql.EachQuorum},
		{"LOCAL_ONE", "LOCAL_ONE", gocql.LocalOne},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.expected, parseConsistency(tc.input))
		})
	}
}

func TestIsUseStatement(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		expected bool
	}{
		{"plain USE", "USE foo", true},
		{"lowercase use", "use foo", true},
		{"leading whitespace", "  USE foo", true},
		{"USE substring is not a USE statement", "USERS_TABLE", false},
		{"CREATE is not USE", "CREATE TABLE foo (id text PRIMARY KEY)", false},
		{"empty", "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.expected, isUseStatement(tc.input))
		})
	}
}
