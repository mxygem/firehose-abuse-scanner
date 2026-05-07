package detect

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewKeywordRule(t *testing.T) {
	testCases := []struct {
		desc          string
		keywords      []string
		regexes       []string
		severity      RuleSeverity
		expected      *KeywordRule
		expectedError error
	}{
		{
			desc:     "empty inputs returns empty rule",
			keywords: []string{},
			regexes:  []string{},
			severity: SeverityLow,
			expected: &KeywordRule{
				name:     "empty inputs returns empty rule",
				patterns: []*regexp.Regexp{},
				severity: SeverityLow,
			},
		},
		{
			desc:     "keywords but no regexes returns rule with only keywords",
			keywords: []string{"abc"},
			regexes:  []string{},
			severity: SeverityLow,
			expected: &KeywordRule{
				name:     "keywords but no regexes returns rule with only keywords",
				patterns: []*regexp.Regexp{regexp.MustCompile("(?i)abc")},
				severity: SeverityLow,
			},
		},
		{
			desc:     "regexes but no keywords returns rule with only regexes",
			keywords: []string{},
			regexes:  []string{"(abc)"},
			severity: SeverityLow,
			expected: &KeywordRule{
				name:     "regexes but no keywords returns rule with only regexes",
				patterns: []*regexp.Regexp{regexp.MustCompile("(abc)")},
				severity: SeverityLow,
			},
		},
		{
			desc:     "single keyword and regex",
			keywords: []string{"buy now", "free crypto"},
			regexes:  []string{"(?i)earn\\s+money\\s+fast"},
			severity: SeverityLow,
			expected: &KeywordRule{
				name:     "single keyword and regex",
				patterns: []*regexp.Regexp{regexp.MustCompile("(?i)buy now"), regexp.MustCompile("(?i)free crypto"), regexp.MustCompile("(?i)earn\\s+money\\s+fast")},
				severity: SeverityLow,
			},
		},
		{
			desc:     "single keyword with severity",
			keywords: []string{"buy now"},
			regexes:  []string{},
			severity: SeverityMedium,
			expected: &KeywordRule{
				name:     "single keyword with severity",
				patterns: []*regexp.Regexp{regexp.MustCompile("(?i)buy now")},
				severity: SeverityMedium,
			},
		},
		{
			desc:     "multiple keywords and multiple regexes with severity",
			keywords: []string{"buy now", "free crypto", "spam"},
			regexes:  []string{"(?i)earn\\s+money\\s+fast", "(?i)free bitcoin"},
			severity: SeverityHigh,
			expected: &KeywordRule{
				name: "multiple keywords and multiple regexes with severity",
				patterns: []*regexp.Regexp{
					regexp.MustCompile("(?i)buy now"),
					regexp.MustCompile("(?i)free crypto"),
					regexp.MustCompile("(?i)spam"),
					regexp.MustCompile(`(?i)earn\s+money\s+fast`),
					regexp.MustCompile("(?i)free bitcoin"),
				},
				severity: SeverityHigh,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := NewKeywordRule(tc.desc, tc.keywords, tc.regexes, tc.severity.String())
			if tc.expectedError != nil {
				require.NotNil(t, err, "expected error, got nil")
				assert.EqualError(t, tc.expectedError, err.Error())
				assert.Nil(t, actual)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected.name, actual.name)
				assert.Equal(t, tc.expected.patterns, actual.patterns)
				assert.Equal(t, tc.expected.severity, actual.severity)
			}
		})
	}
}

func TestKeywordRule_MatchesAndMisses(t *testing.T) {
	r, err := NewKeywordRule(
		"test",
		[]string{"BUY NOW", "free crypto", "[SPAM]"},
		[]string{`(?i)earn\s+money\s+fast`},
		SeverityMedium.String(),
	)
	if err != nil {
		t.Fatalf("NewKeywordRule: %v", err)
	}

	cases := []struct {
		name string
		evt  models.FirehoseEvent
		want bool
	}{
		{"keyword case insensitive", post("buy now while supplies last"), true},
		{"keyword exact", post("Click here for free crypto!! t.co/fakespam"), true},
		{"keyword bracketed literal", post("[SPAM] Earn money fast, DM me"), true},
		{"regex with whitespace", post("you can earn   money   fast online"), true},
		{"clean post", post("Genuine human post about my cat"), false},
		{"empty text", post(""), false},
		{"non-post event ignored", models.FirehoseEvent{Kind: models.EventFollow, Text: "BUY NOW"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hits := r.Inspect(context.Background(), tc.evt)
			if got := len(hits) > 0; got != tc.want {
				t.Fatalf("hit=%v want=%v hits=%+v", got, tc.want, hits)
			}
			if !tc.want {
				return
			}
			h := hits[0]
			if h.RuleID != RuleSpamKeyword {
				t.Errorf("RuleID=%q want=%q", h.RuleID, RuleSpamKeyword)
			}
			if h.Severity != SeverityMedium.String() {
				t.Errorf("Severity=%q want=%q", h.Severity, SeverityMedium)
			}
			if h.Evidence["matches"] == "" {
				t.Error("expected non-empty matches evidence")
			}
		})
	}
}

func TestKeywordRule_MultipleMatchesJoined(t *testing.T) {
	r, err := NewKeywordRule("test", []string{"buy now", "free crypto"}, nil, "")
	if err != nil {
		t.Fatalf("NewKeywordRule: %v", err)
	}
	hits := r.Inspect(context.Background(), post("BUY NOW for free crypto"))
	if len(hits) != 1 {
		t.Fatalf("hits=%d want=1", len(hits))
	}
	parts := strings.Split(hits[0].Evidence["matches"], "|")
	if len(parts) != 2 {
		t.Fatalf("matches=%q want 2 parts", hits[0].Evidence["matches"])
	}
}

func TestKeywordRule_BadRegex(t *testing.T) {
	if _, err := NewKeywordRule("test", nil, []string{"("}, ""); err == nil {
		t.Fatal("expected compile error")
	}
}

func post(text string) models.FirehoseEvent {
	return models.FirehoseEvent{Kind: models.EventPost, Text: text}
}
