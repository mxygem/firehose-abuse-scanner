package detect

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

const (
	RuleSpamKeyword = "spam.keyword"
)

type RuleSeverity int

const (
	SeverityLow RuleSeverity = iota
	SeverityMedium
	SeverityHigh
)

func (r RuleSeverity) String() string {
	switch r {
	case SeverityLow:
		return "low"
	case SeverityMedium:
		return "medium"
	case SeverityHigh:
		return "high"
	default:
		return "low"
	}
}

func toSeverity(s string) RuleSeverity {
	switch s {
	case "low":
		return SeverityLow
	case "medium":
		return SeverityMedium
	case "high":
		return SeverityHigh
	}
	return SeverityLow
}

// KeywordRule contains a set of patterns and a severity level for a given rule.
// It is used to flag posts whose body matches one of the patterns.
type KeywordRule struct {
	name     string
	patterns []*regexp.Regexp
	severity RuleSeverity
}

// NewKeywordRule compiles each keyword as a case-insensitive literal
// (via QuoteMeta) and each regex string as-is. Empty patterns are
// skipped. An error from any compilation is returned immediately.
func NewKeywordRule(name string, keywords, regexes []string, severity string) (*KeywordRule, error) {
	pats := make([]*regexp.Regexp, 0, len(keywords)+len(regexes))
	for _, kw := range keywords {
		if kw == "" {
			continue
		}
		re, err := regexp.Compile("(?i)" + regexp.QuoteMeta(kw))
		if err != nil {
			return nil, fmt.Errorf("keyword %q: %w", kw, err)
		}
		pats = append(pats, re)
	}
	for _, r := range regexes {
		if r == "" {
			continue
		}

		re, err := regexp.Compile(r)
		if err != nil {
			return nil, fmt.Errorf("regex %q: %w", r, err)
		}
		pats = append(pats, re)
	}

	return &KeywordRule{
		name:     name,
		patterns: pats,
		severity: toSeverity(severity),
	}, nil
}

func (r *KeywordRule) Inspect(_ context.Context, evt models.FirehoseEvent) []Hit {
	if evt.Kind != models.EventPost || evt.Text == "" || len(r.patterns) == 0 {
		return nil
	}
	var matches []string
	for _, re := range r.patterns {
		if loc := re.FindStringIndex(evt.Text); loc != nil {
			matches = append(matches, evt.Text[loc[0]:loc[1]])
		}
	}
	if len(matches) == 0 {
		return nil
	}
	return []Hit{{
		RuleID:   RuleSpamKeyword,
		Severity: r.severity.String(),
		Reason:   "matched spam keyword(s)",
		Evidence: map[string]string{"matches": strings.Join(matches, "|")},
	}}
}
