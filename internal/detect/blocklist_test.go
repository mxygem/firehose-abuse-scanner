package detect

import (
	"context"
	"testing"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestBlocklistRule_Inspect(t *testing.T) {
	r := NewBlocklistRule(
		"test",
		[]string{"spam-link.xyz", "*.phishing.example.com", " EVIL.com "},
		SeverityHigh.String(),
	)

	cases := []struct {
		desc    string
		evt     models.FirehoseEvent
		wantHit bool
		wantDom string
	}{
		{
			desc:    "exact host match",
			evt:     postWithLinks("https://spam-link.xyz/free-money"),
			wantHit: true,
			wantDom: "spam-link.xyz",
		},
		{
			desc:    "subdomain match via wildcard entry",
			evt:     postWithLinks("https://login.phishing.example.com/abc"),
			wantHit: true,
			wantDom: "phishing.example.com",
		},
		{
			desc:    "case insensitive host and entry",
			evt:     postWithLinks("https://Evil.COM/path"),
			wantHit: true,
			wantDom: "evil.com",
		},
		{
			desc:    "clean link",
			evt:     postWithLinks("https://bsky.app", "https://news.ycombinator.com"),
			wantHit: false,
		},
		{
			desc:    "no links",
			evt:     models.FirehoseEvent{Kind: models.EventPost, Text: "hello"},
			wantHit: false,
		},
		{
			desc:    "malformed link ignored",
			evt:     postWithLinks("not a url"),
			wantHit: false,
		},
		{
			desc:    "sibling domain does not match wildcard",
			evt:     postWithLinks("https://example.com/login"),
			wantHit: false,
		},
		{
			desc:    "naive suffix does not match (foo-evil.com vs evil.com)",
			evt:     postWithLinks("https://foo-evil.com/x"),
			wantHit: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			hits := r.Inspect(context.Background(), tc.evt)
			if !tc.wantHit {
				assert.Empty(t, hits)
				return
			}
			assert.Len(t, hits, 1)
			assert.Equal(t, RuleLinkBlocklist, hits[0].RuleID)
			assert.Equal(t, SeverityHigh.String(), hits[0].Severity)
			assert.Contains(t, hits[0].Evidence["domains"], tc.wantDom)
		})
	}
}

func TestBlocklistRule_EmptyBlocklist(t *testing.T) {
	r := NewBlocklistRule("empty", nil, "")
	hits := r.Inspect(context.Background(), postWithLinks("https://spam-link.xyz/x"))
	assert.Empty(t, hits)
}

func postWithLinks(links ...string) models.FirehoseEvent {
	return models.FirehoseEvent{
		Kind:  models.EventPost,
		Text:  "post with links",
		Links: links,
	}
}
