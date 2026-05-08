package detect

import (
	"context"
	"net/url"
	"strings"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

const RuleLinkBlocklist = "link.blocklist"

// BlocklistRule flags posts whose Links contain a host listed in the
// blocklist, matching either the exact host or any subdomain of it.
// Events with no links short-circuit before any URL parsing.
type BlocklistRule struct {
	name     string
	domains  map[string]struct{}
	severity RuleSeverity
}

// NewBlocklistRule normalizes domains to lowercase and strips an
// optional leading "*." or ".". Empty entries are skipped.
func NewBlocklistRule(name string, domains []string, severity string) *BlocklistRule {
	set := make(map[string]struct{}, len(domains))
	for _, d := range domains {
		d = strings.ToLower(strings.TrimSpace(d))
		d = strings.TrimPrefix(d, "*.")
		d = strings.TrimPrefix(d, ".")
		if d == "" {
			continue
		}
		set[d] = struct{}{}
	}
	return &BlocklistRule{
		name:     name,
		domains:  set,
		severity: toSeverity(severity),
	}
}

func (r *BlocklistRule) Inspect(_ context.Context, evt models.FirehoseEvent) []Hit {
	if len(evt.Links) == 0 || len(r.domains) == 0 {
		return nil
	}
	var matched []string
	for _, link := range evt.Links {
		host := hostOf(link)
		if host == "" {
			continue
		}
		if dom, ok := r.matchDomain(host); ok {
			matched = append(matched, dom)
		}
	}
	if len(matched) == 0 {
		return nil
	}
	return []Hit{{
		RuleID:   RuleLinkBlocklist,
		Severity: r.severity.String(),
		Reason:   "link host on blocklist",
		Evidence: map[string]string{"domains": strings.Join(matched, "|")},
	}}
}

// matchDomain returns the matching blocklist entry for host, treating
// exact matches and subdomains as equivalent.
func (r *BlocklistRule) matchDomain(host string) (string, bool) {
	if _, ok := r.domains[host]; ok {
		return host, true
	}
	for i := 0; i < len(host); i++ {
		if host[i] != '.' {
			continue
		}
		suffix := host[i+1:]
		if _, ok := r.domains[suffix]; ok {
			return suffix, true
		}
	}
	return "", false
}

// hostOf extracts the lowercased host from a URL. Returns "" if the
// link is malformed or has no host component.
func hostOf(link string) string {
	u, err := url.Parse(link)
	if err != nil {
		return ""
	}
	h := u.Hostname()
	if h == "" {
		return ""
	}
	return strings.ToLower(h)
}
