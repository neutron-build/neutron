package inspect

import (
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// RedactedValue replaces a value an inspection result must not disclose.
const RedactedValue = "[redacted]"

// sensitiveWord matches one snake_case component that names a secret.
var sensitiveWord = regexp.MustCompile(`^(password|passwd|pwd|passphrase|secret|secrets|token|tokens|apikey|credential|credentials|salt|otp|totp|ssn|cvv|cvc|pin)$`)

// sensitivePair matches two adjacent components that together name a secret
// (api_key, private_key, card_number, access_key, refresh_token, ...).
var sensitivePairs = map[string]bool{
	"api_key": true, "private_key": true, "secret_key": true, "access_key": true,
	"card_number": true, "account_number": true, "auth_token": true, "session_id": true,
	"session_key": true, "client_secret": true, "signing_key": true, "encryption_key": true,
}

// SensitiveName reports whether a column/field name looks like it holds a
// secret. Matching is on name components (snake_case or camelCase), so
// "author" or "tokenizer_version" are not secrets while "password_hash",
// "apiKey" and "refresh_token" are. Name-based: a secret stored under an
// innocuous name is not detected.
func SensitiveName(name string) bool {
	parts := nameParts(name)
	for i, p := range parts {
		if sensitiveWord.MatchString(p) {
			return true
		}
		if i+1 < len(parts) && sensitivePairs[p+"_"+parts[i+1]] {
			return true
		}
	}
	return false
}

func nameParts(name string) []string {
	var b strings.Builder
	prevLower := false
	for _, r := range name {
		switch {
		case r == '_' || r == '-' || r == '.' || r == ' ':
			b.WriteByte('_')
			prevLower = false
		case unicode.IsUpper(r):
			if prevLower {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
			prevLower = false
		default:
			b.WriteRune(r)
			prevLower = unicode.IsLower(r) || unicode.IsDigit(r)
		}
	}
	var out []string
	for _, p := range strings.Split(b.String(), "_") {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Redactor hides values stored under sensitive names. Disabled only by an
// explicit operator flag.
type Redactor struct{ Enabled bool }

// Value walks decoded JSON-shaped data and replaces values under sensitive
// keys. It returns the redacted data (the input is not modified) and the
// sorted, de-duplicated key names that were redacted.
func (r Redactor) Value(v any) (any, []string) {
	if !r.Enabled {
		return v, nil
	}
	seen := map[string]bool{}
	out := redactWalk(v, seen)
	return out, sortedKeys(seen)
}

func redactWalk(v any, seen map[string]bool) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, val := range x {
			if SensitiveName(k) && val != nil {
				m[k] = RedactedValue
				seen[k] = true
				continue
			}
			m[k] = redactWalk(val, seen)
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = redactWalk(e, seen)
		}
		return s
	case []map[string]any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = redactWalk(e, seen)
		}
		return s
	default:
		return v
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var urlInText = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>]+`)

// RedactText masks connection passwords wherever they appear in free text
// (error messages): every scheme://user:password@ URL and every
// password=... DSN pair.
func RedactText(s string) string {
	s = urlInText.ReplaceAllStringFunc(s, RedactURL)
	return dsnPassword.ReplaceAllString(s, "${1}xxxxx")
}

var dsnPassword = regexp.MustCompile(`(?i)(password\s*=\s*)('(?:[^'\\]|\\.)*'|\S+)`)

// RedactURL masks the password of a connection URL or key=value DSN so it
// can be logged or returned. Unparseable input is masked conservatively.
func RedactURL(s string) string {
	if strings.Contains(s, "://") {
		if u, err := url.Parse(s); err == nil {
			if u.User != nil {
				if _, has := u.User.Password(); has {
					u.User = url.UserPassword(u.User.Username(), "xxxxx")
				}
			}
			q := u.Query()
			if q.Has("password") {
				q.Set("password", "xxxxx")
				u.RawQuery = q.Encode()
			}
			return u.String()
		}
		// Unparseable URL: drop everything between the scheme and '@'.
		i := strings.Index(s, "://")
		if at := strings.LastIndex(s, "@"); at > i {
			return s[:i+3] + "xxxxx" + s[at:]
		}
		return s
	}
	return dsnPassword.ReplaceAllString(s, "${1}xxxxx")
}
