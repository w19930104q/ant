// Package normalize provides URL normalization.
//
// https://en.wikipedia.org/wiki/URI_normalization
package normalize

import (
	"net/url"
	"path"
	"sort"
	"strings"
)

// RawURL normalizes the given raw URL.
//
//  - Uppercase percent-encoded triplets.
//  - Decode percent-encoded unreserved characters.
//  - Lowercase the scheme and hostname.
//  - Removes the default port (:80, :443).
//  - Removes dot segments.
//  - Converts an empty path to `/`.
//  - Sorts the query parameters.
//  - Removes `?` when query is empty.
//  - Remove the fragment.
//
// Percent-encoded triplets that encode reserved characters
// are kept intact (but uppercased), decoding them may change
// the resource the URL points to, e.g. `%2F` and `/` are not
// necessarily equivalent.
func RawURL(rawurl string) (string, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return "", err
	}
	return URL(u).String(), nil
}

// URL normalizes a parsed URL.
//
// Note that the URL is normalized in-place.
func URL(u *url.URL) *url.URL {
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = hostname(u)
	setpath(u, pathname(u))
	u.RawQuery = query(u.RawQuery)
	u.ForceQuery = false
	u.Fragment = ""
	return u
}

// Hostname normalizes the hostname.
func hostname(u *url.URL) string {
	var host = strings.ToLower(u.Host)

	if j := strings.LastIndexByte(host, ':'); j != -1 {
		switch port := host[j+1:]; {
		case u.Scheme == "http" && port == "80":
			return host[:j]
		case u.Scheme == "https" && port == "443":
			return host[:j]
		}
	}

	return host
}

// Pathname normalizes the pathname.
//
// The method returns the escaped form of the path.
func pathname(u *url.URL) string {
	switch p := pctDecode(u.EscapedPath()); p {
	case "", "/":
		return "/"
	default:
		if p = path.Clean(p); p[0] != '/' {
			p = "/" + p
		}
		return p
	}
}

// Setpath sets the escaped path p on u.
func setpath(u *url.URL, p string) {
	decoded, err := url.PathUnescape(p)
	if err != nil {
		return
	}

	u.Path = decoded
	u.RawPath = ""

	if def := (&url.URL{Path: decoded}).EscapedPath(); def != p {
		u.RawPath = p
	}
}

// Query normalizes and sorts the given query.
func query(q string) string {
	if q == "" {
		return ""
	}

	parts := strings.Split(q, "&")
	for j := range parts {
		parts[j] = pctDecode(parts[j])
	}
	sort.Strings(parts)

	return strings.Join(parts, "&")
}

// PctDecode normalizes percent-encoded triplets in s.
//
// All triplets are uppercased and triplets that encode
// unreserved characters (RFC 3986 §2.3) are decoded,
// any other triplet is kept intact.
func pctDecode(s string) string {
	if strings.IndexByte(s, '%') == -1 {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))

	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if hi, lo := unhex(s[i+1]), unhex(s[i+2]); hi >= 0 && lo >= 0 {
				if v := byte(hi<<4 | lo); isunreserved(v) {
					b.WriteByte(v)
				} else {
					b.WriteByte('%')
					b.WriteByte(hexdigits[hi])
					b.WriteByte(hexdigits[lo])
				}
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}

	return b.String()
}

const hexdigits = "0123456789ABCDEF"

// Unhex returns the value of the hex digit c, or -1.
func unhex(c byte) int {
	switch {
	case '0' <= c && c <= '9':
		return int(c - '0')
	case 'a' <= c && c <= 'f':
		return int(c-'a') + 10
	case 'A' <= c && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// Isunreserved returns true if c is an unreserved character.
//
// RFC 3986 §2.3.
func isunreserved(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z',
		'A' <= c && c <= 'Z',
		'0' <= c && c <= '9':
		return true
	}
	switch c {
	case '-', '.', '_', '~':
		return true
	}
	return false
}
