package normalize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestURL(t *testing.T) {
	var cases = []struct {
		title  string
		input  string
		output string
	}{
		{
			"Uppercase percent-encoded triplets",
			"http://example.com/foo%2a",
			"http://example.com/foo%2A",
		},
		{
			"Uppercase percent-encoded triplets in the query",
			"http://example.com/?a=%2f",
			"http://example.com/?a=%2F",
		},
		{
			"Lowercase the scheme and hostname",
			"HTTP://User@Example.COM/Foo",
			"http://User@example.com/Foo",
		},
		{
			"Decode percent-encoded triplets",
			"http://example.com/%7Efoo",
			"http://example.com/~foo",
		},
		{
			"Decode percent-encoded triplets in the query",
			"http://example.com/?a=%7e&b=%41",
			"http://example.com/?a=~&b=A",
		},
		{
			"Keeps reserved characters percent-encoded",
			"http://example.com/foo%2fbar",
			"http://example.com/foo%2Fbar",
		},
		{
			"Keeps reserved characters percent-encoded in the query",
			"http://example.com/?a=%2f%3A",
			"http://example.com/?a=%2F%3A",
		},
		{
			"Removes dot segments",
			"http://example.com/foo/./bar/baz/../qux",
			"http://example.com/foo/bar/qux",
		},
		{
			"Removes percent-encoded dot segments",
			"http://example.com/a/%2e%2e/b",
			"http://example.com/b",
		},
		{
			"Keeps the leading slash",
			"http://example.com/foo/bar",
			"http://example.com/foo/bar",
		},
		{
			"Converts an empty path to `/`",
			"http://example.com",
			"http://example.com/",
		},
		{
			"Removing the default http port",
			"http://example.com:80/",
			"http://example.com/",
		},
		{
			"Removing the default https port",
			"https://example.com:443/",
			"https://example.com/",
		},
		{
			"Keeps custom ports.",
			"http://example.com:8080/",
			"http://example.com:8080/",
		},
		{
			"Removes `?` when query is empty",
			"http://example.com/?",
			"http://example.com/",
		},
		{
			"Sorts query parameters",
			"http://example.com/?a=1&c=3&b=2",
			"http://example.com/?a=1&b=2&c=3",
		},
		{
			"Remove the fragment",
			"http://example.com/#foo",
			"http://example.com/",
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			var assert = require.New(t)

			v, err := RawURL(c.input)

			assert.NoError(err)
			assert.Equal(c.output, v)
		})
	}
}

func TestSameResource(t *testing.T) {
	var groups = [][]string{
		{
			"http://example.com/a/b",
			"HTTP://EXAMPLE.COM/a/b",
			"http://example.com:80/a/b",
			"http://example.com/a/b?",
			"http://example.com/a/b#fragment",
			"http://example.com/a/./b",
			"http://example.com/a/c/../b",
			"http://example.com/%61/%62",
			"http://example.com/a/%2e/b",
		},
		{
			"https://example.com/",
			"HTTPS://EXAMPLE.COM:443/",
			"https://example.com",
		},
		{
			"http://example.com/?a=1&b=2",
			"http://example.com/?b=2&a=1",
			"http://example.com/?b=%32&a=%31",
			"http://example.com:80/?b=2&a=1#x",
		},
	}

	for _, group := range groups {
		var keys = make(map[string]int)

		for _, rawurl := range group {
			key, err := RawURL(rawurl)
			require.NoError(t, err)
			keys[key]++
		}

		require.Len(t, keys, 1, "group %v produced %v", group, keys)
	}
}

func TestDifferentResources(t *testing.T) {
	var urls = []string{
		"http://example.com/a",
		"https://example.com/a",      // different scheme
		"http://example.com:8080/a",  // different port
		"http://example.com/A",       // path is case-sensitive
		"http://example.com/a%2Fb",   // encoded slash is not a separator
		"http://example.com/a/b",     // different path
		"http://example.com/a?x=1",   // different query
		"http://example.com/a?x=2",   // different query
		"http://example.com/a?x=%2F", // reserved triplet is kept
		"http://example.com/a?x=/",   // different from %2F spelling
		"http://other.com/a",         // different host
	}

	var keys = make(map[string]string)

	for _, rawurl := range urls {
		key, err := RawURL(rawurl)
		require.NoError(t, err)

		if prev, ok := keys[key]; ok {
			t.Fatalf("urls %q and %q produced the same key %q", prev, rawurl, key)
		}
		keys[key] = rawurl
	}
}
