package antcache

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUtils(t *testing.T) {}

func TestKeyof(t *testing.T) {
	t.Run("same resource, same key", func(t *testing.T) {
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
			},
			{
				"https://example.com/",
				"HTTPS://EXAMPLE.COM:443/",
			},
			{
				"http://example.com/?a=1&b=2",
				"http://example.com/?b=2&a=1",
				"http://example.com/?b=%32&a=%31",
			},
		}

		for _, group := range groups {
			var keys = make(map[uint64]int)

			for _, rawurl := range group {
				req, err := http.NewRequest("GET", rawurl, nil)
				require.NoError(t, err)
				keys[keyof(req)]++
			}

			require.Len(t, keys, 1, "group %v produced different keys", group)
		}
	})

	t.Run("different resources, different keys", func(t *testing.T) {
		var urls = []string{
			"http://example.com/a",
			"https://example.com/a",
			"http://example.com:8080/a",
			"http://example.com/A",
			"http://example.com/a%2Fb",
			"http://example.com/a/b",
			"http://example.com/a?x=1",
			"http://example.com/a?x=2",
			"http://other.com/a",
		}

		var keys = make(map[uint64]string)

		for _, rawurl := range urls {
			req, err := http.NewRequest("GET", rawurl, nil)
			require.NoError(t, err)

			key := keyof(req)
			if prev, ok := keys[key]; ok {
				t.Fatalf("urls %q and %q produced the same key", prev, rawurl)
			}
			keys[key] = rawurl
		}
	})

	t.Run("different methods, different keys", func(t *testing.T) {
		var assert = require.New(t)

		get, err := http.NewRequest("GET", "http://example.com/", nil)
		assert.NoError(err)

		head, err := http.NewRequest("HEAD", "http://example.com/", nil)
		assert.NoError(err)

		assert.NotEqual(keyof(get), keyof(head))
	})

	t.Run("does not mutate the request URL", func(t *testing.T) {
		var assert = require.New(t)

		req, err := http.NewRequest("GET", "http://EXAMPLE.com:80/a/./b?b=2&a=1#frag", nil)
		assert.NoError(err)

		keyof(req)

		assert.Equal("http://EXAMPLE.com:80/a/./b?b=2&a=1#frag", req.URL.String())
	})
}
