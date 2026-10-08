package bench

import (
	"net"
	"net/http"
	"time"
)

// benchHTTPClient reuses connections. The default Transport keeps two idle
// connections per host; the saturation tests run tens to hundreds of
// goroutines against one httptest server, so the surplus sockets land in
// TIME_WAIT and the process runs out of ephemeral ports (EADDRNOTAVAIL).
func benchHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 512,
			MaxConnsPerHost:     512,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}
