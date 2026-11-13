package proxy

import (
	"io"
	"net/http"
)

func handler() http.Handler {
	const (
		livePath  = "/livez"
		readyPath = "/readyz"
	)
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+livePath, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET "+readyPath, routingUnavailable)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		switch r.URL.Path {
		case livePath, readyPath:
			mux.ServeHTTP(w, r)
		default:
			routingUnavailable(w, r)
		}
	})
}

func routingUnavailable(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "request routing is unavailable", http.StatusServiceUnavailable)
}
