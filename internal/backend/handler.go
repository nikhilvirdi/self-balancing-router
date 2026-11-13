package backend

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
)

type response struct {
	BackendID string `json:"backend_id"`
	Method    string `json:"method"`
	Path      string `json:"path"`
}

func Handler(backendID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.Header().Set("Cache-Control", "no-store")
			switch r.Method {
			case http.MethodGet:
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "ok\n")
			case http.MethodHead:
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusOK)
			default:
				w.Header().Set("Allow", "GET, HEAD")
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusMethodNotAllowed)
				_, _ = io.WriteString(w, "Method Not Allowed\n")
			}
			return
		}

		payload, err := json.Marshal(response{
			BackendID: backendID,
			Method:    r.Method,
			Path:      r.URL.Path,
		})
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		payload = append(payload, '\n')
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(payload)
		}
	})
}

