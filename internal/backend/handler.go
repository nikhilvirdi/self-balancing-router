package backend

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type response struct {
	BackendID string `json:"backend_id"`
	Method    string `json:"method"`
	Path      string `json:"path"`
}

type errorResponse struct {
	Error string `json:"error"`
}

var overloadPayload = []byte(`{"error":"backend overloaded"}` + "\n")

func Handler(backendID string, pool *Pool) http.Handler {
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

		if r.URL.Path == "/admin" || strings.HasPrefix(r.URL.Path, "/admin/") {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, "{\"error\":\"not found\"}\n")
			return
		}

		resp, err := pool.Submit(r.Context(), requestInfo{
			BackendID: backendID,
			Method:    r.Method,
			Path:      r.URL.Path,
		})
		if err != nil {
			if errors.Is(err, ErrOverloaded) || errors.Is(err, ErrShutdown) {
				writeOverloaded(w, r)
				return
			}
			if r.Context().Err() != nil {
				return
			}
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		payload, err := json.Marshal(resp)
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

func writeOverloaded(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.Itoa(len(overloadPayload)))
	w.WriteHeader(http.StatusServiceUnavailable)
	if r.Method != http.MethodHead {
		_, _ = w.Write(overloadPayload)
	}
}
