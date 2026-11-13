package backend

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxAdminRequestBodyBytes = 4096

type adminWorkloadResponse struct {
	ProcessingDelay          string `json:"processing_delay"`
	ProcessingJitter         string `json:"processing_jitter"`
	BaselineProcessingDelay  string `json:"baseline_processing_delay"`
	BaselineProcessingJitter string `json:"baseline_processing_jitter"`
}

type adminWorkloadUpdateRequest struct {
	ProcessingDelay  *string `json:"processing_delay"`
	ProcessingJitter *string `json:"processing_jitter"`
}

func AdminHandler(model *DelayModel, token string) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/v1/workload", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetWorkload(w, model)
		case http.MethodPut:
			handlePutWorkload(w, r, model)
		default:
			w.Header().Set("Allow", "GET, PUT")
			writeAdminError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})

	mux.HandleFunc("/admin/v1/workload/reset", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeAdminError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handleResetWorkload(w, model)
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")

		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			writeAdminError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		provided := strings.TrimPrefix(auth, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			writeAdminError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		if r.URL.Path != "/admin/v1/workload" && r.URL.Path != "/admin/v1/workload/reset" {
			writeAdminError(w, http.StatusNotFound, "not found")
			return
		}

		mux.ServeHTTP(w, r)
	})
}

func handleGetWorkload(w http.ResponseWriter, model *DelayModel) {
	cfg := model.Workload()
	writeWorkloadResponse(w, http.StatusOK, cfg)
}

func handlePutWorkload(w http.ResponseWriter, r *http.Request, model *DelayModel) {
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminRequestBodyBytes)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req adminWorkloadUpdateRequest
	if err := dec.Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeAdminError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeAdminError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeAdminError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeAdminError(w, http.StatusBadRequest, "trailing content after JSON")
		return
	}

	if req.ProcessingDelay == nil || req.ProcessingJitter == nil {
		writeAdminError(w, http.StatusBadRequest, "missing required fields: processing_delay and processing_jitter are required")
		return
	}

	delay, err := time.ParseDuration(*req.ProcessingDelay)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid processing_delay: expected valid Go duration string")
		return
	}

	jitter, err := time.ParseDuration(*req.ProcessingJitter)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid processing_jitter: expected valid Go duration string")
		return
	}

	if err := model.Update(delay, jitter); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeWorkloadResponse(w, http.StatusOK, model.Workload())
}

func handleResetWorkload(w http.ResponseWriter, model *DelayModel) {
	cfg := model.Reset()
	writeWorkloadResponse(w, http.StatusOK, cfg)
}

func writeWorkloadResponse(w http.ResponseWriter, status int, cfg WorkloadConfig) {
	resp := adminWorkloadResponse{
		ProcessingDelay:          cfg.ProcessingDelay.String(),
		ProcessingJitter:         cfg.ProcessingJitter.String(),
		BaselineProcessingDelay:  cfg.BaselineProcessingDelay.String(),
		BaselineProcessingJitter: cfg.BaselineProcessingJitter.String(),
	}
	payload, err := json.Marshal(resp)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	payload = append(payload, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func writeAdminError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
