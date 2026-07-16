package middleware

import (
	"net/http"
	"strings"
)

// APIKeyAuth returns a middleware that enforces API key authentication.
// It accepts the key via "Authorization: Bearer <key>" or "X-API-Key: <key>".
// If the configured key is empty, authentication is skipped (dev/local mode).
func APIKeyAuth(key string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Bypass if no key is configured
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}

			provided := extractAPIKey(r)
			if provided == "" {
				writeAuthError(w, "missing API key: provide Authorization: Bearer <key> or X-API-Key header")
				return
			}
			if provided != key {
				writeAuthError(w, "invalid API key")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// extractAPIKey tries "Authorization: Bearer <token>" first, then "X-API-Key".
func extractAPIKey(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
			return strings.TrimSpace(parts[1])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

func writeAuthError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("WWW-Authenticate", `Bearer realm="distributed-job-scheduler"`)
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte(`{"error":"` + msg + `"}`))
}