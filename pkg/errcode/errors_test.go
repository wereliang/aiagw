package errcode

import (
	"net/http"
	"testing"
)

func TestNewAPIError(t *testing.T) {
	err := ErrAuthentication("invalid api key")
	if err.HTTPStatus != http.StatusUnauthorized {
		t.Errorf("HTTPStatus = %d, want %d", err.HTTPStatus, http.StatusUnauthorized)
	}
	if err.Type != "authentication_error" {
		t.Errorf("Type = %q, want %q", err.Type, "authentication_error")
	}
	if err.Message != "invalid api key" {
		t.Errorf("Message = %q, want %q", err.Message, "invalid api key")
	}
	if err.Code != "invalid_api_key" {
		t.Errorf("Code = %q, want %q", err.Code, "invalid_api_key")
	}
}

func TestAllErrorTypes(t *testing.T) {
	tests := []struct {
		name       string
		fn         func(string) *APIError
		wantStatus int
		wantType   string
		wantCode   string
	}{
		{"authentication", ErrAuthentication, 401, "authentication_error", "invalid_api_key"},
		{"permission", ErrPermission, 403, "permission_error", "permission_denied"},
		{"not_found", ErrNotFound, 404, "not_found_error", "not_found"},
		{"invalid_request", ErrInvalidRequest, 400, "invalid_request_error", "invalid_request"},
		{"rate_limit", ErrRateLimit, 429, "rate_limit_error", "rate_limit_exceeded"},
		{"service_unavailable", ErrServiceUnavailable, 503, "service_unavailable", "agent_unavailable"},
		{"timeout", ErrTimeout, 504, "timeout_error", "request_timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn("test message")
			if err.HTTPStatus != tt.wantStatus {
				t.Errorf("HTTPStatus = %d, want %d", err.HTTPStatus, tt.wantStatus)
			}
			if err.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", err.Type, tt.wantType)
			}
			if err.Code != tt.wantCode {
				t.Errorf("Code = %q, want %q", err.Code, tt.wantCode)
			}
		})
	}
}

func TestAPIErrorImplementsError(t *testing.T) {
	err := ErrNotFound("model not found")
	var _ error = err
	want := "not_found_error: model not found"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}
