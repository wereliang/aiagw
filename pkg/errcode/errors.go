package errcode

import "fmt"

// APIError represents a structured API error compatible with OpenAI error format.
type APIError struct {
	HTTPStatus int    `json:"-"`
	Type       string `json:"type"`
	Message    string `json:"message"`
	Code       string `json:"code"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

// ErrorResponse wraps an APIError for JSON serialization.
type ErrorResponse struct {
	Error *APIError `json:"error"`
}

func ErrAuthentication(msg string) *APIError {
	return &APIError{HTTPStatus: 401, Type: "authentication_error", Message: msg, Code: "invalid_api_key"}
}

func ErrPermission(msg string) *APIError {
	return &APIError{HTTPStatus: 403, Type: "permission_error", Message: msg, Code: "permission_denied"}
}

func ErrNotFound(msg string) *APIError {
	return &APIError{HTTPStatus: 404, Type: "not_found_error", Message: msg, Code: "not_found"}
}

func ErrInvalidRequest(msg string) *APIError {
	return &APIError{HTTPStatus: 400, Type: "invalid_request_error", Message: msg, Code: "invalid_request"}
}

func ErrRateLimit(msg string) *APIError {
	return &APIError{HTTPStatus: 429, Type: "rate_limit_error", Message: msg, Code: "rate_limit_exceeded"}
}

func ErrServiceUnavailable(msg string) *APIError {
	return &APIError{HTTPStatus: 503, Type: "service_unavailable", Message: msg, Code: "agent_unavailable"}
}

func ErrTimeout(msg string) *APIError {
	return &APIError{HTTPStatus: 504, Type: "timeout_error", Message: msg, Code: "request_timeout"}
}
