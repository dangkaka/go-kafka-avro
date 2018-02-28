package kafka

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Error holds more detailed information about errors coming back from schema registry
type Error struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d - %s", e.ErrorCode, e.Message)
}

func newError(resp *http.Response) *Error {
	err := &Error{}
	parsingErr := json.NewDecoder(resp.Body).Decode(&err)
	if parsingErr != nil {
		return &Error{resp.StatusCode, "Unrecognized error found"}
	}
	return err
}
