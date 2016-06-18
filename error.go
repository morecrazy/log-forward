package main

import (
	"encoding/json"
)

type LogForwardError struct {
	Code int
	Message string
}

func (err *LogForwardError) Error() string {
	error_str, _ := json.Marshal(err)
	return string(error_str)
}