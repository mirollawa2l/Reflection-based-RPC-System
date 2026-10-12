package rpc

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestInvokeDirect(t *testing.T) {
	server, err := NewServer(&testService{})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		method    string
		args      []json.RawMessage
		want      any
		wantError string
	}{
		{
			name:   "Add",
			method: "Add",
			args: []json.RawMessage{
				json.RawMessage("2"),
				json.RawMessage("3"),
			},
			want: 5,
		},
		{
			name:   "No arguments",
			method: "NoArgs",
			want:   "ready",
		},
		{
			name:   "No result",
			method: "NoResult",
		},
		{
			name:   "String argument",
			method: "Echo",
			args: []json.RawMessage{
				json.RawMessage(`"hello"`),
			},
			want: "hello",
		},
		{
			name:   "Value and nil error",
			method: "Ping",
			args: []json.RawMessage{
				json.RawMessage("4"),
			},
			want: "Pong4",
		},
		{
			name:   "Only nil error",
			method: "OnlyError",
			args: []json.RawMessage{
				json.RawMessage("false"),
			},
		},
		{
			name:   "Returned error",
			method: "OnlyError",
			args: []json.RawMessage{
				json.RawMessage("true"),
			},
			wantError: "requested failure",
		},
		{
			name:      "Unknown method",
			method:    "Missing",
			wantError: "unknown method",
		},
		{
			name:   "Wrong argument count",
			method: "Add",
			args: []json.RawMessage{
				json.RawMessage("2"),
			},
			wantError: "expects 2 arguments",
		},
		{
			name:   "Wrong JSON type",
			method: "Add",
			args: []json.RawMessage{
				json.RawMessage(`"hello"`),
				json.RawMessage("3"),
			},
			wantError: "argument 0",
		},
		{
			name:      "Method panic",
			method:    "Panic",
			wantError: "panicked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := request{
				Method: tt.method,
				Args:   tt.args,
			}

			result, err := server.invoke(req)

			// Check expected errors.
			if tt.wantError != "" {
				if err == nil ||
					!strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf(
						"got error %v, want %q",
						err, tt.wantError,
					)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if result != tt.want {
				t.Fatalf(
					"got %v, want %v",
					result, tt.want,
				)
			}
		})
	}
}
