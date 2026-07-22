package transport

import (
	"errors"
	"strings"
	"testing"
)

func TestRedactErr(t *testing.T) {
	// Fake, well-formed-looking token — NEVER put a real bot token in tests.
	const token = "123456789:AA-FAKE-TEST-TOKEN-do-not-use-000000"
	c := New(token, 0, "")

	// A realistic net/http timeout error embedding the token in the URL.
	err := errors.New(`Post "https://api.telegram.org/bot` + token +
		`/sendDocument": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`)

	red := c.redactErr(err)
	if strings.Contains(red.Error(), token) {
		t.Fatalf("token leaked through redactErr: %q", red.Error())
	}
	if !strings.Contains(red.Error(), "<redacted>") {
		t.Errorf("expected <redacted> marker, got %q", red.Error())
	}
	// The non-secret part of the message must be preserved for debugging.
	if !strings.Contains(red.Error(), "context deadline exceeded") {
		t.Errorf("redaction dropped useful context: %q", red.Error())
	}
}

func TestNew_BaseURL(t *testing.T) {
	c := New("tok", 0, "http://localhost:8081/")
	if c.apiBase != "http://localhost:8081/bottok" {
		t.Errorf("apiBase = %q", c.apiBase)
	}
	if got := c.FileURL("documents/x.bin"); got != "http://localhost:8081/file/bottok/documents/x.bin" {
		t.Errorf("FileURL = %q", got)
	}

	d := New("tok", 0, "")
	if d.apiBase != DefaultAPIBase+"/bottok" {
		t.Errorf("default apiBase = %q", d.apiBase)
	}
}

func TestRedactErr_NilAndUnrelated(t *testing.T) {
	c := New("sometoken", 0, "")
	if c.redactErr(nil) != nil {
		t.Error("nil error should pass through as nil")
	}
	other := errors.New("dial tcp: lookup api.telegram.org: no such host")
	if got := c.redactErr(other); got.Error() != other.Error() {
		t.Errorf("unrelated error was altered: %q", got.Error())
	}
}
