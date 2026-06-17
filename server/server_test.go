package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/phalconyx/telconyx"
)

func newTestHandler(t *testing.T, apiKey, chatID string) http.Handler {
	t.Helper()
	c, err := telconyx.NewClient(telconyx.Config{Token: "test-token", ChatID: chatID})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return New(c, Config{APIKey: apiKey})
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestDelete_RequiresAPIKey(t *testing.T) {
	h := newTestHandler(t, "secret", "-100")
	// No X-API-Key header -> 401, regardless of body.
	if w := do(h, "POST", "/delete", `{"url":"telconyx://file/x"}`); w.Code != http.StatusUnauthorized {
		t.Errorf("missing api key: got %d, want 401 (body=%s)", w.Code, w.Body.String())
	}
}

func TestDelete_BadRequests(t *testing.T) {
	h := newTestHandler(t, "", "-100") // auth disabled
	cases := []struct {
		name, body string
		want       int
	}{
		{"invalid json", `{not json`, http.StatusBadRequest},
		{"missing url", `{}`, http.StatusBadRequest},
		{"blank url", `{"url":"   "}`, http.StatusBadRequest},
		{"not a telconyx url", `{"url":"https://example.com/x"}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if w := do(h, "POST", "/delete", tc.body); w.Code != tc.want {
				t.Errorf("got %d, want %d (body=%s)", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func TestDelete_NonNumericChatID(t *testing.T) {
	// A valid link, but the server is configured with a non-numeric chat id.
	// DeleteChunks fails before any Telegram call, so this is hermetic.
	h := newTestHandler(t, "", "@mygroup")
	link := (&telconyx.FileLink{FileID: "fid", MessageID: 5, ChatID: -100}).URL()
	w := do(h, "POST", "/delete", `{"url":"`+link+`"}`)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("got %d, want 502 (body=%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "numeric") {
		t.Errorf("expected numeric-ChatID error, got %s", w.Body.String())
	}
}
