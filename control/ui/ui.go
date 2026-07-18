// Package ui embeds and serves the control-plane dashboard — a single
// self-contained HTML page (inline CSS/JS, no external assets) so the
// controller binary needs no build step or static file deployment.
package ui

import (
	"embed"
	"net/http"
)

//go:embed index.html
var files embed.FS

// Index serves the embedded single-page dashboard at the app root.
func Index() http.Handler {
	page, err := files.ReadFile("index.html")
	if err != nil {
		panic("ui: embedded index.html missing: " + err.Error())
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(page)
	})
}
