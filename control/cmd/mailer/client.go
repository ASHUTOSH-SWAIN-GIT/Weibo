package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/store"
)

// client is a thin REST client for the controller API. Every management
// subcommand goes through it, so they work unchanged against a controller
// on another host — nothing here touches local Docker or the store.
type client struct {
	base  string
	token string
	http  *http.Client
}

func newClient(base, token string) *client {
	return &client{
		base:  strings.TrimRight(base, "/"),
		token: strings.TrimSpace(token),
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

// jobListRow mirrors the API's list item: a Job plus its latest-run phase.
type jobListRow struct {
	store.Job
	Phase string `json:"phase"`
}

// jobDetail mirrors the GET /jobs/{id} response.
type jobDetail struct {
	Job         *store.Job          `json:"job"`
	LatestRun   *store.Run          `json:"latestRun,omitempty"`
	Transitions []*store.Transition `json:"transitions,omitempty"`
}

// do issues a request and returns the response body on a 2xx. On any other
// status it decodes the API's {"error": "..."} envelope into a Go error so
// the CLI can print a clean message instead of a raw status line.
func (c *client) do(ctx context.Context, method, path string, contentType string, body []byte) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("contacting controller at %s: %w", c.base, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if msg := apiError(data); msg != "" {
			return nil, fmt.Errorf("%s (%s)", msg, resp.Status)
		}
		return nil, fmt.Errorf("controller returned %s", resp.Status)
	}
	return data, nil
}

// apiError pulls the message out of an {"error": "..."} body, if present.
func apiError(data []byte) string {
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(data, &e) == nil {
		return e.Error
	}
	return ""
}

// submit posts a workflow/manifest document (+ optional env) to POST /jobs.
// It returns the created job and a non-fatal warning, which the controller
// sets (HTTP 202) when the spec was valid but the launch itself failed.
func (c *client) submit(ctx context.Context, doc []byte, env map[string]string) (job *store.Job, warning string, err error) {
	payload, err := json.Marshal(struct {
		Workflow string            `json:"workflow"`
		Env      map[string]string `json:"env,omitempty"`
	}{Workflow: string(doc), Env: env})
	if err != nil {
		return nil, "", err
	}
	data, err := c.do(ctx, http.MethodPost, "/jobs", "application/json", payload)
	if err != nil {
		return nil, "", err
	}
	// 201 returns the bare job; 202 wraps it as {"job":..,"warning":..}.
	var wrapped struct {
		Job     *store.Job `json:"job"`
		Warning string     `json:"warning"`
	}
	if json.Unmarshal(data, &wrapped) == nil && wrapped.Job != nil {
		return wrapped.Job, wrapped.Warning, nil
	}
	var j store.Job
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, "", err
	}
	return &j, "", nil
}

// listJobs returns all jobs with their latest-run phase.
func (c *client) listJobs(ctx context.Context) ([]jobListRow, error) {
	data, err := c.do(ctx, http.MethodGet, "/jobs", "", nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Jobs []jobListRow `json:"jobs"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

// getJob returns a job with its latest run and transition history.
func (c *client) getJob(ctx context.Context, id string) (*jobDetail, error) {
	data, err := c.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(id), "", nil)
	if err != nil {
		return nil, err
	}
	var d jobDetail
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// cancel requests a graceful stop of the job.
func (c *client) cancel(ctx context.Context, id string) error {
	_, err := c.do(ctx, http.MethodPost, "/jobs/"+url.PathEscape(id)+"/cancel", "", nil)
	return err
}

// restart resumes a job. An empty savepoint resumes from the last automatic
// checkpoint; otherwise it resumes from the named savepoint.
func (c *client) restart(ctx context.Context, id, savepoint string) (*store.Job, error) {
	var body []byte
	ct := ""
	if savepoint != "" {
		body, _ = json.Marshal(map[string]string{"savepoint": savepoint})
		ct = "application/json"
	}
	data, err := c.do(ctx, http.MethodPost, "/jobs/"+url.PathEscape(id)+"/restart", ct, body)
	if err != nil {
		return nil, err
	}
	var j store.Job
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// savepoint triggers a stop-with-savepoint under the given label.
func (c *client) savepoint(ctx context.Context, id, label string) error {
	q := "?label=" + url.QueryEscape(label)
	_, err := c.do(ctx, http.MethodPost, "/jobs/"+url.PathEscape(id)+"/savepoint"+q, "", nil)
	return err
}

// logs returns the last `tail` lines of the job's container logs as text.
func (c *client) logs(ctx context.Context, id string, tail int) (string, error) {
	q := "?tail=" + strconv.Itoa(tail)
	data, err := c.do(ctx, http.MethodGet, "/jobs/"+url.PathEscape(id)+"/logs"+q, "", nil)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
