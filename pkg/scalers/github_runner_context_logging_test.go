package scalers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
)

func TestGithubRunnerMetrics_CallerCancellationIsNotLoggedAsFailure(t *testing.T) {
	for _, mode := range []string{"between pages", "own transport", "unrelated transport"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := paginationTestScaler("https://github.invalid", true)
			s.metadata.RunnerScope = REPO
			s.metadata.Repos = []string{"repo"}
			s.metadata.NoDefaultLabels = true
			var records []map[string]any
			s.logger = funcr.NewJSON(func(message string) {
				var record map[string]any
				if err := json.Unmarshal([]byte(message), &record); err != nil {
					t.Errorf("decode diagnostic: %v", err)
				}
				records = append(records, record)
			}, funcr.Options{})
			requests := 0
			s.httpClient = &http.Client{Transport: githubSnapshotTestTransport(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/actions/runs") {
					return githubSnapshotWorkflowResponse(r), nil
				}
				requests++
				if mode != "unrelated transport" {
					cancel()
				}
				if mode != "between pages" {
					return nil, context.Canceled
				}
				body, _ := json.Marshal(paginationTestJobs(1, 31, "queued"))
				return githubSnapshotTestResponse(http.StatusOK, string(body)), nil
			})}
			metrics, active, err := s.GetMetricsAndActivity(ctx, "test-metric")
			if !errors.Is(err, context.Canceled) || len(metrics) != 0 || active || requests != 1 || len(s.previousJobs) != 0 {
				t.Fatalf("canceled scan published or continued: metrics %v, active %v, error %v, requests %d", metrics, active, err, requests)
			}
			if len(records) != 1 {
				t.Fatalf("want one terminal diagnostic, got %v", records)
			}
			_, info := records[0]["level"]
			if wantInfo := mode != "unrelated transport"; info != wantInfo {
				t.Fatalf("cancellation diagnostic level: info=%v, want %v; record %v", info, wantInfo, records[0])
			}
		})
	}
}

func TestGetWorkflowRunJobs_ContextExpiryPreservesRealFailures(t *testing.T) {
	for _, expiry := range []string{"cancellation", "deadline"} {
		for _, response := range []struct {
			name   string
			status int
			body   string
			cause  string
		}{
			{name: "invalid job", status: http.StatusOK, body: `{"total_count":1,"jobs":[{"id":0,"status":"queued"}]}`, cause: "returned an invalid job"},
			{name: "malformed JSON", status: http.StatusOK, body: `{"total_count":1,"jobs":`, cause: "decode workflow run 42 jobs page 1"},
			{name: "server failure", status: http.StatusBadGateway, body: "", cause: "status: 502"},
		} {
			t.Run(expiry+"/"+response.name, func(t *testing.T) {
				var ctx context.Context
				var cancel context.CancelFunc
				if expiry == "deadline" {
					ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
				} else {
					ctx, cancel = context.WithCancel(context.Background())
				}
				defer cancel()
				s := paginationTestScaler("https://github.invalid", true)
				s.httpClient = &http.Client{Transport: githubSnapshotTestTransport(func(r *http.Request) (*http.Response, error) {
					if expiry == "deadline" {
						<-r.Context().Done()
					} else {
						cancel()
					}
					return githubSnapshotTestResponse(response.status, response.body), nil
				})}
				jobs, err := s.getWorkflowRunJobs(ctx, 42, "repo")
				if err == nil || !strings.Contains(err.Error(), response.cause) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || jobs != nil || len(s.previousJobs) != 0 {
					t.Fatalf("context expiry hid the actual failure: jobs %v, error %v; want %q", jobs, err, response.cause)
				}
			})
		}
	}
}

func githubSnapshotWorkflowResponse(r *http.Request) *http.Response {
	runs := WorkflowRuns{}
	if r.URL.Query().Get("status") == "queued" {
		runs = WorkflowRuns{TotalCount: 1, WorkflowRuns: []WorkflowRun{{ID: 42, Status: "queued", Repository: Repo{Name: "repo"}}}}
	}
	body, _ := json.Marshal(runs)
	return githubSnapshotTestResponse(http.StatusOK, string(body))
}

func githubSnapshotTestResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
