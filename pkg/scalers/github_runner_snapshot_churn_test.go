package scalers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
)

func TestGetWorkflowRunJobs_RestartsChangingSnapshotWithoutCachedValidators(t *testing.T) {
	for _, enableEtags := range []bool{false, true} {
		t.Run(strconv.FormatBool(enableEtags), func(t *testing.T) {
			var pages []int
			attempt := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("per_page") != "30" {
					t.Errorf("unexpected page size: %s", r.URL.RawQuery)
				}
				page, _ := strconv.Atoi(r.URL.Query().Get("page"))
				pages = append(pages, page)
				if page == 1 {
					attempt++
				}
				if attempt > 1 && r.Header.Get("If-None-Match") != "" {
					t.Errorf("restart reused a cached validator for page %d", page)
				}
				if attempt == 1 && page == 1 {
					if enableEtags && r.Header.Get("If-None-Match") == `"previous-1"` {
						w.WriteHeader(http.StatusNotModified)
						return
					}
					_ = json.NewEncoder(w).Encode(paginationTestJobs(1, 95, "completed"))
					return
				}
				w.Header().Set("ETag", fmt.Sprintf(`"fresh-%d"`, page))
				_ = json.NewEncoder(w).Encode(paginationTestJobs(page, 96, "queued"))
			}))
			defer srv.Close()
			s := paginationTestScaler(srv.URL, enableEtags)
			var diagnostics []string
			s.logger = funcr.New(func(_, message string) { diagnostics = append(diagnostics, message) }, funcr.Options{})
			key := jobCacheKey{repo: "repo", runID: 42}
			if enableEtags {
				for page := 1; page <= 4; page++ {
					s.previousJobs[key] = append(s.previousJobs[key], githubJobPage{
						etag: fmt.Sprintf(`"previous-%d"`, page), jobs: paginationTestJobs(page, 95, "completed"),
					})
				}
			}
			jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
			if err != nil || len(jobs) != 96 {
				t.Fatalf("stable restart: got %d jobs, error %v; want 96 jobs", len(jobs), err)
			}
			if want := []int{1, 2, 1, 2, 3, 4}; !reflect.DeepEqual(pages, want) {
				t.Fatalf("pages = %v; want %v", pages, want)
			}
			if len(diagnostics) != 1 || !strings.Contains(diagnostics[0], "Restarting GitHub workflow job snapshot after count changed") || !strings.Contains(diagnostics[0], `"runID"=42`) || !strings.Contains(diagnostics[0], `"repo"="repo"`) || !strings.Contains(diagnostics[0], `"attempt"=2`) {
				t.Fatalf("bounded snapshot restart was not observable: %v", diagnostics)
			}
			for i, job := range jobs {
				if job.ID != i+1 || job.Status != "queued" {
					t.Fatalf("restart retained an abandoned page: job %d = %+v", i, job)
				}
			}
			if enableEtags {
				cached := s.previousJobs[key]
				if len(cached) != 4 || cached[0].etag != `"fresh-1"` || cached[3].jobs.TotalCount != 96 {
					t.Fatalf("restart did not atomically commit its complete pages: %+v", cached)
				}
			}
		})
	}
}

func TestGetWorkflowRunJobs_ChangingSnapshotExhaustsOneRestart(t *testing.T) {
	var pages []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		pages = append(pages, page)
		count := 95
		if page > 1 {
			count = 96
		}
		w.Header().Set("ETag", `"uncommitted"`)
		_ = json.NewEncoder(w).Encode(paginationTestJobs(page, count, "completed"))
	}))
	defer srv.Close()
	s := paginationTestScaler(srv.URL, true)
	key := jobCacheKey{repo: "repo", runID: 42}
	previous := []githubJobPage{{etag: `"previous"`, jobs: paginationTestJobs(1, 1, "queued")}}
	s.previousJobs[key] = previous
	jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
	if err == nil || jobs != nil || !strings.Contains(err.Error(), "GitHub workflow job snapshot changed during pagination:") || !strings.Contains(err.Error(), "2 attempts") {
		t.Fatalf("changing snapshot must fail distinctly after its bounded restart: jobs %v, error %v", jobs, err)
	}
	if want := []int{1, 2, 1, 2}; !reflect.DeepEqual(pages, want) {
		t.Fatalf("pages = %v; want %v", pages, want)
	}
	if !reflect.DeepEqual(s.previousJobs[key], previous) {
		t.Fatal("abandoned snapshots replaced the last complete cache")
	}
}

func TestGetWorkflowRunJobs_RestartsShrinkingSnapshot(t *testing.T) {
	attempt := 0
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page == 1 {
			attempt++
		}
		count := 31
		if attempt == 1 && page == 1 {
			count = 95
		}
		_ = json.NewEncoder(w).Encode(paginationTestJobs(page, count, "queued"))
	}))
	defer srv.Close()
	s := paginationTestScaler(srv.URL, false)
	jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
	if err != nil || len(jobs) != 31 || requests != 4 {
		t.Fatalf("stable smaller snapshot: got %d jobs, error %v, requests %d; want 31 jobs in four requests", len(jobs), err, requests)
	}
}

func TestGetWorkflowRunJobs_MalformedChangingPageDoesNotRestart(t *testing.T) {
	invalidID := paginationTestJobs(2, 96, "queued")
	invalidID.Jobs[0].ID = 0
	invalidStatus := paginationTestJobs(2, 96, "queued")
	invalidStatus.Jobs[0].Status = " "
	duplicate := paginationTestJobs(2, 96, "queued")
	duplicate.Jobs[0].ID = 1
	truncated := paginationTestJobs(2, 96, "queued")
	truncated.Jobs = truncated.Jobs[:29]
	tests := []struct {
		name string
		page Jobs
	}{
		{name: "negative total", page: Jobs{TotalCount: -1, Jobs: []Job{}}},
		{name: "excessive total", page: Jobs{TotalCount: 30001, Jobs: []Job{}}},
		{name: "invalid id", page: invalidID},
		{name: "invalid status", page: invalidStatus},
		{name: "duplicate job", page: duplicate},
		{name: "truncated page", page: truncated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				page := paginationTestJobs(1, 95, "queued")
				if r.URL.Query().Get("page") == "2" {
					page = tt.page
				}
				_ = json.NewEncoder(w).Encode(page)
			}))
			defer srv.Close()
			s := paginationTestScaler(srv.URL, true)
			jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
			if err == nil || jobs != nil || requests != 2 || len(s.previousJobs) != 0 || strings.Contains(err.Error(), "GitHub workflow job snapshot changed during pagination:") {
				t.Fatalf("malformed page must remain a terminal validation error: jobs %v, error %v, requests %d", jobs, err, requests)
			}
		})
	}
}

func TestGetWorkflowRunJobs_RestartSharesOriginalDeadline(t *testing.T) {
	var deadlines []time.Time
	var pages []string
	s := paginationTestScaler("https://github.invalid", false)
	s.httpClient = &http.Client{Transport: githubSnapshotTestTransport(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok {
			t.Error("snapshot request has no deadline")
		}
		deadlines = append(deadlines, deadline)
		pages = append(pages, r.URL.Query().Get("page"))
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		count := 95
		if len(pages) == 2 || len(pages) >= 3 {
			count = 96
		}
		body, _ := json.Marshal(paginationTestJobs(page, count, "queued"))
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}
	started := time.Now()
	jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
	if err != nil || len(jobs) != 96 || len(deadlines) != 6 {
		t.Fatalf("bounded restart: got %d jobs, error %v, requests %d", len(jobs), err, len(deadlines))
	}
	for _, deadline := range deadlines {
		if deadline.IsZero() || !deadline.Equal(deadlines[0]) || deadline.Before(started.Add(29*time.Second)) || deadline.After(started.Add(31*time.Second)) {
			t.Fatalf("requests did not share one 30-second snapshot deadline: %v", deadlines)
		}
	}
}

func TestGetWorkflowRunJobs_CancellationPreventsSnapshotRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := 0
	s := paginationTestScaler("https://github.invalid", true)
	var diagnostics []string
	s.logger = funcr.New(func(_, message string) { diagnostics = append(diagnostics, message) }, funcr.Options{})
	s.httpClient = &http.Client{Transport: githubSnapshotTestTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		count := 95
		if page == 2 {
			count = 96
			cancel()
		}
		body, _ := json.Marshal(paginationTestJobs(page, count, "queued"))
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}
	jobs, err := s.getWorkflowRunJobs(ctx, 42, "repo")
	if !errors.Is(err, context.Canceled) || jobs != nil || requests != 2 || len(s.previousJobs) != 0 {
		t.Fatalf("canceled snapshot restarted or committed: jobs %v, error %v, requests %d", jobs, err, requests)
	}
	if len(diagnostics) != 0 {
		t.Fatalf("already canceled snapshot announced a restart: %v", diagnostics)
	}
}

func TestGetWorkflowRunJobs_PreservesEarlierParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()
	s := paginationTestScaler("https://github.invalid", false)
	s.httpClient = &http.Client{Transport: githubSnapshotTestTransport(func(r *http.Request) (*http.Response, error) {
		if deadline, ok := r.Context().Deadline(); !ok || !deadline.Equal(wantDeadline) {
			t.Errorf("snapshot replaced the earlier parent deadline: got %v, want %v", deadline, wantDeadline)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"total_count":0,"jobs":[]}`))}, nil
	})}
	jobs, err := s.getWorkflowRunJobs(ctx, 42, "repo")
	if err != nil || len(jobs) != 0 {
		t.Fatalf("empty complete snapshot: jobs %v, error %v", jobs, err)
	}
}

func TestGetWorkflowQueueLength_ChangingSnapshotDoesNotUseRateLimitedCache(t *testing.T) {
	churn := false
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/actions/runs") {
			if r.URL.Query().Get("status") == "queued" {
				_ = json.NewEncoder(w).Encode(WorkflowRuns{TotalCount: 1, WorkflowRuns: []WorkflowRun{{ID: 42, Status: "queued", Repository: Repo{Name: "repo"}}}})
			} else {
				_ = json.NewEncoder(w).Encode(WorkflowRuns{})
			}
			return
		}
		requests++
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		count := 95
		if churn && page > 1 {
			count = 96
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
		}
		_ = json.NewEncoder(w).Encode(paginationTestJobs(page, count, "queued"))
	}))
	defer srv.Close()
	s := paginationTestScaler(srv.URL, true)
	s.metadata.RunnerScope = REPO
	s.metadata.Repos = []string{"repo"}
	s.metadata.NoDefaultLabels = true
	queue, err := s.GetWorkflowQueueLength(context.Background())
	if err != nil || queue != 95 {
		t.Fatalf("initial complete queue: got %d, error %v", queue, err)
	}
	previousTime := s.previousQueueLengthTime
	previousPages := s.previousJobs[jobCacheKey{repo: "repo", runID: 42}]
	churn = true
	requests = 0
	queue, err = s.GetWorkflowQueueLength(context.Background())
	if err == nil || queue != -1 || requests != 4 {
		t.Fatalf("changing snapshot was replaced by cached demand: queue %d, error %v, requests %d", queue, err, requests)
	}
	if s.previousQueueLength != 95 || s.previousQueueLengthTime != previousTime || !reflect.DeepEqual(previousPages, s.previousJobs[jobCacheKey{repo: "repo", runID: 42}]) {
		t.Fatal("failed changing snapshot advanced complete queue or page cache")
	}
}

func TestGetWorkflowRunJobs_TransportDeadlinePreservesRequestedURL(t *testing.T) {
	for _, waitForBody := range []bool{false, true} {
		t.Run(strconv.FormatBool(waitForBody), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if waitForBody {
					// nosemgrep: no-direct-write-to-responsewriter
					_, _ = w.Write([]byte(`{"total_count":1,"jobs":[`))
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer srv.Close()
			s := paginationTestScaler(srv.URL, true)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			jobs, err := s.getWorkflowRunJobs(ctx, 42, "repo")
			var requestErr *url.Error
			if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &requestErr) || jobs != nil || len(s.previousJobs) != 0 {
				t.Fatalf("transport deadline lost its cause or URL: jobs %v, error %v", jobs, err)
			}
			if requestErr.Op != "Get" || requestErr.URL != srv.URL+"/repos/owner/repo/actions/runs/42/jobs?per_page=30&page=1" {
				t.Fatalf("transport error does not identify the actual request: %+v", requestErr)
			}
		})
	}
}

func TestGetWorkflowRunJobs_LateSuccessDeadlineDoesNotInventRequestFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	s := paginationTestScaler("https://github.invalid", true)
	s.httpClient = &http.Client{Transport: githubSnapshotTestTransport(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"total_count":1,"jobs":[{"id":1,"status":"queued"}]}`))}, nil
	})}
	jobs, err := s.getWorkflowRunJobs(ctx, 42, "repo")
	var requestErr *url.Error
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &requestErr) || jobs != nil || len(s.previousJobs) != 0 {
		t.Fatalf("late successful response published or invented a request error: jobs %v, error %v", jobs, err)
	}
	if err.Error() != "GitHub workflow job snapshot deadline exceeded: run 42: context deadline exceeded" {
		t.Fatalf("late snapshot expiry lost its specific dependency cause: %v", err)
	}
}

func TestGetWorkflowRunJobs_DeadlineBeforeNextPageDoesNotInventRequestFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	requests := 0
	s := paginationTestScaler("https://github.invalid", true)
	s.httpClient = &http.Client{Transport: githubSnapshotTestTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		<-r.Context().Done()
		body, _ := json.Marshal(paginationTestJobs(1, 31, "queued"))
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}
	jobs, err := s.getWorkflowRunJobs(ctx, 42, "repo")
	var requestErr *url.Error
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &requestErr) || jobs != nil || requests != 1 || len(s.previousJobs) != 0 {
		t.Fatalf("expired pagination issued another request or invented its failure: jobs %v, error %v, requests %d", jobs, err, requests)
	}
	if err.Error() != "GitHub workflow job snapshot deadline exceeded: run 42: context deadline exceeded" {
		t.Fatalf("expiry before the next request lost its specific cause: %v", err)
	}
}

type githubSnapshotTestTransport func(*http.Request) (*http.Response, error)

func (f githubSnapshotTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
