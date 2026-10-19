package scalers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGetWorkflowRunJobs_PaginatesSmallPages(t *testing.T) {
	for _, enableEtags := range []bool{false, true} {
		t.Run(strconv.FormatBool(enableEtags), func(t *testing.T) {
			var pages []int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("per_page") != "30" {
					// The large GitHub response consistently fails for affected matrix runs.
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				page, err := strconv.Atoi(r.URL.Query().Get("page"))
				if err != nil || page < 1 || page > 4 {
					t.Errorf("unexpected page: %s", r.URL.RawQuery)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				pages = append(pages, page)
				jobs := make([]Job, 0, 30)
				for id := (page-1)*30 + 1; id <= page*30 && id <= 116; id++ {
					jobs = append(jobs, Job{ID: id, RunID: 42, Status: "queued"})
				}
				w.Header().Set("ETag", fmt.Sprintf("\"page-%d\"", page))
				_ = json.NewEncoder(w).Encode(Jobs{TotalCount: 116, Jobs: jobs})
			}))
			defer srv.Close()
			s := paginationTestScaler(srv.URL, enableEtags)
			jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
			if err != nil {
				t.Fatalf("complete job snapshot: %v", err)
			}
			if len(jobs) != 116 || len(pages) != 4 {
				t.Fatalf("got %d jobs over pages %v; want 116 jobs over four pages", len(jobs), pages)
			}
			for i, job := range jobs {
				if job.ID != i+1 {
					t.Fatalf("job %d has ID %d", i, job.ID)
				}
			}
		})
	}
}

func TestGetWorkflowRunJobs_RevalidatesEveryCachedPage(t *testing.T) {
	var requests [3]int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 || page > 2 {
			t.Errorf("unexpected page: %s", r.URL.RawQuery)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests[page]++
		etag := fmt.Sprintf("\"page-%d\"", page)
		if page == 2 && requests[page] > 1 {
			etag = "\"page-2-updated\""
		}
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		jobs := make([]Job, 0, 30)
		for id := (page-1)*30 + 1; id <= page*30 && id <= 31; id++ {
			status := "queued"
			if page == 2 && requests[page] > 1 {
				status = "completed"
			}
			jobs = append(jobs, Job{ID: id, Status: status})
		}
		w.Header().Set("ETag", etag)
		_ = json.NewEncoder(w).Encode(Jobs{TotalCount: 31, Jobs: jobs})
	}))
	defer srv.Close()
	s := paginationTestScaler(srv.URL, true)
	for scan := 1; scan <= 3; scan++ {
		jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
		if err != nil || len(jobs) != 31 {
			t.Fatalf("scan %d: got %d jobs, error %v", scan, len(jobs), err)
		}
		if scan > 1 && jobs[30].Status != "completed" {
			t.Fatalf("scan %d reused stale page 2 after page 1 returned 304", scan)
		}
	}
	if requests != [3]int{0, 3, 3} {
		t.Fatalf("each page must be revalidated every scan: requests = %v", requests)
	}
}

func paginationTestScaler(apiURL string, enableEtags bool) *githubRunnerScaler {
	return &githubRunnerScaler{
		metadata:     &githubRunnerMetadata{GithubAPIURL: apiURL, Owner: "owner", EnableEtags: enableEtags},
		httpClient:   http.DefaultClient,
		etags:        map[string]string{},
		previousJobs: map[jobCacheKey][]githubJobPage{},
		previousWfrs: map[string]map[string]*WorkflowRuns{},
	}
}

func TestGetWorkflowRunJobs_PageBoundaries(t *testing.T) {
	for _, count := range []int{0, 1, 30, 31, 62, 100, 116} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			var requests int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				page, _ := strconv.Atoi(r.URL.Query().Get("page"))
				_ = json.NewEncoder(w).Encode(paginationTestJobs(page, count, "queued"))
			}))
			defer srv.Close()
			s := paginationTestScaler(srv.URL, false)
			jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
			wantRequests := max(1, (count+29)/30)
			if err != nil || len(jobs) != count || requests != wantRequests {
				t.Fatalf("got %d jobs, %d requests, error %v; want %d jobs in %d requests", len(jobs), requests, err, count, wantRequests)
			}
		})
	}
}

func TestGetWorkflowRunJobs_PageErrorsDoNotReturnPartialJobs(t *testing.T) {
	for _, status := range []int{401, 403, 404, 429, 500, 502, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.URL.Query().Get("page") == "2" {
					if status == http.StatusTooManyRequests {
						w.Header().Set("X-RateLimit-Remaining", "0")
						w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
					}
					w.WriteHeader(status)
					return
				}
				_ = json.NewEncoder(w).Encode(paginationTestJobs(1, 31, "queued"))
			}))
			defer srv.Close()
			s := paginationTestScaler(srv.URL, true)
			jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
			if err == nil || jobs != nil || requests != 2 || len(s.previousJobs) != 0 {
				t.Fatalf("got jobs %v, requests %d, error %v, cached runs %d", jobs, requests, err, len(s.previousJobs))
			}
			if status == http.StatusTooManyRequests && !s.isRateLimited() {
				t.Fatal("rate limit response must retain the existing rate limit state")
			}
		})
	}
}

func TestGetWorkflowQueueLength_PaginationFailurePreservesCompleteSnapshot(t *testing.T) {
	scan := 0
	var validators []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/actions/runs") {
			if r.URL.Query().Get("status") == "queued" {
				_ = json.NewEncoder(w).Encode(WorkflowRuns{TotalCount: 1, WorkflowRuns: []WorkflowRun{{ID: 42, Status: "queued", Repository: Repo{Name: "repo"}}}})
			} else {
				_ = json.NewEncoder(w).Encode(WorkflowRuns{})
			}
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page == 1 {
			scan++
		}
		if scan == 3 {
			validators = append(validators, r.Header.Get("If-None-Match"))
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if scan == 2 && page == 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		etag := fmt.Sprintf("\"scan-%d-page-%d\"", scan, page)
		w.Header().Set("ETag", etag)
		status := "queued"
		if scan == 2 {
			status = "completed"
		}
		_ = json.NewEncoder(w).Encode(paginationTestJobs(page, 31, status))
	}))
	defer srv.Close()
	s := paginationTestScaler(srv.URL, true)
	s.metadata.RunnerScope = REPO
	s.metadata.Repos = []string{"repo"}
	s.metadata.NoDefaultLabels = true
	queue, err := s.GetWorkflowQueueLength(context.Background())
	if err != nil || queue != 31 {
		t.Fatalf("initial complete scan: queue %d, error %v", queue, err)
	}
	timestamp := s.previousQueueLengthTime
	previousPages := s.previousJobs[jobCacheKey{repo: "repo", runID: 42}]
	queue, err = s.GetWorkflowQueueLength(context.Background())
	if err == nil || queue != -1 {
		t.Fatalf("incomplete scan must fail: queue %d, error %v", queue, err)
	}
	if s.previousQueueLength != 31 || s.previousQueueLengthTime != timestamp || !reflect.DeepEqual(previousPages, s.previousJobs[jobCacheKey{repo: "repo", runID: 42}]) {
		t.Fatal("failed scan replaced complete demand or page cache")
	}
	queue, err = s.GetWorkflowQueueLength(context.Background())
	if err != nil || queue != 31 {
		t.Fatalf("revalidated prior snapshot: queue %d, error %v", queue, err)
	}
	if want := []string{`"scan-1-page-1"`, `"scan-1-page-2"`}; !reflect.DeepEqual(validators, want) {
		t.Fatalf("cache validators advanced on failed scan: got %v, want %v", validators, want)
	}
}

func TestGetWorkflowRunJobs_InvalidPaginationFailsClosed(t *testing.T) {
	tests := []struct {
		name  string
		page1 Jobs
		page2 Jobs
	}{
		{name: "negative total", page1: Jobs{TotalCount: -1}},
		{name: "excessive total", page1: Jobs{TotalCount: githubRunnerMaxJobPages*githubDefaultPerPage + 1}},
		{name: "truncated first page", page1: Jobs{TotalCount: 31, Jobs: []Job{{ID: 1}}}},
		{name: "truncated last page", page1: paginationTestJobs(1, 31, "queued"), page2: Jobs{TotalCount: 31}},
		{name: "changing total", page1: paginationTestJobs(1, 31, "queued"), page2: paginationTestJobs(2, 32, "queued")},
		{name: "duplicate job", page1: paginationTestJobs(1, 31, "queued"), page2: Jobs{TotalCount: 31, Jobs: []Job{{ID: 1}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests++
				jobs := tt.page1
				if requests > 1 {
					jobs = tt.page2
				}
				_ = json.NewEncoder(w).Encode(jobs)
			}))
			defer srv.Close()
			s := paginationTestScaler(srv.URL, true)
			jobs, err := s.getWorkflowRunJobs(context.Background(), 42, "repo")
			if err == nil || jobs != nil || len(s.previousJobs) != 0 || requests > 2 {
				t.Fatalf("invalid snapshot: jobs %v, error %v, cached runs %d, requests %d", jobs, err, len(s.previousJobs), requests)
			}
		})
	}
}

func TestGetWorkflowRunJobs_CancellationStopsPagination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			cancel()
			<-r.Context().Done()
			return
		}
		_ = json.NewEncoder(w).Encode(paginationTestJobs(1, 31, "queued"))
	}))
	defer srv.Close()
	s := paginationTestScaler(srv.URL, true)
	jobs, err := s.getWorkflowRunJobs(ctx, 42, "repo")
	if !errors.Is(err, context.Canceled) || jobs != nil || len(s.previousJobs) != 0 {
		t.Fatalf("canceled scan: jobs %v, error %v, cached runs %d", jobs, err, len(s.previousJobs))
	}
}

func TestGetWorkflowRunJobs_CacheIsolatedByRepositoryAndRun(t *testing.T) {
	var mu sync.Mutex
	requests := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		key := r.URL.Path
		requests[key]++
		etag := strconv.Quote(key)
		if requests[key] == 1 {
			if got := r.Header.Get("If-None-Match"); got != "" {
				t.Errorf("first request for %s reused another run's validator: %s", key, got)
			}
		} else {
			if got := r.Header.Get("If-None-Match"); got != etag {
				t.Errorf("validator for %s: got %s, want %s", key, got, etag)
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		_ = json.NewEncoder(w).Encode(Jobs{TotalCount: 1, Jobs: []Job{{ID: len(key), Name: key}}})
	}))
	defer srv.Close()
	s := paginationTestScaler(srv.URL, true)
	for scan := 0; scan < 2; scan++ {
		var group sync.WaitGroup
		for _, repo := range []string{"repo-a", "repo-b"} {
			for _, runID := range []int64{42, 43} {
				group.Go(func() {
					jobs, err := s.getWorkflowRunJobs(context.Background(), runID, repo)
					wantPath := fmt.Sprintf("/repos/owner/%s/actions/runs/%d/jobs", repo, runID)
					if err != nil || len(jobs) != 1 || jobs[0].Name != wantPath {
						t.Errorf("repo %s run %d: got %v, error %v", repo, runID, jobs, err)
					}
				})
			}
		}
		group.Wait()
	}
}

func paginationTestJobs(page, count int, status string) Jobs {
	jobs := make([]Job, 0, 30)
	for id := (page-1)*30 + 1; id <= page*30 && id <= count; id++ {
		jobs = append(jobs, Job{ID: id, RunID: 42, Status: status})
	}
	return Jobs{TotalCount: count, Jobs: jobs}
}

func TestGithubRunnerPruneCachesBoundsJobPages(t *testing.T) {
	s := paginationTestScaler("https://api.github.com", true)
	for runID := int64(1); runID <= 6; runID++ {
		s.previousJobs[jobCacheKey{repo: "repo", runID: runID}] = make([]githubJobPage, githubRunnerMaxJobPages)
	}
	s.pruneCaches([]string{"repo"})
	pages := 0
	for _, cached := range s.previousJobs {
		pages += len(cached)
	}
	if pages > githubScalerMaxCacheEntries {
		t.Fatalf("cached job pages %d exceed bound %d", pages, githubScalerMaxCacheEntries)
	}
}
