//go:build default

package lock

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

type testSearchable string

func (t testSearchable) Id() string {
	return string(t)
}

func (t testSearchable) Cost() int {
	return len(t)
}

func searchWithTimeout(t *testing.T, s map[string][]testSearchable) map[string]testSearchable {
	done := make(chan map[string]testSearchable)
	go func() {
		done <- search(s)
	}()

	var rv map[string]testSearchable
	select {
	case rv = <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Search timed out after 10 seconds")
	}
	return rv
}

func TestSearch(t *testing.T) {
	t.Run("impossible search", func(t *testing.T) {
		s := map[string][]testSearchable{}
		// 1 less resource than number of requests -> no match possible
		for i := 0; i < 100; i++ {
			requestId := "r" + strconv.Itoa(i)
			s[requestId] = []testSearchable{}
			for resourceId := 0; resourceId < 99; resourceId++ {
				s[requestId] = append(s[requestId], testSearchable(strconv.Itoa(resourceId)))
			}
		}

		if searchWithTimeout(t, s) != nil {
			t.Fatalf("didn't expect search to return a result")
		}
	})

	for testcase := 0; testcase <= 4; testcase += 1 {
		t.Run("difficult search "+strconv.Itoa(testcase), func(t *testing.T) {
			// 64 requests, 64 resources where request 1 gets 1 resource, 2 gets 2, etc.
			const numRequests = 64
			s := map[string][]testSearchable{}
			var resources []testSearchable
			for i := 0; i < numRequests; i++ {
				resources = append(resources, testSearchable(strconv.Itoa(i)))
			}
			rand.Shuffle(len(resources), func(i, j int) {
				resources[i], resources[j] = resources[j], resources[i]
			})
			requestIdStart := rand.Intn(1000) // randomize the requestId so we don't get lucky
			for requestId := 0; requestId < numRequests; requestId++ {
				s["r"+strconv.Itoa(requestIdStart+requestId)] = resources[0 : requestId+1]
			}

			if searchWithTimeout(t, s) == nil {
				t.Fatalf("expected search to return a result")
			}
		})
	}

	t.Run("lowest score search", func(t *testing.T) {
		const numRequests = 100
		s := map[string][]testSearchable{}
		var resources []testSearchable
		expectedSum := 0
		for i := 0; i < numRequests*2; i++ {
			fmtStr := "%0" + strconv.Itoa(i+1) + "d"
			ts := testSearchable(fmt.Sprintf(fmtStr, i))
			resources = append(resources, ts)
			if i < numRequests {
				expectedSum += ts.Cost()
			}
		}
		rand.Shuffle(len(resources), func(i, j int) {
			resources[i], resources[j] = resources[j], resources[i]
		})
		requestIdStart := rand.Intn(1000) // randomize the requestId so we don't get lucky
		for requestId := 0; requestId < numRequests; requestId++ {
			s["r"+strconv.Itoa(requestIdStart+requestId)] = resources
		}

		var rv map[string]testSearchable
		if rv = searchWithTimeout(t, s); rv == nil {
			t.Fatalf("expected search to return a result")
		}
		actualSum := 0
		for _, resource := range rv {
			actualSum += resource.Cost()
		}
		if expectedSum != actualSum {
			t.Fatalf("expected sum of scores to be %d, got %d", expectedSum, actualSum)
		}
	})

}
