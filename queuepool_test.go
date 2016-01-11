package queuepool

import (
	"fmt"
	"sync"
	"testing"
)

type backend struct {
	exclusionMutex sync.Mutex
	exclusion      map[int64]interface{}
	fillCount      int
	fillMutex      sync.Mutex
}

func (b *backend) Fill(queueSize int, key string) ([]interface{}, error) {
	b.fillMutex.Lock()
	c := b.fillCount
	b.fillCount++
	b.fillMutex.Unlock()

	f := make([]interface{}, 0, queueSize)
	offset := queueSize / 2 * c
	for i := offset; i < queueSize+offset; i++ {
		f = append(f, int64(i))
	}
	return f, nil
}

func (b *backend) Gotten(queueSize interface{}) error {
	return nil
}

func (b *backend) Used(queueItem interface{}) error {
	return nil
}

func (b *backend) AddQueue(queue string) error {
	return nil
}

func (b *backend) Exclude(queueItem interface{}) bool {
	qi := queueItem.(int64)
	// Not using a RW map as the most common case should be a write
	b.exclusionMutex.Lock()
	defer b.exclusionMutex.Unlock()
	if _, ok := b.exclusion[qi]; ok {
		return true
	}
	b.exclusion[qi] = nil
	return false
}

func TestGetNext(t *testing.T) {
	b := &backend{exclusion: map[int64]interface{}{}}
	q := NewQueuePool(100, b)
	results := make([]int64, 0, 2000)
	resultsChan := make(chan int64, 100)
	uniqueResults := map[int64]interface{}{}

	var wgRecords sync.WaitGroup
	go func() {
		for r := range resultsChan {
			results = append(results, r)
			uniqueResults[r] = nil
			wgRecords.Done()
		}
	}()
	wgRecords.Add(1000 * 10)
	for i := 0; i < 10; i++ {
		qName := fmt.Sprintf("test%d", i)
		q.AddQueue(qName)
		go func(qName string) {
			for j := 0; j < 1000; j++ {
				i, e := q.Get(qName)
				if e != nil {
					fmt.Println(e)
				}
				resultsChan <- i.(int64)

			}
		}(qName)
	}

	wgRecords.Wait()
	close(resultsChan)

	if resultsCount, uniqResultsCount := len(results), len(uniqueResults); resultsCount != uniqResultsCount {
		t.Fatalf("Total results and unique results differed: %d != %d ", resultsCount, uniqResultsCount)
	} else {
		t.Logf("Total queue records pulled %d", resultsCount)
	}
}
