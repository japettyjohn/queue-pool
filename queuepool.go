package queuepool

import (
	"errors"
	"sync"
	"time"
)

var (
	// Used whenever a queue cannot be filled
	ErrQueueEmpty = errors.New("Queue is empty.")

	// Used when a key has not been added
	ErrInvalidKey = errors.New("Invalid key.")
)

// QueueBackend is used to provide data to the QueuePool i.e.
// running a query against a database. See queuepool_test.go
// for a simplistic example. Also expect that all methods of
// this may be called concurrently.
type QueueBackend interface {
	// Provide queueSize many items for the given queue to buffer
	Fill(queueSize int, key string) ([]interface{}, error)

	// Called during Get(), return true to exclude use of queueItem
	Exclude(queueItem interface{}) bool

	// Called when adding queues, should error minimally if queue key must adhere to backend list and is invalid
	AddQueue(key string) error
}

// QueuePool is a thread safe multiqueued queue. While it does
// not concern itself with how the data is provided to the queue
// as this is covered by QueueBackend. But with enough data
// buffered it is expected to deal with 1000s of concurrent
// requests, likely to be throttle by the QueueBackend.
type QueuePool struct {
	queues         map[string]chan interface{}
	queueSize      int
	queueLastFill  map[string]time.Time
	queueMutex     sync.RWMutex
	fillQueueMutex sync.Mutex
	backend        QueueBackend
}

// Provide the number of items to buffer per queue and QueueBackend implementation
func NewQueuePool(queueSize int, backend QueueBackend) *QueuePool {
	return &QueuePool{
		queueSize:     queueSize,
		backend:       backend,
		queueLastFill: make(map[string]time.Time),
		queues:        make(map[string]chan interface{}),
	}
}

// Get a record from the given queue, error if queue is empty or key is invalid.
func (qp *QueuePool) Get(key string) (queueRecord interface{}, e error) {
	var queue chan interface{}
	queue, e = qp.getQueue(key)

	for queueRecord == nil && e == nil {
		select {
		case qr := <-queue:
			if !qp.backend.Exclude(qr) {
				queueRecord = qr
			}
		default:
			e = qp.fillQueue(key, queue)
		}
	}

	return queueRecord, e
}

// Add a one or more queues to the pool. AddQueue(key) must
// be called before using Get(key).
func (qp *QueuePool) AddQueue(keys ...string) error {
	qp.queueMutex.Lock()
	defer qp.queueMutex.Unlock()
	for _, k := range keys {
		qp.queues[k] = make(chan interface{}, qp.queueSize)
		if e := qp.backend.AddQueue(k); e != nil {
			return e
		}
	}
	return nil
}

func (qp *QueuePool) getQueue(key string) (chan interface{}, error) {
	qp.queueMutex.RLock()
	defer qp.queueMutex.RUnlock()
	buf, ok := qp.queues[key]
	if !ok {
		return nil, ErrInvalidKey
	}
	return buf, nil
}

// Called to refill a queue, this must not be called unless a queue is empty
func (qp *QueuePool) fillQueue(key string, buf chan interface{}) error {
	requestTime := time.Now()
	qp.fillQueueMutex.Lock()
	defer qp.fillQueueMutex.Unlock()

	if !requestTime.After(qp.queueLastFill[key]) {
		return nil
	}

	items, e := qp.backend.Fill(qp.queueSize, key)
	if e != nil {
		return e
	} else if l := len(items); l == 0 {
		return ErrQueueEmpty
	} else if l > qp.queueSize {
		items = items[:qp.queueSize]
	}

	for _, item := range items {
		buf <- item
	}

	return nil
}
