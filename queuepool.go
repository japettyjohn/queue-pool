package queuepool

import (
	"errors"
	"sync"
	"time"
)

var (
	QueueEmpty = errors.New("Queue is empty.")
	InvalidKey = errors.New("Invalid key.")
)

// It should be assumed that any of these methods may be called concurrently
type QueueBackend interface {
	Fill(queueSize int, key string) ([]interface{}, error)
	Exclude(queueItem interface{}) bool
	AddQueue(key string) error
}

// Thread safe multiqueueed queue
type QueuePool struct {
	queues         map[string]chan interface{}
	queueSize      int
	queueLastFill  map[string]time.Time
	queueMutex     sync.RWMutex
	fillQueueMutex sync.Mutex
	backend        QueueBackend
}

func NewQueuePool(queueSize int, backend QueueBackend) *QueuePool {
	return &QueuePool{
		queueSize:     queueSize,
		backend:       backend,
		queueLastFill: make(map[string]time.Time),
		queues:        make(map[string]chan interface{}),
	}
}

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
		return nil, InvalidKey
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
		return QueueEmpty
	} else if l > qp.queueSize {
		items = items[:qp.queueSize]
	}

	for _, item := range items {
		buf <- item
	}

	return nil
}
