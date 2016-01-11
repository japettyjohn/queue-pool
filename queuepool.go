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
	Completed(queueItem interface{}) error
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

func (qp *QueuePool) Get(key string) (interface{}, error) {
	queue, e := qp.getQueue(key)
	if e != nil {
		return 0, e
	}

	for {
		select {
		case queueRecord := <-queue:
			if !qp.backend.Exclude(queueRecord) {
				return queueRecord, nil
			}
		default:
			if _, e := qp.fillQueue(key, queue); e != nil {
				return 0, e
			}
		}
	}

	return 0, nil
}

func (qp *QueuePool) Completed(queueItem int64) error {
	return qp.backend.Completed(queueItem)
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

func (qp *QueuePool) fillQueue(key string, buf chan interface{}) (int, error) {
	requestTime := time.Now()
	qp.fillQueueMutex.Lock()
	defer qp.fillQueueMutex.Unlock()
	loaded := 0
	if !requestTime.After(qp.queueLastFill[key]) {
		return 0, nil
	}

	items, e := qp.backend.Fill(qp.queueSize, key)
	if e != nil {
		return 0, e
	}

	// As the current 'length' of a channel cannot be checked,
	// bail when the channel is full
	for _, item := range items {
		select {
		case buf <- item:
			loaded++
		default:
			return loaded, nil
		}
	}

	return loaded, nil
}
