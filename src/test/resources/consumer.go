package borges

import (
	"sync"
	"time"

	"gopkg.in/src-d/framework.v0/queue"
)

// Consumer consumes jobs from a queue and uses multiple workers to process
// them.
type Consumer struct {
	Notifiers struct {
		QueueError func(error)
	}
	WorkerPool *WorkerPool
	Queue      queue.Queue

	running bool
	quit    chan struct{}
	done    chan struct{}
	iter    queue.JobIter
	m       *sync.Mutex
}

// NewConsumer creates a new consumer.
func NewConsumer(queue queue.Queue, pool *WorkerPool) *Consumer {
	return &Consumer{
		WorkerPool: pool,
		Queue:      queue,
		m:          &sync.Mutex{},
	}
}

// Start initializes the consumer and starts it, blocking until it is stopped.
func (c *Consumer) Start() {
	c.m.Lock()
	c.quit = make(chan struct{})
	c.done = make(chan struct{})
	c.m.Unlock()

	defer func() { close(c.done) }()

	for {
		select {
		case <-c.quit:
			return
		default:
			if err := c.consumeQueue(c.Queue); err != nil {
				c.notifyQueueError(err)
			}

			c.backoff()
		}
	}
}

// Stop stops the consumer. Note that it does not close the underlying queue
// and worker pool. It blocks until the consumer has actually stopped.
func (c *Consumer) Stop() {
	c.m.Lock()
	close(c.quit)
	if err := c.iter.Close(); err != nil {
		c.notifyQueueError(err)
	}
	c.m.Unlock()
	<-c.done
}

func (c *Consumer) backoff() {
	time.Sleep(time.Second * 5)
}

func (c *Consumer) reject(j *queue.Job, origErr error) {
	c.notifyQueueError(origErr)
	if err := j.Reject(false); err != nil {
		c.notifyQueueError(err)
	}
}

func (c *Consumer) consumeQueue(q queue.Queue) error {
	var err error
	c.m.Lock()
	c.iter, err = c.Queue.Consume(c.WorkerPool.Len())
	c.m.Unlock()
	if err != nil {
		return err
	}

	return c.consumeJobIter(c.iter)
}

func (c *Consumer) consumeJobIter(iter queue.JobIter) error {
	for {
		j, err := iter.Next()
		if err == queue.ErrEmptyJob {
			c.notifyQueueError(err)
			continue
		}

		if err == queue.ErrAlreadyClosed {
			return nil
		}

		if err != nil {
			return err
		}

		if err := c.consumeJob(j); err != nil {
			c.notifyQueueError(err)
		}
	}
}

func (c *Consumer) consumeJob(j *queue.Job) error {
	job := &Job{}
	if err := j.Decode(job); err != nil {
		c.reject(j, err)
		return err
	}

	c.WorkerPool.Do(&WorkerJob{job, j})
	return nil
}

func (c *Consumer) notifyQueueError(err error) {
	if c.Notifiers.QueueError == nil {
		return
	}

	c.Notifiers.QueueError(err)
}
