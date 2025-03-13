package dispatcher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Message[T comparable] interface {
	MessageID() T
}

type MessageHandler[T comparable] func(message Message[T])

type Dispatcher[T comparable] struct {
	name     string
	opts     *Option
	handlers map[T]MessageHandler[T]
	queue    chan Message[T]

	logger Logger

	stopped atomic.Bool

	wg sync.WaitGroup

	curMessageMutex sync.RWMutex
	curMessage      Message[T]
}

func NewDispatcher[T comparable](name string, queueSize int, logger Logger, opts *Option) *Dispatcher[T] {
	return &Dispatcher[T]{
		name:     name,
		logger:   logger,
		opts:     opts,
		handlers: make(map[T]MessageHandler[T]),
		queue:    make(chan Message[T], queueSize),
	}
}

func (d *Dispatcher[T]) RegisterHandler(id T, handler MessageHandler[T]) {
	d.handlers[id] = handler
}

func (d *Dispatcher[T]) Dispatch(msg Message[T]) {
	if d.stopped.Load() {
		return
	}
	d.queue <- msg
}

func (d *Dispatcher[T]) Start() error {
	err := getMonitor().register(d.name, func() {
		var errStr = fmt.Sprintf("dispatcher %s may offline. %s", d.name, d.getCurMessage())
		d.logger.LogError(errStr)
	})
	if err != nil {
		d.logger.LogError("register dispatcher %s to monitor failed, err:%v", d.name, err)
		return err
	}
	d.wg.Add(1)

	d.logger.LogInfo("dispatcher %s start", d.name)

	defer func() {
		d.wg.Done()

		if err := recover(); err != nil {
			d.logger.LogStack("dispatcher panic! name:%v, err:%v", d.name, err)
		}
	}()

	if nil != d.opts.beforeLoop {
		d.opts.beforeLoop()
	}

	doLoopFuncTk := time.NewTicker(d.opts.loopEventProcInterval)
	defer doLoopFuncTk.Stop()
EndLoop:
	for {
		select {
		case rec, ok := <-d.queue:
			if !ok {
				break EndLoop
			}
			if d.single([]Message[T]{rec}) {
				break EndLoop
			}
		case <-doLoopFuncTk.C:
			if d.single(nil) {
				break EndLoop
			}
		}
	}

	if nil != d.opts.afterLoop {
		d.opts.afterLoop()
	}

	return nil
}

func (d *Dispatcher[T]) Stop() {
	d.stopped.Store(true)
	close(d.queue)
	d.wg.Wait()

	d.logger.LogInfo("dispatcher %s stop", d.name)
}

func (d *Dispatcher[T]) single(list []Message[T]) (exit bool) {
	list, exit = d.fetchQueue(list)

	d.opts.loopFunc()

	for _, msg := range list {
		d.handleMessage(msg)
	}

	getMonitor().report(d.name)
	return
}

func (d *Dispatcher[T]) fetchQueue(list []Message[T]) ([]Message[T], bool) {
	t := time.Now()
	for {
		select {
		case msg, ok := <-d.queue:
			if !ok {
				return list, true
			}
			list = append(list, msg)
			if len(list) >= d.opts.fetchOnce {
				return list, false
			}
			if since := time.Since(t); since > d.opts.batchMessageMaxWait {
				return list, false
			}
		default:
			return list, false
		}
	}
}

func (d *Dispatcher[T]) handleMessage(message Message[T]) {
	d.setCurMessage(message)
	defer d.setCurMessage(nil)

	t := time.Now()
	id := message.MessageID()

	defer func() {
		if since := time.Since(t); since > d.opts.slowTime {
			d.logger.LogError("process msg end! id:%v, cost:%v", id, since)
		}

		if err := recover(); err != nil {
			d.logger.LogStack("process msg panic! id:%v, err:%v", id, err)
		}
	}()

	if handler, ok := d.handlers[id]; ok {
		handler(message)
	} else {
		d.logger.LogError("No handler for message type: %s", id)
	}
}

func (d *Dispatcher[T]) setCurMessage(msg Message[T]) {
	d.curMessageMutex.Lock()
	defer d.curMessageMutex.Unlock()
	d.curMessage = msg
}

func (d *Dispatcher[T]) getCurMessage() string {
	d.curMessageMutex.RLock()
	defer d.curMessageMutex.RUnlock()
	if d.curMessage == nil {
		return ""
	}
	return fmt.Sprintf("dispatcher[%s] id:%v, message:%+v", d.name, d.curMessage.MessageID(), d.curMessage)
}
