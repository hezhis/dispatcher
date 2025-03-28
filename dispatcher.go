package dispatcher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Dispatcher[T comparable] struct {
	name     string
	opts     *Option
	handlers map[T]MessageHandler
	queue    chan *message[T]

	logger Logger

	stopped atomic.Bool

	wg sync.WaitGroup

	curMessageMutex sync.RWMutex
	curMessage      *message[T]
}

func NewDispatcher[T comparable](name string, queueSize int, logger Logger, opts *Option) *Dispatcher[T] {
	return &Dispatcher[T]{
		name:     name,
		logger:   logger,
		opts:     opts,
		handlers: make(map[T]MessageHandler),
		queue:    make(chan *message[T], queueSize),
	}
}

func (d *Dispatcher[T]) RegisterHandler(id T, handler MessageHandler) error {
	if _, ok := d.handlers[id]; ok {
		return fmt.Errorf("dispatcher[%s] handler for message id: %v already exists", d.name, id)
	}
	d.handlers[id] = handler
	return nil
}

func (d *Dispatcher[T]) Dispatch(id T, args ...interface{}) {
	if d.stopped.Load() {
		return
	}
	d.queue <- &message[T]{
		MessageID: id,
		Args:      args,
	}
}

func (d *Dispatcher[T]) Start(beforeLoop, loop, afterLoop func()) error {
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

	if beforeLoop != nil {
		beforeLoop()
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
			if d.single(loop, []*message[T]{rec}) {
				break EndLoop
			}
		case <-doLoopFuncTk.C:
			if d.single(loop, nil) {
				break EndLoop
			}
		}
	}

	if afterLoop != nil {
		afterLoop()
	}

	return nil
}

func (d *Dispatcher[T]) Stop() {
	d.stopped.Store(true)
	close(d.queue)
	d.wg.Wait()

	d.logger.LogInfo("dispatcher %s stop", d.name)
}

func (d *Dispatcher[T]) single(loop func(), list []*message[T]) (exit bool) {
	list, exit = d.fetchQueue(list)

	if loop != nil {
		loop()
	}

	for _, msg := range list {
		d.handleMessage(msg)
	}

	getMonitor().report(d.name)
	return
}

func (d *Dispatcher[T]) fetchQueue(list []*message[T]) ([]*message[T], bool) {
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

func (d *Dispatcher[T]) handleMessage(message *message[T]) {
	d.setCurMessage(message)
	defer d.setCurMessage(nil)

	t := time.Now()
	id := message.MessageID

	defer func() {
		if since := time.Since(t); since > d.opts.slowTime {
			d.logger.LogError("process msg end! id:%v, cost:%v", id, since)
		}

		if err := recover(); err != nil {
			d.logger.LogStack("process msg panic! id:%v, err:%v", id, err)
		}
	}()

	if handler, ok := d.handlers[id]; ok {
		handler(message.Args...)
	} else {
		d.logger.LogError("No handler for message type: %s", id)
	}
}

func (d *Dispatcher[T]) setCurMessage(msg *message[T]) {
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
	return fmt.Sprintf("dispatcher[%s] id:%v, message:%+v", d.name, d.curMessage.MessageID, d.curMessage)
}
