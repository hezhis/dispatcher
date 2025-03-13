package dispatcher

import "time"

type OptionFunc func(*Option)

type Option struct {
	fetchOnce int

	loopFunc   func()
	beforeLoop func()
	afterLoop  func()

	slowTime              time.Duration
	batchMessageMaxWait   time.Duration
	loopEventProcInterval time.Duration
}

func NewOption(opts ...OptionFunc) *Option {
	option := &Option{
		fetchOnce:             100,
		slowTime:              20 * time.Millisecond,
		batchMessageMaxWait:   5 * time.Millisecond,
		loopEventProcInterval: 10 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(option)
	}

	return option
}

func WithFetchOnce(fetchOnce int) OptionFunc {
	return func(option *Option) {
		option.fetchOnce = fetchOnce
	}
}

func WithLoopFunc(loopFunc func()) OptionFunc {
	return func(option *Option) {
		option.loopFunc = loopFunc
	}
}

func WithBeforeLoop(beforeLoop func()) OptionFunc {
	return func(option *Option) {
		option.beforeLoop = beforeLoop
	}
}

func WithAfterLoop(afterLoop func()) OptionFunc {
	return func(option *Option) {
		option.afterLoop = afterLoop
	}
}

func WithSlowTime(slowTime time.Duration) OptionFunc {
	return func(option *Option) {
		option.slowTime = slowTime
	}
}

func WithBatchMessageMaxWait(batchMessageMaxWait time.Duration) OptionFunc {
	return func(option *Option) {
		option.batchMessageMaxWait = batchMessageMaxWait
	}
}

func WithLoopEventProcInterval(loopEventProcInterval time.Duration) OptionFunc {
	return func(option *Option) {
		option.loopEventProcInterval = loopEventProcInterval
	}
}
