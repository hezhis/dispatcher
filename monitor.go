package dispatcher

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	once      sync.Once
	singleton *monitor
)

type description struct {
	flag            atomic.Bool
	timeoutCallback func()
}

type monitor struct {
	dispatchers sync.Map
	exit        chan struct{}
}

func getMonitor() *monitor {
	once.Do(func() {
		singleton = &monitor{
			dispatchers: sync.Map{},
			exit:        make(chan struct{}, 1),
		}

	})
	return singleton
}

func (m *monitor) stop() {
	m.exit <- struct{}{}
	close(m.exit)
}

func (m *monitor) register(name string, callback func()) error {
	if _, ok := m.dispatchers.Load(name); ok {
		return fmt.Errorf("dispatcher %s already registered", name)
	}

	m.dispatchers.Store(name, &description{
		timeoutCallback: callback,
	})
	return nil
}

func (m *monitor) report(name string) {
	if desc, ok := m.dispatchers.Load(name); ok {
		desc.(*description).flag.Store(false)
	}
}

func (m *monitor) start() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGKILL, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGQUIT)

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	for {
		select {
		case <-m.exit:
			return
		case <-sc:
			m.stop()
		case <-ticker.C:
			m.dispatchers.Range(func(_, value interface{}) bool {
				if desc, ok := value.(*description); ok {
					if desc.flag.Load() {
						desc.timeoutCallback()
					}
				}
				return true
			})
		}
	}
}

func init() {
	go getMonitor().start()
}
