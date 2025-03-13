package dispatcher

import (
	"log"
	"testing"
	"time"
)

type TestLogger struct {
}

func (l TestLogger) LogInfo(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (l TestLogger) LogError(format string, args ...interface{}) {
	log.Printf(format, args...)
}
func (l TestLogger) LogStack(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func TestDispatcher_Dispatch(t *testing.T) {
	p := NewDispatcher[int]("test", 10000, &TestLogger{}, NewOption(
		WithFetchOnce(50),
		WithAfterLoop(func() { log.Println("after loop") }),
		WithBeforeLoop(func() { log.Println("before loop") }),
		WithLoopFunc(func() { log.Println("loop") }),
	),
	)
	p.RegisterHandler(1, Handle)

	go func() {
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
	}()

	p.Dispatch(1, Args{Count: 10026})
	time.Sleep(time.Second)
	log.Println(p.getCurMessage())

	p.Stop()

	p.wg.Wait()
}

type Args struct {
	Count int64
}

func Handle(args ...interface{}) {
	log.Printf("handle: %v\n", args)
	time.Sleep(time.Second * 5)
}
