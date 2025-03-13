package dispatcher

type Logger interface {
	LogInfo(format string, args ...interface{})
	LogError(format string, args ...interface{})
	LogStack(format string, args ...interface{})
}
