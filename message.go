package dispatcher

type message[T comparable] struct {
	MessageID T
	Args      []interface{}
}

type MessageHandler func(params ...interface{})
