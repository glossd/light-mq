package pubsub

type InputError struct {
	Msg string
}

func (e InputError) Error() string {
	return e.Msg
}
