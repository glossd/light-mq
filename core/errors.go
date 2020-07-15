package core

type InputError struct {
	Msg string
}

func (e InputError) Error() string {
	return e.Msg
}

var emptyTopicError = InputError{Msg: "Topic can't be empty"}
