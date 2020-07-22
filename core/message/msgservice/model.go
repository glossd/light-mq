package msgservice

import "fmt"

type Message struct {
	Offset uint64
	Body   []byte
}

func (m Message) String() string {
	return fmt.Sprintf("{o:%d, m:%s}", m.Offset, m.Body)
}
