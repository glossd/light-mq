package msgservice

type Message struct {
	Offset int
	Body   []byte
}
