package bufio

// TrackedBufioReader wraps a bufio.Reader and tracks the number of bytes read.
type TrackedBufioReader struct {
	r reader
	n uint64
}

type reader interface {
	ReadString(delim byte) (string, error)
	ReadByte() (byte, error)
}

func NewTrackedBufioReader(r reader) *TrackedBufioReader {
	return &TrackedBufioReader{r: r}
}

func (t *TrackedBufioReader) ReadString(delim byte) (string, error) {
	s, err := t.r.ReadString(delim)
	if err != nil {
		return "", err
	}
	t.n += uint64(len([]byte(s)))
	return s, nil
}

func (t *TrackedBufioReader) ReadByte() (byte, error) {
	b, err := t.r.ReadByte()
	if err != nil {
		return 0, err
	}
	t.n++
	return b, nil
}

func (t *TrackedBufioReader) NAndReset() uint64 {
	n := t.n
	t.n = 0
	return n
}
