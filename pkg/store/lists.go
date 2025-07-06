package store

type Deque struct {
	Buffer []string
	Head   int
	Tail   int
	Size   int
}

func NewDeque(size int) *Deque {
	return &Deque{
		Buffer: make([]string, size),
		Head:   0,
		Tail:   0,
		Size:   0,
	}
}

func (d *Deque) Wrap(index int) int {
	n := len(d.Buffer)
	return (index + n) % n
}

func (d *Deque) Grow() {
	newSize := len(d.Buffer) * 2
	if newSize == 0 {
		newSize = 4
	}

	newBuffer := make([]string, newSize)
	for idx, _ := range d.Buffer {
		newBuffer[idx] = d.Buffer[d.Wrap(d.Head+idx)]
	}

	d.Buffer = newBuffer
	d.Head = 0
	d.Tail = d.Size
}
