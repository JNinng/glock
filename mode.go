package glock

// Mode 是锁模式。
type Mode uint8

const (
	// Write 互斥（写）模式：同一 Key 至多一个持有者。
	Write Mode = iota
	// Read 共享（读）模式：同一 Key 可多个持有者，与 Write 互斥。
	Read
)

func (m Mode) String() string {
	if m == Read {
		return "read"
	}
	return "write"
}
