package glock

// Mode 是持有模式。
type Mode uint8

const (
	// Write 互斥（写）模式：同一 Key 至多一个持有者。
	Write Mode = iota
	// Read 共享（读）模式：同一 Key 可多个持有者，与 Write 互斥。
	Read
	// Semaphore 信号量模式：N 上限资源池中的一个槽位持有。仅由
	// SemaphoreDriver.TryAcquireSemaphore 签发（TryAcquire 不接受此模式），
	// Refresh/Release 以它路由到信号量记录——信号量记录的键空间与锁隔离。
	Semaphore
)

func (m Mode) String() string {
	switch m {
	case Read:
		return "read"
	case Semaphore:
		return "semaphore"
	default:
		return "write"
	}
}
