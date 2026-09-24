package glock

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// MultiLock 是一组互斥持有的句柄，由 Locker.AcquireAll/TryAcquireAll 返回。
// 整组一个 watchdog：任一成员续约失败即整组标记丢失。
type MultiLock struct {
	holds   []*hold
	lost    chan struct{}
	lostOne sync.Once
}

func newMultiLock(holds []*hold) *MultiLock {
	ml := &MultiLock{holds: holds, lost: make(chan struct{})}
	// 任一成员终结（丢失或释放）即关闭整组的 Lost；全组释放时所有源都会关闭。
	for _, h := range holds {
		go func(h *hold) {
			<-h.Lost()
			ml.lostOne.Do(func() { close(ml.lost) })
		}(h)
	}
	return ml
}

// Release 释放整组（逆序）。全部尝试完毕后返回聚合错误；单个成员失败
// 不阻断其余成员的释放。
func (m *MultiLock) Release(ctx context.Context) error {
	var errs []error
	for i := len(m.holds) - 1; i >= 0; i-- {
		if err := m.holds[i].release(ctx); err != nil {
			errs = append(errs, fmt.Errorf("key %q: %w", m.holds[i].pubKey, err))
		}
	}
	m.lostOne.Do(func() { close(m.lost) })
	return errors.Join(errs...)
}

// Refresh 手动续约整组；任一成员失败即整组标记丢失并返回错误。
func (m *MultiLock) Refresh(ctx context.Context) error {
	for _, h := range m.holds {
		if err := h.refresh(ctx); err != nil {
			for _, other := range m.holds {
				other.markLost(err)
			}
			return err
		}
	}
	return nil
}

// Lost 在整组终结（任一成员丢失，或整组释放）时关闭。
func (m *MultiLock) Lost() <-chan struct{} { return m.lost }

// OwnerToken 返回持有者身份（整组同一身份）。
func (m *MultiLock) OwnerToken() string { return m.holds[0].OwnerToken() }

// FencingTokens 返回 Key → 栅栏令牌的映射（Key 为使用者传入的原始值）。
func (m *MultiLock) FencingTokens() map[string]uint64 {
	out := make(map[string]uint64, len(m.holds))
	for _, h := range m.holds {
		out[h.pubKey] = h.fence
	}
	return out
}
