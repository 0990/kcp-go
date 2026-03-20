package kcp

import (
	"github.com/pkg/errors"
	"io"
	"sync/atomic"
	"time"
)

// 读取一个kcp的完整包
func (s *UDPSession) ReadPacket() (packet []byte, err error) {
	var timeout *time.Timer
	// deadline for current reading operation
	var c <-chan time.Time
	if trd, ok := s.rd.Load().(time.Time); ok && !trd.IsZero() {
		delay := time.Until(trd)
		timeout = time.NewTimer(delay)
		c = timeout.C
		defer timeout.Stop()
	}

	for {
		s.mu.Lock()
		if size := s.kcp.PeekSize(); size > 0 { // peek data size from kcp
			packet = make([]byte, size)
			s.kcp.Recv(packet)
			s.mu.Unlock()
			atomic.AddUint64(&DefaultSnmp.BytesReceived, uint64(size))
			return packet, nil
		}

		s.mu.Unlock()

		// wait for read event or timeout or error
		select {
		case <-s.chReadEvent:
		case <-c:
			return nil, errors.WithStack(errTimeout)
		case <-s.chSocketReadError:
			return nil, s.socketReadError.Load().(error)
		case <-s.die:
			return nil, errors.WithStack(io.ErrClosedPipe)
		}
	}
}
