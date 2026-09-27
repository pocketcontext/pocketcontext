package tracing

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
)

const (
	BufferTTL         = 120 * time.Second
	BufferMaxRecords  = 1024
	BufferUserRecords = 64
	BufferUserBytes   = 1 << 20
	BufferRecordBytes = 64 << 10
)

type bufferedTrace struct {
	id, owner, quota string
	data             []byte
	expires          time.Time
}

// Buffer stores completed JSON snapshots, never caller-owned mutable traces.
// Owner is the authenticated collection/account/session key, not client metadata.
// Retrieval is repeatable until expiry; all capacity limits evict oldest entries.
type Buffer struct {
	mu       sync.Mutex
	entries  []bufferedTrace
	bytes    int64
	maxBytes int64
	now      func() time.Time
}

func NewBuffer(maxBytes int64) *Buffer { return &Buffer{maxBytes: maxBytes, now: time.Now} }
func (b *Buffer) remove(i int) {
	b.bytes -= int64(len(b.entries[i].data))
	copy(b.entries[i:], b.entries[i+1:])
	b.entries[len(b.entries)-1] = bufferedTrace{}
	b.entries = b.entries[:len(b.entries)-1]
}
func (b *Buffer) prune(now time.Time) {
	for i := len(b.entries) - 1; i >= 0; i-- {
		if !now.Before(b.entries[i].expires) {
			b.remove(i)
		}
	}
}
func (b *Buffer) Submit(owner string, trace *Trace) {
	data, err := json.Marshal(trace)
	if err != nil || len(data) > BufferRecordBytes || int64(len(data)) > b.maxBytes {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.prune(now)
	quota := owner
	if i := strings.LastIndexByte(owner, ':'); i >= 0 {
		quota = owner[:i]
	}
	count, size := 0, 0
	for _, e := range b.entries {
		if e.quota == quota {
			count++
			size += len(e.data)
		}
	}
	for count >= BufferUserRecords || size+len(data) > BufferUserBytes {
		for i, e := range b.entries {
			if e.quota == quota {
				count--
				size -= len(e.data)
				b.remove(i)
				break
			}
		}
	}
	for len(b.entries) >= BufferMaxRecords || b.bytes+int64(len(data)) > b.maxBytes {
		b.remove(0)
	}
	b.entries = append(b.entries, bufferedTrace{trace.RequestID, owner, quota, data, now.Add(BufferTTL)})
	b.bytes += int64(len(data))
}
func (b *Buffer) Get(owner, id string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(b.now())
	for _, e := range b.entries {
		if e.id == id && e.owner == owner {
			return append([]byte(nil), e.data...), true
		}
	}
	return nil, false
}
