package tracing

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBufferOwnershipExpiry(t *testing.T) {
	b := NewBuffer(1 << 20)
	now := time.Now()
	b.now = func() time.Time { return now }
	tr := &Trace{RequestID: "one", SQL: "original"}
	b.Submit("collection:user:key", tr)
	tr.SQL = "changed"
	data, ok := b.Get("collection:user:key", "one")
	if !ok || !strings.Contains(string(data), "original") {
		t.Fatal("not snapshotted")
	}
	data[0] = 'x'
	again, _ := b.Get("collection:user:key", "one")
	if again[0] != '{' {
		t.Fatal("mutable result")
	}
	for _, owner := range []string{"other:user:key", "collection:other:key", "collection:user:newkey"} {
		if _, ok := b.Get(owner, "one"); ok {
			t.Fatal("owner leak")
		}
	}
	now = now.Add(BufferTTL)
	if _, ok := b.Get("collection:user:key", "one"); ok {
		t.Fatal("expired")
	}
	if len(b.entries) != 0 || b.bytes != 0 {
		t.Fatal("not pruned")
	}
}
func TestBufferBounds(t *testing.T) {
	b := NewBuffer(1 << 20)
	for i := 0; i < BufferUserRecords+2; i++ {
		b.Submit("a", &Trace{RequestID: fmt.Sprint(i)})
	}
	if _, ok := b.Get("a", "0"); ok {
		t.Fatal("user count")
	}
	if len(b.entries) != BufferUserRecords {
		t.Fatal(len(b.entries))
	}
	b = NewBuffer(16 << 20)
	for i := 0; i < BufferMaxRecords+2; i++ {
		b.Submit(fmt.Sprint(i), &Trace{RequestID: fmt.Sprint(i)})
	}
	if len(b.entries) != BufferMaxRecords {
		t.Fatal("global count")
	}
	if _, ok := b.Get("0", "0"); ok {
		t.Fatal("oldest global")
	}
	b = NewBuffer(2 << 20)
	for i := 0; i < 100; i++ {
		b.Submit("same", &Trace{RequestID: fmt.Sprint(i), SQL: strings.Repeat("x", 40000)})
	}
	if b.bytes > BufferUserBytes {
		t.Fatal("user bytes")
	}
	b = NewBuffer(1 << 20)
	for i := 0; i < 100; i++ {
		b.Submit(fmt.Sprint(i), &Trace{RequestID: fmt.Sprint(i), SQL: strings.Repeat("x", 40000)})
	}
	if b.bytes > 1<<20 {
		t.Fatal("global bytes")
	}
	before := len(b.entries)
	b.Submit("big", &Trace{RequestID: "big", SQL: strings.Repeat("x", BufferRecordBytes)})
	if len(b.entries) != before {
		t.Fatal("oversized")
	}
}
func TestBufferConcurrent(t *testing.T) {
	b := NewBuffer(1 << 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				id := fmt.Sprintf("%d-%d", i, j)
				b.Submit("owner", &Trace{RequestID: id})
				b.Get("owner", id)
			}
		}(i)
	}
	wg.Wait()
}
func TestBufferConfig(t *testing.T) {
	c := Config{Enabled: true, Delivery: "buffer", Service: "app"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Delivery = "remote"
	if c.Validate() == nil {
		t.Fatal("invalid delivery")
	}
}

func TestBufferAccountQuotaSurvivesKeyRotation(t *testing.T) {
	b := NewBuffer(16 << 20)
	for i := 0; i < 100; i++ {
		b.Submit(fmt.Sprintf("collection:user:key%d", i), &Trace{RequestID: fmt.Sprint(i)})
	}
	if len(b.entries) != BufferUserRecords {
		t.Fatal("rotating session bypassed account quota", len(b.entries))
	}
}
