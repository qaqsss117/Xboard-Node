package tracker

import (
	"testing"
	"time"
)

func TestProcess_InitialTraffic(t *testing.T) {
	tr := New()
	cumTraffic := map[int][2]int64{
		1: {100, 200},
		2: {50, 80},
	}
	aliveIPs := map[int]map[string]bool{
		1: {"1.1.1.1": true},
		2: {"2.2.2.2": true},
	}
	tr.Process(cumTraffic, aliveIPs, 2)

	flushed := tr.FlushTraffic()
	if flushed[1] != [2]int64{100, 200} {
		t.Errorf("user 1 traffic: got %v", flushed[1])
	}
	if flushed[2] != [2]int64{50, 80} {
		t.Errorf("user 2 traffic: got %v", flushed[2])
	}
}

func TestProcess_DeltaCalculation(t *testing.T) {
	tr := New()

	// First tick
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)

	// Second tick — counters advanced
	tr.Process(map[int][2]int64{1: {300, 500}}, nil, 1)

	flushed := tr.FlushTraffic()
	// Total: first tick 100/200 + delta 200/300 = 300/500
	if flushed[1] != [2]int64{300, 500} {
		t.Errorf("accumulated: got %v, want [300,500]", flushed[1])
	}
}

func TestProcess_CounterReset(t *testing.T) {
	tr := New()

	// First tick with high counters
	tr.Process(map[int][2]int64{1: {1000, 2000}}, nil, 1)
	tr.FlushTraffic() // clear accumulator

	// Counter reset (new value < old value) — treat as fresh
	tr.Process(map[int][2]int64{1: {50, 80}}, nil, 1)

	flushed := tr.FlushTraffic()
	if flushed[1] != [2]int64{50, 80} {
		t.Errorf("counter reset: got %v, want [50,80]", flushed[1])
	}
}

func TestProcess_UserDisappears(t *testing.T) {
	tr := New()

	// Tick 1: user present
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)

	// Tick 2: user gone
	tr.Process(map[int][2]int64{}, nil, 0)

	// Tick 3: user reappears — should count full bytes (treated as new)
	tr.Process(map[int][2]int64{1: {50, 30}}, nil, 1)

	flushed := tr.FlushTraffic()
	// First tick: 100/200, tick 2: 0, tick 3: 50/30 = total 150/230
	if flushed[1] != [2]int64{150, 230} {
		t.Errorf("after disappear: got %v, want [150,230]", flushed[1])
	}
}

func TestProcess_ZeroTraffic(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{}, nil, 0)

	if tr.HasTraffic() {
		t.Error("expected no traffic for empty input")
	}
}

func TestFlushTraffic(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{
		1: {100, 200},
		2: {50, 80},
	}, nil, 2)

	flushed := tr.FlushTraffic()
	if flushed[1] != [2]int64{100, 200} {
		t.Errorf("flushed user 1: got %v", flushed[1])
	}
	if flushed[2] != [2]int64{50, 80} {
		t.Errorf("flushed user 2: got %v", flushed[2])
	}

	// After flush, should be empty
	if tr.HasTraffic() {
		t.Error("expected no traffic after flush")
	}
	flushed2 := tr.FlushTraffic()
	if len(flushed2) != 0 {
		t.Errorf("expected empty after second flush, got %v", flushed2)
	}
}

func TestRestoreTraffic(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)

	flushed := tr.FlushTraffic()
	tr.RestoreTraffic(flushed)

	if !tr.HasTraffic() {
		t.Error("expected traffic after restore")
	}

	restored := tr.FlushTraffic()
	if restored[1] != [2]int64{100, 200} {
		t.Errorf("restored: got %v", restored[1])
	}
}

func TestRestoreTraffic_Additive(t *testing.T) {
	tr := New()

	// Two ticks accumulate
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	tr.Process(map[int][2]int64{1: {200, 400}}, nil, 1)

	// Flush and restore
	first := tr.FlushTraffic()
	tr.RestoreTraffic(first)

	// Generate additional traffic
	tr.Process(map[int][2]int64{1: {250, 500}}, nil, 1)

	final := tr.FlushTraffic()
	// Restored: 200/400 (initial 100/200 + delta 100/200) + new delta 50/100 = 250/500
	if final[1][0] != 250 || final[1][1] != 500 {
		t.Errorf("additive restore: got %v", final[1])
	}
}

func TestFlushAliveIPs(t *testing.T) {
	tr := New()
	aliveIPs := map[int]map[string]bool{
		1: {"1.1.1.1": true, "2.2.2.2": true},
		2: {"3.3.3.3": true},
	}
	tr.Process(map[int][2]int64{1: {100, 200}, 2: {30, 40}}, aliveIPs, 3)

	flushed := tr.FlushAliveIPs()
	if len(flushed[1]) != 2 {
		t.Errorf("user 1 IPs: got %d, want 2", len(flushed[1]))
	}
	if len(flushed[2]) != 1 {
		t.Errorf("user 2 IPs: got %d, want 1", len(flushed[2]))
	}

	// Without another Process call, hash is same → returns nil (skip duplicate)
	flushed2 := tr.FlushAliveIPs()
	if flushed2 != nil {
		t.Errorf("expected nil on duplicate flush, got %v", flushed2)
	}
}

func TestFlushAliveIPs_ResendsAfterWindow(t *testing.T) {
	tr := New()
	aliveIPs := map[int]map[string]bool{
		1: {"1.1.1.1": true},
	}
	tr.Process(map[int][2]int64{1: {100, 200}}, aliveIPs, 1)

	if tr.FlushAliveIPs() == nil {
		t.Fatal("first flush should return data")
	}
	if tr.FlushAliveIPs() != nil {
		t.Fatal("second flush should be suppressed as duplicate")
	}

	// Backdate the last flush past the resend window. The IP set is still
	// unchanged, but the panel's TTL needs refreshing.
	tr.mu.Lock()
	tr.lastAliveFlush = time.Now().Add(-tr.aliveResendAfter - time.Second)
	tr.mu.Unlock()

	forced := tr.FlushAliveIPs()
	if forced == nil {
		t.Fatal("flush after resend window should return data")
	}
	if len(forced[1]) != 1 {
		t.Errorf("user 1 IPs: got %d, want 1", len(forced[1]))
	}

	// The forced resend restarts the window.
	if tr.FlushAliveIPs() != nil {
		t.Error("flush right after forced resend should be suppressed")
	}
}

func TestFlushAliveIPs_ReturnsIndependentCopy(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{1: {100, 200}}, map[int]map[string]bool{
		1: {"1.1.1.1": true},
	}, 1)

	first := tr.FlushAliveIPs()
	if first == nil {
		t.Fatal("first flush should return data")
	}

	// Mutating the returned map must not corrupt later flushes: callers hand
	// it to a goroutine that serializes it concurrently with the next flush.
	first[1][0] = "mutated"
	first[99] = []string{"injected"}

	tr.Process(map[int][2]int64{1: {150, 250}}, map[int]map[string]bool{
		1: {"1.1.1.1": true, "2.2.2.2": true},
	}, 2)

	second := tr.FlushAliveIPs()
	if second == nil {
		t.Fatal("changed IP set should flush")
	}
	if _, leaked := second[99]; leaked {
		t.Error("second flush aliased the first flush's map")
	}
	if len(second[1]) != 2 {
		t.Fatalf("user 1 IPs: got %d, want 2", len(second[1]))
	}
	for _, ip := range second[1] {
		if ip == "mutated" {
			t.Error("second flush shares slice storage with the first")
		}
	}
}

func TestInvalidateAliveIPs(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{1: {100, 200}}, map[int]map[string]bool{
		1: {"1.1.1.1": true},
	}, 1)

	if tr.FlushAliveIPs() == nil {
		t.Fatal("first flush should return data")
	}
	if tr.FlushAliveIPs() != nil {
		t.Fatal("second flush should be suppressed as duplicate")
	}

	// A failed report lifts the suppression without needing data restored:
	// the live snapshot still holds the kernel's current set.
	tr.InvalidateAliveIPs()

	retried := tr.FlushAliveIPs()
	if retried == nil {
		t.Fatal("flush after invalidate should return data")
	}
	if len(retried[1]) != 1 {
		t.Errorf("user 1 IPs: got %d, want 1", len(retried[1]))
	}
}

func TestFlushAliveIPs_DedupSameIP(t *testing.T) {
	tr := New()
	aliveIPs := map[int]map[string]bool{
		1: {"1.1.1.1": true},
	}
	tr.Process(map[int][2]int64{1: {100, 200}}, aliveIPs, 2)

	flushed := tr.FlushAliveIPs()
	if len(flushed[1]) != 1 {
		t.Errorf("expected 1 IP, got %d", len(flushed[1]))
	}
}

func TestHasTraffic(t *testing.T) {
	tr := New()
	if tr.HasTraffic() {
		t.Error("new tracker should not have traffic")
	}

	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	if !tr.HasTraffic() {
		t.Error("should have traffic after process")
	}

	tr.FlushTraffic()
	if tr.HasTraffic() {
		t.Error("should not have traffic after flush")
	}
}

func TestProcess_NoTrafficDelta(t *testing.T) {
	tr := New()

	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	tr.FlushTraffic() // clear

	// Same values — no delta
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	if tr.HasTraffic() {
		t.Error("expected no traffic for zero delta")
	}
}

func TestTrafficAccumulation(t *testing.T) {
	tr := New()

	// Tick 1
	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 1)
	// Tick 2 (cumulative 300, 500 → delta 200, 300)
	tr.Process(map[int][2]int64{1: {300, 500}}, nil, 1)
	// Tick 3 (cumulative 350, 600 → delta 50, 100)
	tr.Process(map[int][2]int64{1: {350, 600}}, nil, 1)

	flushed := tr.FlushTraffic()
	// Total: 100+200+50 = 350 upload, 200+300+100 = 600 download
	if flushed[1] != [2]int64{350, 600} {
		t.Errorf("accumulated: got %v, want [350,600]", flushed[1])
	}
}
