package tracker

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

// defaultAliveResendAfter bounds how long FlushAliveIPs may suppress an
// unchanged alive-IP set before resending it anyway.
//
// The panel stores device state in Redis under a 300s TTL that is only
// refreshed when a report actually carries an "alive" payload. Without a
// periodic forced resend, a user whose IP set never changes stops being
// reported after the first flush and silently expires out of the device
// list — the longer a client stays connected, the more certainly it
// disappears.
//
// This value MUST stay well below the panel's TTL. It also has to absorb
// the phase offset between the report ticker (server_push_interval, 60s by
// default) and the device-report ticker (device_report_interval, 30s), so
// the worst-case refresh interval is roughly 2x this value's ticker
// granularity. 120s leaves a full 2x margin against the 300s TTL.
const defaultAliveResendAfter = 120 * time.Second

// snapshot is an immutable point-in-time view of tracker state.
// It is swapped atomically so readers never block writers.
type snapshot struct {
	traffic   map[int][2]int64        // userID → [upload, download] delta
	aliveIPs  map[int]map[string]bool // userID → set of source IPs
	online    map[int]int             // userID → distinct IP count
	connCount int
	inSpeed   int64
	outSpeed  int64
}

// Tracker computes per-user traffic deltas from cumulative counters
// provided by the kernel, and accumulates totals for panel reporting.
//
// Architecture: the kernel maintains per-user atomic counters and IP sets.
// Each tick, the service calls Process() with cumulative per-user traffic.
// Tracker computes deltas against the previous cycle's values — O(users),
// not O(connections).
//
// Thread safety: Process() acquires mu to update internal state, then
// atomically publishes a new snapshot. All read methods (Flush*,
// CurrentOnline, LogStats, *Speed) read the snapshot lock-free.
// This eliminates contention between the 10s Process tick and the 60s
// flush/push tick.
type Tracker struct {
	mu sync.Mutex

	// lastSeen stores the cumulative traffic from the previous Process() call.
	// Protected by mu — only written by Process.
	lastSeen map[int][2]int64 // userID → [upload, download] cumulative

	// pending accumulates deltas between flushes.
	// Protected by mu — written by Process, drained by FlushTraffic.
	pendingTraffic map[int][2]int64

	// live holds the current snapshot, swapped atomically.
	// Readers load this pointer without any lock.
	live atomic.Pointer[snapshot]

	// lastAliveIPsHash detects changes to avoid duplicate reports.
	lastAliveIPsHash string

	// lastAliveFlush records when FlushAliveIPs last handed out a payload.
	// Used to force a periodic resend so the panel's TTL keeps getting
	// refreshed even while the IP set is unchanged.
	lastAliveFlush time.Time

	// aliveResendAfter is the maximum suppression window for an unchanged
	// alive-IP set. See defaultAliveResendAfter.
	aliveResendAfter time.Duration
}

func New() *Tracker {
	t := &Tracker{
		lastSeen:         make(map[int][2]int64),
		pendingTraffic:   make(map[int][2]int64),
		aliveResendAfter: defaultAliveResendAfter,
	}
	// Publish initial empty snapshot.
	t.live.Store(&snapshot{
		traffic:  make(map[int][2]int64),
		aliveIPs: make(map[int]map[string]bool),
		online:   make(map[int]int),
	})
	return t
}

// Process computes per-user traffic deltas from cumulative kernel counters.
// Also stores alive IPs and connection count. O(users).
//
// This is the only writer to lastSeen and pendingTraffic.
// After computing deltas, it publishes a new snapshot for lock-free reads.
func (t *Tracker) Process(
	cumTraffic map[int][2]int64,
	kernelAliveIPs map[int]map[string]bool,
	connCount int,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var cycleIn, cycleOut int64

	for uid, cum := range cumTraffic {
		prev := t.lastSeen[uid]
		deltaUp := cum[0] - prev[0]
		deltaDown := cum[1] - prev[1]

		// Guard against counter reset (kernel restart).
		if deltaUp < 0 {
			deltaUp = cum[0]
		}
		if deltaDown < 0 {
			deltaDown = cum[1]
		}

		t.lastSeen[uid] = cum

		if deltaUp > 0 || deltaDown > 0 {
			cur := t.pendingTraffic[uid]
			cur[0] += deltaUp
			cur[1] += deltaDown
			t.pendingTraffic[uid] = cur

			cycleOut += deltaUp
			cycleIn += deltaDown
		}
	}

	// Compute online from alive IPs.
	online := make(map[int]int, len(kernelAliveIPs))
	for uid, ips := range kernelAliveIPs {
		online[uid] = len(ips)
	}

	// Publish new snapshot (readers will see this atomically).
	t.live.Store(&snapshot{
		traffic:   copyTrafficMap(t.pendingTraffic),
		aliveIPs:  kernelAliveIPs, // kernel provides fresh copy each tick
		online:    online,
		connCount: connCount,
		inSpeed:   cycleIn,
		outSpeed:  cycleOut,
	})
}

// FlushTraffic returns accumulated per-user traffic and resets the pending buffer.
// Lock-free: reads from live snapshot, then acquires mu only to drain.
func (t *Tracker) FlushTraffic() map[int][2]int64 {
	t.mu.Lock()
	data := t.pendingTraffic
	t.pendingTraffic = make(map[int][2]int64, len(data))
	t.mu.Unlock()
	return data
}

// RestoreTraffic adds traffic back (used when push to panel fails).
func (t *Tracker) RestoreTraffic(data map[int][2]int64) {
	t.mu.Lock()
	for uid, d := range data {
		cur := t.pendingTraffic[uid]
		cur[0] += d[0]
		cur[1] += d[1]
		t.pendingTraffic[uid] = cur
	}
	t.mu.Unlock()
}

// HasTraffic returns true if there is accumulated traffic to report.
func (t *Tracker) HasTraffic() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pendingTraffic) > 0
}

// FlushAliveIPs returns per-user alive IPs, or nil when the set is unchanged
// and the forced-resend window has not elapsed yet.
//
// The returned map is freshly allocated and owned by the caller: callers hand
// it to a background goroutine for JSON serialization, so it must not alias
// any state that a later flush would mutate.
func (t *Tracker) FlushAliveIPs() map[int][]string {
	s := t.live.Load()

	t.mu.Lock()
	defer t.mu.Unlock()

	currentHash := calcAliveIPsHash(s.aliveIPs)

	// Unchanged set: normally suppressed as a duplicate, but resend once the
	// window elapses so the panel keeps refreshing its device-state TTL.
	if currentHash == t.lastAliveIPsHash &&
		time.Since(t.lastAliveFlush) < t.aliveResendAfter {
		return nil
	}

	t.lastAliveIPsHash = currentHash
	t.lastAliveFlush = time.Now()

	out := make(map[int][]string, len(s.aliveIPs))
	for uid, ips := range s.aliveIPs {
		list := make([]string, 0, len(ips))
		for ip := range ips {
			list = append(list, ip)
		}
		out[uid] = list
	}

	return out
}

// calcAliveIPsHash computes a deterministic hash for change detection.
func calcAliveIPsHash(aliveIPs map[int]map[string]bool) string {
	if len(aliveIPs) == 0 {
		return ""
	}

	h := sha256.New()

	// Sort user IDs for consistent hashing
	userIDs := make([]int, 0, len(aliveIPs))
	for uid := range aliveIPs {
		userIDs = append(userIDs, uid)
	}
	sort.Ints(userIDs)

	for _, uid := range userIDs {
		ips := aliveIPs[uid]
		// Sort IPs for consistent hashing
		ipList := make([]string, 0, len(ips))
		for ip := range ips {
			ipList = append(ipList, ip)
		}
		sort.Strings(ipList)

		// Write user ID and IPs to hash
		h.Write([]byte{byte(uid >> 24), byte(uid >> 16), byte(uid >> 8), byte(uid)})
		for _, ip := range ipList {
			h.Write([]byte(ip))
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}

// CurrentOnline returns a snapshot copy of user_id → device count (distinct IPs).
// Lock-free: reads from live snapshot.
func (t *Tracker) CurrentOnline() map[int]int {
	s := t.live.Load()
	cp := make(map[int]int, len(s.online))
	for k, v := range s.online {
		cp[k] = v
	}
	return cp
}

// InvalidateAliveIPs clears the dedup state so the next FlushAliveIPs resends,
// used when a push to the panel fails.
//
// Alive IPs are state, not a delta: every Process tick republishes the kernel's
// current set into the live snapshot, so a failed report needs no data restored
// — only the duplicate-suppression lifted.
func (t *Tracker) InvalidateAliveIPs() {
	t.mu.Lock()
	t.lastAliveIPsHash = ""
	t.lastAliveFlush = time.Time{}
	t.mu.Unlock()
}

// LogStats logs current tracking statistics.
// Lock-free: reads from live snapshot.
func (t *Tracker) LogStats() {
	s := t.live.Load()
	nlog.TrackerStats(s.connCount, len(s.online))
}

// ActiveConnections returns the last observed active connection count.
// Lock-free: reads from live snapshot.
func (t *Tracker) ActiveConnections() int {
	return t.live.Load().connCount
}

// TotalConnections is deprecated — no longer tracked per-connection.
// Returns 0 for backward compatibility.
func (t *Tracker) TotalConnections() int64 {
	return 0
}

// InboundSpeed returns the last observed inbound (download) speed in bytes/second.
// Lock-free: reads from live snapshot.
func (t *Tracker) InboundSpeed() int64 {
	return t.live.Load().inSpeed / 10
}

// OutboundSpeed returns the last observed outbound (upload) speed in bytes/second.
// Lock-free: reads from live snapshot.
func (t *Tracker) OutboundSpeed() int64 {
	return t.live.Load().outSpeed / 10
}

// copyTrafficMap creates a shallow copy of the traffic map.
func copyTrafficMap(src map[int][2]int64) map[int][2]int64 {
	dst := make(map[int][2]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
