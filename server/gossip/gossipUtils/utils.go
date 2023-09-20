package gossiputils

import (
	"math/rand"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
)

const (
	ALIVE     = 0
	SUSPECTED = 1
	DOWN      = 2
)

type Member struct {
	Ip                string
	Port              string
	CreationTimestamp int64
	HeartbeatCounter  int
	State             int
}

const INTRODUCER_IP string = "172.22.158.162"
const GOSSIP_PORT string = "9998"
const MLIST_SIZE int = 20480
const ENABLE_SUSPICION bool = true

const GOSSIP_K int = 2
const GOSSIP_SEND_T int64 = 2

const Tfail int64 = 5 * 1e9    // 5 seconds * 10^9 nanoseconds
const Tcleanup int64 = 1 * 1e9 // 1 second * 10^9 nanoseconds

var MembershipMap cmap.ConcurrentMap[string, Member]
var MembershipUpdateTimes cmap.ConcurrentMap[string, int64]
var Ip string

// Returns most up to date member and if any update occurs and if any update needs to be made (if members have different heartbeats)
func CurrentMember(LocalMember Member, NewMember Member) (Member, bool) {
	if LocalMember.HeartbeatCounter < NewMember.HeartbeatCounter {
		return NewMember, false
	} else if LocalMember.HeartbeatCounter > NewMember.HeartbeatCounter {
		return LocalMember, false
	}
	return nil, true
}

// Returns max between two ints
func Max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func RandomKIpAddrs() []string {
	rand.Seed(time.Now().UnixNano())

	keys := MembershipMap.Keys()
	// keys := make([]string, 0, len(MembershipList.Items()))

	if len(keys) < GOSSIP_K {
		return keys
	}

	min := 0
	max := len(keys) - 1

	// Generate k random IP addrs from membership list
	rv := make([]string, GOSSIP_K)
	for i := 0; i < GOSSIP_K; i++ {
		idx := rand.Intn(max-min+1) + min

		node, exists := MembershipMap.Get(keys[idx]) 
		if !exists {
			panic("Race condition in random k selection")
		}
		
		if keys[idx] == Ip || node.State == DOWN {
			i--
		} else {
			rv = append(rv, keys[idx])
		}
	}

	return rv
}
