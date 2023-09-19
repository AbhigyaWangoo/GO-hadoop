package gossiputils

import (
	"math/rand"
	"time"
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
const ENABLE_SUSPICION bool = false

const GOSSIP_K int = 2
const GOSSIP_SEND_T int64 = 2

const Tfail = 5 * 1e9    // 5 seconds * 10^9 nanoseconds
const Tcleanup = 1 * 1e9 // 1 second * 10^9 nanoseconds

var MembershipList map[string]Member
var MembershipUpdateTimes map[string]int64
var Ip string

// Returns most up to date member and if any update occurs and if any update needs to be made (members have different heartbeats)
func CurrentMember(LocalMember Member, NewMember Member) Member {
	if LocalMember.HeartbeatCounter < NewMember.HeartbeatCounter {
		return NewMember
	}
	return LocalMember
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

	keys := make([]string, 0, len(MembershipList))
	for key := range MembershipList {
		keys = append(keys, key)
	}

	min := 0
	max := len(MembershipList) - 1

	// Generate k random IP addrs from membership list
	rv := make([]string, GOSSIP_K)
	for i := 0; i < GOSSIP_K; i++ {
		idx := rand.Intn(max-min+1) + min
		
		if keys[idx] == Ip {
			i--;
		} else {
			rv = append(rv, keys[idx])
		}
	}

	return rv
}
