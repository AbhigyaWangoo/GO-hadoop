package gossiputils

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

var MembershipList map[string][]Member
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
