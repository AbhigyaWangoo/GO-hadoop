package gossiputils

import (
	"fmt"
	"io/ioutil"
	"math/big"
	"sort"
	"sync"

	"crypto/rand"
	"os"
	"strings"

	cmap "github.com/orcaman/concurrent-map/v2"
)

const (
	ALIVE     = 0
	SUSPECTED = 1
	DOWN      = 2
	LEFT      = 3
)

type SdfsNodeType int

const (
	LEADER     SdfsNodeType = 2
	SUB_LEADER SdfsNodeType = 1
	FOLLOWER   SdfsNodeType = 0
)

type Member struct {
	Ip                string
	Port              string
	CreationTimestamp int64
	HeartbeatCounter  int
	State             int
	Type              SdfsNodeType
}

const INTRODUCER_IP string = "172.22.158.162"
const GOSSIP_PORT string = "7000"
const MLIST_SIZE int = 20480
const ENABLE_SUSPICION_MSG = "enable"
const DISABLE_SUSPICION_MSG = "disable"
const NUM_LEADERS = 4
const MAX_INT64 = 9223372036854775807

const GOSSIP_K int = 2
const GOSSIP_SEND_T int64 = 2

const Tfail int64 = 1.5 * 1e9  // 5 seconds * 10^9 nanoseconds
const Tcleanup int64 = 1 * 1e9 // 1 second * 10^9 nanoseconds

var MembershipMap cmap.ConcurrentMap[string, Member]
var MembershipUpdateTimes cmap.ConcurrentMap[string, int64]
var FailureHandler cmap.ConcurrentMap[string, bool] // If IPs are in this set as True, that means the process is done rereplicating the date on that IP.
var Ip string
var MessageDropRate float32 = 0.0
var ENABLE_SUSPICION bool = false
var LogFile = GetLogFilePointer()

var GossipMutex sync.Mutex

// Returns most up to date member and if any update occurs and if any update needs to be made (if members have different heartbeats)
func CurrentMember(LocalMember Member, NewMember Member) (Member, bool) {
	if LocalMember.HeartbeatCounter < NewMember.HeartbeatCounter {
		return NewMember, false
	} else if LocalMember.HeartbeatCounter > NewMember.HeartbeatCounter {
		return LocalMember, false
	}
	return Member{}, true
}

// Returns max between two ints
func Max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func RandomNumInclusive() float32 {
	// Generate a random integer between 0 and 1000 (inclusive on both sides)
	randomInt, _ := rand.Int(rand.Reader, big.NewInt(1001))

	// Scale the random integer to a floating-point number between 0.0 and 1.0
	randomFloat := float64(randomInt.Int64()) / 1000.0
	return float32(randomFloat)
}

func RandomKIpAddrs() []string {
	keys := MembershipMap.Keys()

	if len(keys) < GOSSIP_K {
		return keys
	}

	min := 0
	max := len(keys) - 1

	// Generate k random IP addrs from membership list
	rv := make([]string, GOSSIP_K)
	for i := 0; i < GOSSIP_K; i++ {
		var val int64 = int64(max - min + 1)
		randomNum, _ := rand.Int(rand.Reader, big.NewInt(val))
		idx := randomNum.Int64() + int64(min)

		node, exists := MembershipMap.Get(keys[idx])
		if !exists {
			panic("Race condition in random k selection")
		}

		if keys[idx] == Ip || node.State == DOWN || node.State == LEFT { // skip a certain selection if it's down, has left, or is the current node
			i--
		} else {
			rv = append(rv, keys[idx])
		}
	}

	return rv
}

// For mp1 distributed grep setup
func GetMachineNumber() string {
	os.Chdir("../../cs425mps")
	fileData, err := ioutil.ReadFile("logs/machine.txt")
	if err != nil {
		fmt.Println("Error reading file:", err)
		return ""
	}

	num := strings.TrimSpace(string(fileData))
	return num
}

// get current pointer in logfile
func GetLogFilePointer() *os.File {
	machineNumber := GetMachineNumber()
	logFilePath := fmt.Sprintf("logs/machine.%s.log", machineNumber)
	logFile, _ := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	return logFile
}

// checks current machine's IP addr in gossip's MembershipMap, returns whether current machine is leader, subleader, or follower
func MachineType() SdfsNodeType {
	kleaders := GetKLeaders()
	leader := kleaders[0]
	thisIp := Ip
	myMember, ok := MembershipMap.Get(Ip)

	if thisIp == leader {
		// fmt.Println("_____I AM A LEADER____")

		if ok {
			myMember.Type = LEADER
		}

		MembershipMap.Set(Ip, myMember)
		return LEADER
	}

	for i := 1; i < len(kleaders); i++ {
		if thisIp == kleaders[i] {
			// fmt.Println("_____I AM A SUB LEADER____")
			if ok {
				myMember.Type = SUB_LEADER
			}

			MembershipMap.Set(Ip, myMember)
			return SUB_LEADER
		}
	}

	// fmt.Println("_____I AM A FOLLOWER____")
	if ok {
		myMember.Type = FOLLOWER
	}

	MembershipMap.Set(Ip, myMember)
	return FOLLOWER
}

func GetKLeaders() []string {

	allKeys := MembershipMap.Keys()
	allMembers := make([]Member, 0)

	for _, key := range allKeys {
		member, _ := MembershipMap.Get(key)
		if member.State != DOWN {
			allMembers = append(allMembers, member)
		}
	}
	sort.Slice(allMembers, func(i, j int) bool {
		return allMembers[i].CreationTimestamp < allMembers[j].CreationTimestamp
	})

	var kLeaders []string
	numLeaders := NUM_LEADERS
	for _, member := range allMembers {
		if numLeaders == 0 {
			break
		}
		kLeaders = append(kLeaders, member.Ip)
		numLeaders--
	}

	return kLeaders
}

func GetLeader() string {
	var oldestTime int64 = MAX_INT64
	var oldestMemberIp string

	allKeys := MembershipMap.Keys()
	for key := range allKeys {
		member, exist := MembershipMap.Get(allKeys[key])

		if exist && member.CreationTimestamp < int64(oldestTime) && member.State != DOWN {
			oldestMemberIp = member.Ip
			oldestTime = member.CreationTimestamp
		}
	}

	return oldestMemberIp
}
