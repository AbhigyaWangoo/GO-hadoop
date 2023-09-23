package gossiputils

import (
	"fmt"
	"io/ioutil"
	"math/big"
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
const ENABLE_SUSPICION_MSG = "enable"
const DISABLE_SUSPICION_MSG = "disable"

const GOSSIP_K int = 2
const GOSSIP_SEND_T int64 = 2

const Tfail int64 = 1.5 * 1e9    // 5 seconds * 10^9 nanoseconds
const Tcleanup int64 = 1 * 1e9 // 1 second * 10^9 nanoseconds

var MembershipMap cmap.ConcurrentMap[string, Member]
var MembershipUpdateTimes cmap.ConcurrentMap[string, int64]
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
	// Seed the random number generator with the current time
	// rand.Seed(time.Now().UnixNano())

	// Generate a random integer between 0 and 1000 (inclusive on both sides)
	randomInt, _ := rand.Int(rand.Reader, big.NewInt(1001))

	// Scale the random integer to a floating-point number between 0.0 and 1.0
	randomFloat := float64(randomInt.Int64()) / 1000.0
	return float32(randomFloat)
}

func RandomKIpAddrs() []string {

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
		var val int64 = int64(max - min + 1)
		randomNum, _ := rand.Int(rand.Reader, big.NewInt(val))
		idx := randomNum.Int64() + int64(min)
		// idx := rand.Intn(max-min+1) + min

		node, exists := MembershipMap.Get(keys[idx])
		if !exists {
			panic("Race condition in random k selection")
		}

		if keys[idx] == Ip || node.State == DOWN || node.State == LEFT {
			i--
		} else {
			rv = append(rv, keys[idx])
		}
	}

	return rv
}

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

func GetLogFilePointer() *os.File {
	machineNumber := GetMachineNumber()
	logFilePath := fmt.Sprintf("logs/machine.%s.log", machineNumber)
	logFile, _ := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	return logFile
}
