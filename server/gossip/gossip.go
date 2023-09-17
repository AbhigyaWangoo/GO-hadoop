package gossip

import (
	"fmt"
	"log"
	"net"
	"time"

	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
)

func main() {
	utils.Ip = GetOutboundIP().String()
	timestamp := time.Now().Unix()

	newMember := utils.Member{
		Ip:                utils.Ip,
		Port:              utils.GOSSIP_PORT,
		CreationTimestamp: timestamp,
		HeartbeatCounter:  0,
		State:             utils.ALIVE,
	}
	utils.MembershipList[utils.Ip] = make([]utils.Member, 1)
	utils.MembershipList[utils.Ip] = append(utils.MembershipList[utils.Ip], newMember)
	utils.MembershipUpdateTimes[utils.Ip] = timestamp

	// for member := range membershipList {
	// 	fmt.Println("Member string: ", MemberPrint(membershipList[member]))
	// }

	// PingIntroducer() I SHOULD ONLY RUN IF THIS MACHINE IS THE INTRODUCER MACHINE
	// ListenForLists() // TODO RUN ME IN A THREAD
	// go SendMembershipList()
}

func MemberPrint(m utils.Member) string {
	return fmt.Sprintf("IP: %s, Port: %s, Timestamp: %d, State: %d", m.Ip, m.Port, m.CreationTimestamp, m.State)
}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// Check if nodes need to be degraded from ALIVE or DOWN statuses
func PruneNodeMembers() {
	// Go through all currently stored nodes and check their lastUpdatedTimes
	for nodeIp, lastUpdateTime := range utils.MembershipUpdateTimes {
		// If the time elasped since last updated is greater than 6 (Tfail + Tcleanup), mark node as DOWN
		if time.Now().Unix()-lastUpdateTime >= 6 {
			utils.MembershipList[nodeIp][0].State = utils.DOWN
		} else if time.Now().Unix()-lastUpdateTime >= 5 { // If the time elasped since last updated is greater than 5 (Tfail), mark node as SUSPECTED
			utils.MembershipList[nodeIp][0].State = utils.SUSPECTED
		} else {
			utils.MembershipList[nodeIp][0].State = utils.ALIVE
		}
	}
}

// The new flow is as following:
// 1. A node when coming online, creates a membership list with just it's data and sends that to the introducer
// 2. The introducer node treats the connection just like any other node. With the merge, the introducer is now aware of the new node.
// 3. Start the listening for lists to listen for udp connections from the socket in a thread
// 4. start a thread that every t seconds sends its membership list to k machines.
