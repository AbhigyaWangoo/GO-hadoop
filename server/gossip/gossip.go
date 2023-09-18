package gossip

import (
	"fmt"
	"log"
	"net"
	"time"

	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
)

func InitializeGossip() {
	utils.Ip = GetOutboundIP().String()
	timestamp := time.Now().UnixNano()

	newMember := utils.Member{
		Ip:                utils.Ip,
		Port:              utils.GOSSIP_PORT,
		CreationTimestamp: timestamp,
		HeartbeatCounter:  0,
		State:             utils.ALIVE,
	}

	// We can take older versions out, need to worry about false positives
	// Index by hostname

	utils.MembershipList[utils.Ip] = newMember
	utils.MembershipUpdateTimes[utils.Ip] = timestamp

	for member := range utils.MembershipList {
		fmt.Println("Member string: ", MemberPrint(utils.MembershipList[member]))
	}
	ch := make(chan struct{})

	if utils.Ip != utils.INTRODUCER_IP {
		PingServer(utils.INTRODUCER_IP)
	}
	go ListenForLists()
	go SendMembershipList()

	<-ch // infinite waiting, todo is this the best way to do this?

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
		if utils.ENABLE_SUSPICION && time.Now().UnixNano()-lastUpdateTime >= utils.Tfail+utils.Tcleanup {
			if node, ok := utils.MembershipList[nodeIp]; ok {
				node.State = utils.DOWN
				utils.MembershipList[nodeIp] = node
			}
		} else if time.Now().UnixNano()-lastUpdateTime >= utils.Tfail { // If the time elasped since last updated is greater than 5 (Tfail), mark node as SUSPECTED
			if node, ok := utils.MembershipList[nodeIp]; ok {
				if utils.ENABLE_SUSPICION {
					node.State = utils.SUSPECTED
				} else {
					node.State = utils.DOWN
				}
				utils.MembershipList[nodeIp] = node
			}
		} else {
			if node, ok := utils.MembershipList[nodeIp]; ok {
				node.State = utils.ALIVE
				utils.MembershipList[nodeIp] = node
			}
		}
	}
}

// The new flow is as following:
// 1. A node when coming online, creates a membership list with just it's data and sends that to the introducer
// 2. The introducer node treats the connection just like any other node. With the merge, the introducer is now aware of the new node.
// 3. Start the listening for lists to listen for udp connections from the socket in a thread
// 4. start a thread that every t seconds sends its membership list to k machines.
