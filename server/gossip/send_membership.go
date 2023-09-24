package gossip

import (
	"fmt"
	"net"
	"os"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
)

func SerializeStruct(data cmap.ConcurrentMap[string, utils.Member]) ([]byte, error) {
	// var buf bytes.Buffer
	// encoder := gob.NewEncoder(&buf)
	json, errMarshal := data.MarshalJSON()
	utils.BandWidth += len(json)

	// if err := encoder.Encode(json); err != nil {
	// return nil, err
	// }

	return json, errMarshal
}

func PingServer(ServerIpAddr string, suspicionMessage string) {
	serverAddr, err := net.ResolveUDPAddr("udp", ServerIpAddr+":"+utils.GOSSIP_PORT)
	if err != nil {
		fmt.Println("Error resolving server address:", err)
		os.Exit(1)
	}

	// Create a UDP connection
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error creating UDP connection:", err)
		os.Exit(1)
	}
	defer conn.Close()

	if node, ok := utils.MembershipMap.Get(utils.Ip); ok && node.State != utils.LEFT {
		node.HeartbeatCounter += 1
		node.State = utils.ALIVE
		utils.MembershipMap.Set(utils.Ip, node)
	}

	var msg []byte
	if len(suspicionMessage) == 0 {
		// Data to send
		message, errDeseriealize := SerializeStruct(utils.MembershipMap)
		if errDeseriealize != nil {
			panic(err)
		}
		msg = message
	} else {
		msg = []byte(suspicionMessage)
	}

	// Send the data
	_, err = conn.Write(msg)
	if err != nil {
		fmt.Println("Error sending data:", err)
		os.Exit(1)
	}

	// fmt.Println("Data sent to", serverAddr)
}

func SendMembershipList() {
	// IN A WHILE LOOP, CONSTANTLY SEND YOUR MEMBERSHIP LIST TO K RANDOM ADDRESSES IN THE SUBNET. Need to increment hearbeats every time we send data

	for {
		// 1. Select k ip addrs, and send mlist to each
		ipAddrs := utils.RandomKIpAddrs()

		for ipAddr := range ipAddrs {
			if ipAddrs[ipAddr] != utils.Ip {
				PingServer(ipAddrs[ipAddr], "")
			}
		}

		// 2. Sleep for x nanoseconds
		time.Sleep((time.Second / 5))
	}
}
