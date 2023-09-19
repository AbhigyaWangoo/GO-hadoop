package gossip

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"time"

	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
)

func SerializeStruct(data map[string]utils.Member) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	if err := encoder.Encode(data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func PingServer(ServerIpAddr string) {
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

	// Data to send
	message, errDeseriealize := SerializeStruct(utils.MembershipList)
	if errDeseriealize != nil {
		panic(err)
	}

	// Send the data
	_, err = conn.Write(message)
	if err != nil {
		fmt.Println("Error sending data:", err)
		os.Exit(1)
	}

	fmt.Println("Data sent to", serverAddr)
}

func SendMembershipList() {
	// IN A WHILE LOOP, CONSTANTLY SEND YOUR MEMBERSHIP LIST TO K RANDOM ADDRESSES IN THE SUBNET. Need to increment hearbeats every time we send data

	for {
		// 1. Select k ip addrs, and send mlist to each
		ipAddrs := utils.RandomKIpAddrs()
		
		for ipAddr := range ipAddrs {
			if ipAddrs[ipAddr] != utils.Ip {
				PingServer(ipAddrs[ipAddr])
			}
		}

		// 2. Sleep for x nanoseconds
		time.Sleep((time.Second / 2))
		fmt.Println("am i running")
	}
}
