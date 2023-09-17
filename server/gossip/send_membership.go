package gossip

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"

	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
)

func SerializeStruct(data map[string][]utils.Member) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	if err := encoder.Encode(data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func PingIntroducer() {
	serverAddr, err := net.ResolveUDPAddr("udp", utils.INTRODUCER_IP+":"+utils.GOSSIP_PORT)
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
	// IN A WHILE LOOP, CONSTANTLY SEND YOUR MEMBERSHIP LIST TO K RANDOM ADDRESSES IN THE SUBNET
}
