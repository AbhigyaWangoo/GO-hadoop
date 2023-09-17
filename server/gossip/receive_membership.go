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

func DeserializeStruct(serializedData []byte) (map[string][]utils.Member, error) {
	var data map[string][]utils.Member
	buf := bytes.NewBuffer(serializedData)

	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

// With merge, only need to check if incomming member info has more recent data. If local member info has more data, changes will be reflected in push
func Merge(NewMemberInfo map[string][]utils.Member) {
	// Iterate through the incoming membership list
	for newMemberIp, newMemberList := range NewMemberInfo {
		// Check if the a node in the current membership list matches a found node in the incoming membership list
		if memberList, exists := utils.MembershipList[newMemberIp]; exists {
			// Call update membership to get most up to date information on node
			upToDateMember, newVersion := UpdateMembership(memberList[0], newMemberList[0])
			// If there is a new node, add it to history
			if newVersion {
				memberList = append([]utils.Member{upToDateMember}, memberList...)
			} else { // If there isn't just set latest node to most recent info
				memberList[0] = upToDateMember
			}
			// Set current membership list to most updated membership
			utils.MembershipList[newMemberIp] = memberList
		} else { // If its a new node not currently in the membership list
			// Update the local membership list's version history and update time
			utils.MembershipList[newMemberIp] = newMemberList
			utils.MembershipUpdateTimes[newMemberIp] = time.Now().Unix()
		}
	}

}

// Returns updated member and if updated member needs to be added to list. Member creation timestamp created on originating machine
func UpdateMembership(localMember utils.Member, newMember utils.Member) (utils.Member, bool) {
	// If both members are the same version of a node
	if localMember.CreationTimestamp == newMember.CreationTimestamp {
		// If either one of them is down, mark the local node as down and move on
		if utils.Max(localMember.State, newMember.State) == utils.DOWN {
			localMember.State = utils.DOWN
		} else { // Otherwise...
			// Find the current most up to date member by heartbeats
			upToDateMember := utils.CurrentMember(localMember, newMember)
			// If that member isn't the local member, update the local member
			if localMember != upToDateMember {
				localMember.HeartbeatCounter = upToDateMember.HeartbeatCounter
				localMember.State = newMember.State
				// Set that the node has been updated at the most recent local time
				utils.MembershipUpdateTimes[localMember.Ip] = time.Now().Unix()
			}
		}
		// Return the updated local member and that a new node doesn't needed to be added to the version history
		return localMember, false
	} else if localMember.CreationTimestamp < newMember.CreationTimestamp { // If the local version is lower than the new version, return that the new member needs to be added to the local version history
		// Update the local update time for the node
		utils.MembershipUpdateTimes[localMember.Ip] = time.Now().Unix()
		// Return that the incoming node is a new node version
		return newMember, true
	}
	// If the local node version is ahead of the external node version, then nothing needs to happen. Changes will be made to the network when current node pushes to other nodes
	return localMember, false
}

// This function keeps a udp socket open
func ListenForLists() {
	serverAddr, resolveErr := net.ResolveUDPAddr("udp", ":"+utils.GOSSIP_PORT)
	if resolveErr != nil {
		fmt.Println("Error resolving address:", resolveErr)
		os.Exit(1)
	}

	udpConn, listenErr := net.ListenUDP("udp", serverAddr)
	if listenErr != nil {
		fmt.Println("Error listening:", listenErr)
		os.Exit(1)
	}
	defer udpConn.Close()

	fmt.Println("gossip client is listening on", serverAddr)

	buffer := make([]byte, utils.MLIST_SIZE)
	for {
		// Read data from the UDP connection
		n, addr, err := udpConn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading from UDP connection:", err)
			continue
		}

		data := buffer[:n]

		newlist, errDeseriealize := DeserializeStruct(data)
		if errDeseriealize != nil {
			fmt.Println("Inbound data was not a membership list: ", errDeseriealize)
		} else {
			go Merge(newlist)
			fmt.Printf("Received data from %s: %s\n", addr, data)
		}
	}
}
