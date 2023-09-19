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

func DeserializeStruct(serializedData []byte) (map[string]utils.Member, error) {
	var data map[string]utils.Member
	buf := bytes.NewBuffer(serializedData)

	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

// With merge, only need to check if incomming member info has more recent data. If local member info has more data, changes will be reflected in push
func Merge(NewMemberInfo map[string]utils.Member) {


	// Iterate through the incoming membership list
	for newMemberIp, newMemberVersion := range NewMemberInfo {

		// Check if the a node in the current membership list matches a found node in the incoming membership list
		if localMemberVersion, exists := utils.MembershipList[newMemberIp]; exists {

			// Call update membership to get most up to date information on node
			upToDateMember := UpdateMembership(localMemberVersion, newMemberVersion)

			// Set current membership list to most updated node membership
			utils.MembershipList[newMemberIp] = upToDateMember
		} else { // If its a new node not currently in the membership list
			// Update the local membership list's version history and update time
			utils.MembershipList[newMemberIp] = newMemberVersion
			utils.MembershipUpdateTimes[newMemberIp] = time.Now().Unix()
		}
	}

}

// Returns updated member and if updated member needs to be added to list. Member creation timestamp created on originating machine
func UpdateMembership(localMember utils.Member, newMember utils.Member) utils.Member {
	// If both members are the same version of a node
	if localMember.CreationTimestamp == newMember.CreationTimestamp {
		// Find the current most up to date member by heartbeats
		upToDateMember := utils.CurrentMember(localMember, newMember)
		// If that member isn't the local member, update the local member
		if localMember != upToDateMember {
			localMember.HeartbeatCounter = upToDateMember.HeartbeatCounter
			localMember.State = utils.ALIVE
			// Set that the node has been updated at the most recent local time
			utils.MembershipUpdateTimes[localMember.Ip] = time.Now().Unix()
		}
		// Return the updated local member and that a new node doesn't needed to be added to the version history
		return localMember
	} else if localMember.CreationTimestamp < newMember.CreationTimestamp { // If the local version is lower than the new version, return that the new member needs to be added to the local version history
		// Update the local update time for the node
		utils.MembershipUpdateTimes[localMember.Ip] = time.Now().Unix()
		newMember.State = utils.ALIVE
		// Return that the incoming node is a new node version
		return newMember
	}
	// If the local node version is ahead of the external node version, then nothing needs to happen. Changes will be made to the network when current node pushes to other nodes
	return localMember
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
		n, _, err := udpConn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading from UDP connection:", err)
			continue
		}

		data := buffer[:n]

		newlist, errDeseriealize := DeserializeStruct(data)
		if errDeseriealize != nil {
			fmt.Println("Inbound data was not a membership list: ", errDeseriealize)
		} else {
			Merge(newlist)
			
			// for member := range utils.MembershipList {
			// 	fmt.Println("Member string: ", MemberPrint(utils.MembershipList[member]))
			// }
			fmt.Println("Member length: ", len(utils.MembershipList))
		}
	}
}
