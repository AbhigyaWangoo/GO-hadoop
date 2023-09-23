package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
)

// Send suspicion flip message to all machines
func setSendingSuspicionFlip(enable bool) {
	// utils.GossipMutex.Lock()
	utils.SendingSuspicionMessages = true
	utils.ENABLE_SUSPICION = enable
	// utils.GossipMutex.Unlock()

	// Send enable messages to all nodes
	for info := range utils.MembershipMap.IterBuffered() {
		nodeIp, _ := info.Key, info.Val

		if enable { // 172.22.158.162
			gossip.PingServer(nodeIp, utils.ENABLE_SUSPICION_MSG)
		} else {
			gossip.PingServer(nodeIp, utils.DISABLE_SUSPICION_MSG)
		}
	}

	// time.Sleep(2) // sleep for 2 seconds to allow messages to propagate in network
}

func main() {

	go gossip.InitializeGossip()

	for {
		reader := bufio.NewReader(os.Stdin)
		command, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		if strings.Contains(command, "list_mem") {
			gossip.PrintMembership()
		} else if strings.Contains(command, "list_self") {
			if selfMember, ok := utils.MembershipMap.Get(utils.Ip); ok {
				fmt.Printf("%d\n", selfMember.CreationTimestamp)
			}
		} else if strings.Contains(command, "leave") {
			if member, ok := utils.MembershipMap.Get(utils.Ip); ok {
				member.State = utils.LEFT
				utils.MembershipMap.Set(utils.Ip, member)
				// gossip.SendMembershipList()
				time.Sleep(time.Second)
			}
			os.Exit(0)
		} else if strings.Contains(command, "es") {
			// TODO implement
			setSendingSuspicionFlip(true)
		} else if strings.Contains(command, "ds") {
			// TODO implement
			setSendingSuspicionFlip(false)
		} else {
			error_msg := `
			Command not understood. Available commands are as follows:
				list_mem # list the membership list
				list_self # list this node's entry
				leave # leave the network
				<percentage from 0.0 -> 1.0> # induce a network drop rate 
			`
			float, err_parse := strconv.ParseFloat(command, 32)
			if err_parse != nil {
				fmt.Println(error_msg)
			} else {
				utils.MessageDropRate = float32(float)
			}

		}
	}
}

// Run grep server in a seperate thread/proccess
// Initialize sender and reciever threads as well as thread counting nodes
