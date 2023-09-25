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
	utils.ENABLE_SUSPICION = enable

	// Send enable messages to all nodes
	for info := range utils.MembershipMap.IterBuffered() {
		nodeIp, _ := info.Key, info.Val

		if enable { // if enabling, suspicion, send a certain string. else, send a differnt one
			gossip.PingServer(nodeIp, utils.ENABLE_SUSPICION_MSG)
		} else {
			gossip.PingServer(nodeIp, utils.DISABLE_SUSPICION_MSG)
		}
	}
}

func main() {

	go gossip.InitializeGossip()

	for {
		reader := bufio.NewReader(os.Stdin) // Our reader to handle userinputted commands
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
				time.Sleep(time.Second)
			}
			os.Exit(0)
		} else if strings.Contains(command, "es") {
			setSendingSuspicionFlip(true)
		} else if strings.Contains(command, "ds") {
			setSendingSuspicionFlip(false)
		} else {
			error_msg := `
			Command not understood. Available commands are as follows:
				list_mem # list the membership list
				list_self # list this node's entry
				leave # leave the network
				<percentage from 0.0 -> 1.0> # induce a network drop rate 
				ds # Disable suspicion
				es # Disable suspicion
			`
			float, err_parse := strconv.ParseFloat(command[:len(command)-1], 32)
			if err_parse != nil {
				fmt.Println(error_msg)
			} else {
				utils.MessageDropRate = float32(float)
			}

		}
	}
}