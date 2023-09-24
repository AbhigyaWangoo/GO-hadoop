package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"

	"gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip"
)

func SleepTillNextMinute() {
	// Get the current time
	now := time.Now()

	// Calculate the duration to the start of the next minute
	sleepDuration := time.Duration(60-now.Second()) * time.Second

	fmt.Printf("Sleeping for %v seconds...\n", sleepDuration.Seconds())

	// Sleep until the start of the next minute
	time.Sleep(sleepDuration)

	fmt.Println("Exiting at the start of the new minute!")
}

func main() {
	if len(os.Args) == 2 && strings.Contains(os.Args[1], "fail") {
		SleepTillNextMinute()
		fmt.Printf("Machine %s started at time: %d\n", utils.GetMachineNumber(), time.Now().UnixNano())
		go gossip.InitializeGossip()
		time.Sleep(time.Second * 30)
		fmt.Printf("Machine %s ending at time: %d\n", utils.GetMachineNumber(), time.Now().UnixNano())
		os.Exit(0)
	} else {
		gossip.InitializeGossip()
	}

	// for {
	// 	reader := bufio.NewReader(os.Stdin)
	// 	command, err := reader.ReadString('\n')
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	if strings.Contains(command, "list_mem") {
	// 		gossip.PrintMembership()
	// 	} else if strings.Contains(command, "list_self") {
	// 		if selfMember, ok := utils.MembershipMap.Get(utils.Ip); ok {
	// 			fmt.Printf("%d\n", selfMember.CreationTimestamp)
	// 		}
	// 	} else if strings.Contains(command, "leave") {
	// 		if member, ok := utils.MembershipMap.Get(utils.Ip); ok {
	// 			member.State = utils.LEFT
	// 			utils.MembershipMap.Set(utils.Ip, member)
	// 			// gossip.SendMembershipList()
	// 			time.Sleep(time.Second)
	// 		}
	// 		os.Exit(0)
	// 	} else if strings.Contains(command, "es") {
	// 		setSendingSuspicionFlip(true)
	// 	} else if strings.Contains(command, "ds") {
	// 		setSendingSuspicionFlip(false)
	// 	} else {
	// 		error_msg := `
	// 		Command not understood. Available commands are as follows:
	// 			list_mem # list the membership list
	// 			list_self # list this node's entry
	// 			leave # leave the network
	// 			<percentage from 0.0 -> 1.0> # induce a network drop rate
	// 			ds # Disable suspicion
	// 			es # Disable suspicion
	// 		`
	// 		float, err_parse := strconv.ParseFloat(command[:len(command)-1], 32)
	// 		if err_parse != nil {
	// 			fmt.Println(error_msg)
	// 		} else {
	// 			utils.MessageDropRate = float32(float)
	// 		}

	// 	}
	// }
}

// Run grep server in a seperate thread/proccess
// Initialize sender and reciever threads as well as thread counting nodes
