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
	"gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfs_client "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
)

type CLICommand string

const (
	PUT       CLICommand = "put"
	GET       CLICommand = "get"
	DELETE    CLICommand = "delete"
	LS        CLICommand = "ls"
	STORE     CLICommand = "store"
	LIST_MEM  CLICommand = "list_mem"
	LIST_SELF CLICommand = "list_self"
	LEAVE     CLICommand = "leave"
	EN_SUS    CLICommand = "enable_sus"
	D_SUS     CLICommand = "disable_sus"
	MULTIREAD CLICommand = "multiread"
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

func RunCLI() {
	for {
		reader := bufio.NewReader(os.Stdin) // Our reader to handle userinputted commands
		command, err := reader.ReadString('\n')
		fmt.Println(command)
		if err != nil {
			log.Fatal(err)
		}

		if strings.Contains(command, string(LIST_MEM)) {
			gossip.PrintMembership()
		} else if strings.Contains(command, string(LIST_SELF)) {
			if selfMember, ok := utils.MembershipMap.Get(utils.Ip); ok {
				fmt.Printf("%d\n", selfMember.CreationTimestamp)
			}
		} else if strings.Contains(command, string(LEAVE)) {
			if member, ok := utils.MembershipMap.Get(utils.Ip); ok {
				member.State = utils.LEFT
				utils.MembershipMap.Set(utils.Ip, member)
				time.Sleep(time.Second)
			}
			os.Exit(0)
		} else if strings.Contains(command, string(EN_SUS)) {
			setSendingSuspicionFlip(true)
		} else if strings.Contains(command, string(D_SUS)) {
			setSendingSuspicionFlip(false)
		} else if strings.Contains(command, string(PUT)) {
			parts := strings.Split(command, " ")
			localfilename := strings.TrimSpace(parts[1])
			sdfs_filename := strings.TrimSpace(parts[2])

			locations, locationErr := sdfs_client.SdfsClientMain(sdfs_filename)
			if locationErr != nil {
				fmt.Println("Error with sdfsclient main. Aborting Put command: ", locationErr)
				return
			}

			if len(locations) != 0 {
				time.Sleep(time.Second)
				sdfs_client.InitiateDeleteCommand(sdfs_filename, locations)
	
				locations, locationErr = sdfs_client.SdfsClientMain(sdfs_filename)
				if locationErr != nil {
					fmt.Println("Error with sdfsclient main. Aborting Put command: ", locationErr)
					return
				}
				fmt.Println("mappings detected after delete: ", locations)
			}

			sdfs_client.InitiatePutCommand(localfilename, sdfs_filename)

		} else if strings.Contains(command, string(GET)) {
			parts := strings.Split(command, " ")
			localfilename := strings.TrimSpace(parts[1])
			sdfs_filename := strings.TrimSpace(parts[2])

			locations, locationErr := sdfs_client.SdfsClientMain(sdfs_filename)
			if locationErr != nil {
				fmt.Println("Error with sdfsclient main. Aborting Get command: ", locationErr)
				return
			}

			sdfs_client.InitiateGetCommand(sdfs_filename, localfilename, locations)

		} else if strings.Contains(command, string(DELETE)) {
			parts := strings.Split(command, " ")
			sdfs_filename := strings.TrimSpace(parts[1])

			mappings, mappingsErr := sdfs_client.SdfsClientMain(sdfs_filename)
			if mappingsErr != nil {
				fmt.Println("Error with sdfsclient main. Aborting Get command: ", mappingsErr)
				return
			}

			sdfs_client.InitiateDeleteCommand(sdfs_filename, mappings)

		} else if strings.Contains(command, string(LS)) {
			parts := strings.Split(command, " ")
			sdfs_filename := strings.TrimSpace(parts[1])

			mappings, mappingsErr := sdfs_client.SdfsClientMain(sdfs_filename)
			if mappingsErr != nil {
				fmt.Println("Error with sdfsclient main. Aborting Get command: ", mappingsErr)
				return
			}

			sdfs_client.InitiateLsCommand(sdfs_filename, mappings)

		} else if strings.Contains(command, string(STORE)) {
			sdfs_client.InitiateStoreCommand()
		} else if strings.Contains(command, string(MULTIREAD)) {
			parts := strings.Split(command, " ")
			for i, part := range parts {
				part = strings.TrimSpace(part)
				parts[i] = part
			}
			sdfs_client.InitiateMultiRead(parts[1], parts[2:])
		} else {
			error_msg := `
			Command not understood. Available commands are as follows:
				_____________________________________________________
				GOSSIP COMMANDS:
				list_mem # list the membership list
				list_self # list this node's entry
				leave # leave the network
				<percentage from 0.0 -> 1.0> # induce a network drop rate 
				ds # Disable suspicion
				es # Disable suspicion
				_____________________________________________________
				_____________________________________________________
				SDFS COMMANDS:
				put <localfilename> <sdfs_filename> # put a file from your local machine into sdfs
				get <sdfs_filename> <localfilename> # get a file from sdfs and write it to local machine
				delete <sdfs_filename> # delete a file from sdfs
				ls sdfs_filename # list all vm addresses where the file is stored
				store # at this machine, list all files paritally or fully stored at this machine
				_____________________________________________________
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

func main() {
	go gossip.InitializeGossip()
	go sdfs.InitializeSdfsProcess()

	RunCLI()
}
