package main

import "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip"

// import (
// 	distributedGrepServer "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/distributedGrepServer"
// )

// func main() {
// 	distributedGrepServer.InitializeServer()
// }

func main() {
	// addrs, err := net.InterfaceAddrs()
	// if err != nil {
	// 	fmt.Println("Error:", err)
	// 	return
	// }

	// for _, addr := range addrs {
	// 	if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
	// 		if ipnet.IP.To4() != nil {
	// 			fmt.Println("Local IP address:", ipnet.IP.String())
	// 			return
	// 		}
	// 	}
	// }
	gossip.InitializeGossip()
}

// Run grep server in a seperate thread/proccess
// Initialize sender and reciever threads as well as thread counting nodes
