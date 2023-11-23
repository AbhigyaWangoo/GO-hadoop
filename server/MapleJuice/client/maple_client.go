package maplejuice

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"

	mapleutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfsclient "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitiateMaplePhase(LocalExecFile string, NMaples uint32, SdfsPrefix string, SdfsSrcDataset string) {

	locations, locationErr := sdfsclient.SdfsClientMain(SdfsSrcDataset)
	if locationErr != nil {
		fmt.Println("Error with sdfsclient main. Aborting Get command: ", locationErr)
		return
	}

	sdfsclient.InitiateGetCommand(SdfsSrcDataset, SdfsSrcDataset, locations)
	mapleIps := getMapleIps(NMaples)

	file, err := os.Open(SdfsSrcDataset)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var ipsToConnections map[string]net.Conn
	nodeDesignation := uint32(0)
	for scanner.Scan() {
		line := scanner.Text()
		ip := getPartitionIp(line, mapleIps, NMaples)
		conn, exists := ipsToConnections[ip]
		if !exists {
			conn, _ = sdfsutils.OpenTCPConnection(ip, mapleutils.MAPLE_JUICE_PORT)
			ipsToConnections[ip] = conn
			mapleTask := mapleutils.MapleJuiceTask{
				Type:            mapleutils.MAPLE,
				NodeDesignation: nodeDesignation,
				SdfsPrefix:      SdfsPrefix,
				SdfsExecFile:    LocalExecFile,
			}

			conn.Write(mapleTask.Marshal())
			conn.Write([]byte{'\n'})

			nodeDesignation++
		}
		conn.Write([]byte(line))
	}

	for _, conn := range ipsToConnections {
		conn.Close()
	}

	// Initiates the Maple phase via client command

	// 1. GET SdfsSrcDataset -> dataset.txt
	// 2. get an array of NMaples IPs from gossip memlist, call it MapleDsts
	// 3. For each line in dataset.txt,
	// 		4. i = hash(line) % NMaples. Also, need to save in append mode this line into a seperate file for later sending
	// 		5. SendMapleTask(MapleDsts[i], CreatedTask) // Need to think about this carefully
}

func getMapleIps(nMaples uint32) []string {
	kRandomIpAddrs := gossiputils.RandomKIpAddrs(int(nMaples))
	return kRandomIpAddrs
}

func getPartitionIp(key string, ips []string, nMaples uint32) string {
	numPartitions := uint32(len(ips)) //
	return ips[hashFunction(key, numPartitions)]
}

func hashFunction(key string, numPartitions uint32) uint32 {
	hash := crc32.ChecksumIEEE([]byte(key))
	return uint32(hash) % numPartitions
}
