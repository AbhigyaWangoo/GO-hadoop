package maplejuice

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"path/filepath"

	mapleutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitiateMaplePhase(LocalExecFile string, nMaples uint32, SdfsPrefix string, SdfsSrcDataset string) {

	// locations, locationErr := sdfsclient.SdfsClientMain(SdfsSrcDataset)
	// if locationErr != nil {
	// 	fmt.Println("Error with sdfsclient main. Aborting Get command: ", locationErr)
	// 	return
	// }

	ipsToConnections := make(map[string]net.Conn)

	// sdfsclient.InitiateGetCommand(SdfsSrcDataset, SdfsSrcDataset, locations)
	mapleIps := getMapleIps(nMaples)

	entries, err := os.ReadDir(SdfsSrcDataset)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	for _, entry := range entries {
		fileName := filepath.Join(SdfsSrcDataset, entry.Name())

		if entry.IsDir() {
			continue
		}

		fp := mapleutils.OpenFile(fileName, os.O_RDONLY)
		defer fp.Close()

		ipsToConnections = sendAllLinesInAFile(mapleIps, ipsToConnections, fp, nMaples, SdfsPrefix, LocalExecFile)
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

func sendAllLinesInAFile(mapleIps []string, ipsToConnections map[string]net.Conn, fp *os.File, nMaples uint32, sdfsPrefix, localExecFile string) map[string]net.Conn {
	scanner := bufio.NewScanner(fp)

	nodeDesignation := uint32(0)
	for scanner.Scan() {
		line := scanner.Text()
		ip := getPartitionIp(line, mapleIps)
		conn, exists := ipsToConnections[ip]
		if !exists {
			log.Printf(ip)
			conn, _ = sdfsutils.OpenTCPConnection(ip, mapleutils.MAPLE_JUICE_PORT)
			ipsToConnections[ip] = conn
			mapleTask := mapleutils.MapleJuiceTask{
				Type:            mapleutils.MAPLE,
				NodeDesignation: nodeDesignation,
				SdfsPrefix:      sdfsPrefix,
				SdfsExecFile:    localExecFile,
				NumberOfMJTasks: nMaples,
			}

			conn.Write(mapleTask.Marshal())
			conn.Write([]byte{'\n'})

			nodeDesignation++
			sdfsutils.ReadSmallAck(conn)
		}
		conn.Write([]byte(line))
		conn.Write([]byte{'\n'})
		// log.Println(line)
	}
	return ipsToConnections
}

func getMapleIps(nMaples uint32) []string {
	kRandomIpAddrs := gossiputils.RandomKIpAddrs(int(nMaples), true)
	return kRandomIpAddrs
}

func getPartitionIp(key string, ips []string) string {
	// log.Println(ips[0], ips[1])
	numPartitions := uint32(len(ips))
	return ips[hashFunction(key, numPartitions)]
}

func hashFunction(key string, numPartitions uint32) uint32 {
	hash := crc32.ChecksumIEEE([]byte(key))
	return uint32(hash) % numPartitions
}

// maple map_executable 1 prefix mapTestDir
