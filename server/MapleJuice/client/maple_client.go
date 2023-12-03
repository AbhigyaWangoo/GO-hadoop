package maplejuice

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"log"
	"math/big"
	"net"
	"os"

	mapleutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfsfuncs "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitiateMaplePhase(LocalExecFile string, nMaples uint32, SdfsPrefix string, SdfsSrcDataset string, ExecFileArgs []string) {

	// locations, locationErr := sdfsclient.SdfsClientMain(SdfsSrcDataset)
	// if locationErr != nil {
	// 	fmt.Println("Error with sdfsclient main. Aborting Get command: ", locationErr)
	// 	return
	// }
	ipsToConnections := make(map[string]net.Conn)

	// sdfsclient.InitiateGetCommand(SdfsSrcDataset, SdfsSrcDataset, locations)
	mapleIps := getMapleIps(nMaples)

	mapleTask := mapleutils.MapleJuiceTask{
		Type:              mapleutils.MAPLE,
		SdfsPrefix:        SdfsPrefix,
		SdfsExecFile:      LocalExecFile,
		NumberOfMJTasks:   nMaples,
		ExecFileArguments: ExecFileArgs,
	}

	sdfsFileNames := sdfsfuncs.InitiateLsWithPrefix(SdfsSrcDataset)
	for _, sdfsFile := range sdfsFileNames {
		blockLocations, locationErr := sdfsfuncs.SdfsClientMain(sdfsFile, true)
		if locationErr != nil {
			fmt.Println("Error with sdfsclient main. Aborting Get command: ", locationErr)
			return
		}
		log.Println(sdfsFile)
		randomHash, _ := generateRandomHash()
		sdfsfuncs.InitiateGetCommand(sdfsFile, randomHash+sdfsFile, blockLocations)

		fp := mapleutils.OpenFile(randomHash+sdfsFile, os.O_RDONLY)
		defer fp.Close()

		ipsToConnections = sendAllLinesInAFile(mapleIps, ipsToConnections, fp, mapleTask)

	}

	for _, conn := range ipsToConnections {
		conn.Close()
		fmt.Println("Closed connection")
	}

	tcpConn, listenError := sdfsutils.ListenOnTCPConnection(mapleutils.MAPLE_JUICE_ACK_PORT)
	if listenError != nil {
		fmt.Printf("Error listening on port %s", mapleutils.MAPLE_JUICE_ACK_PORT)
		return
	}
	defer tcpConn.Close()

	fmt.Println("maplejuiceack client is listening on local machine")

	numAcksRecieved := uint32(0)
	for {
		if numAcksRecieved == nMaples {
			break
		}
		// Read data from the TCP connection
		conn, err := tcpConn.Accept()
		// conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

		if err != nil {
			fmt.Println("Error accepting tcp connection:", err)
			continue
		}

		numAcksRecieved++
		conn.Close()
	}

	// Initiates the Maple phase via client command

	// 1. GET SdfsSrcDataset -> dataset.txt
	// 2. get an array of NMaples IPs from gossip memlist, call it MapleDsts
	// 3. For each line in dataset.txt,
	// 		4. i = hash(line) % NMaples. Also, need to save in append mode this line into a seperate file for later sending
	// 		5. SendMapleTask(MapleDsts[i], CreatedTask) // Need to think about this carefully
}

func sendAllLinesInAFile(mapleIps []string, ipsToConnections map[string]net.Conn, fp *os.File, mapleTask mapleutils.MapleJuiceTask) map[string]net.Conn {
	scanner := bufio.NewScanner(fp)

	nodeDesignation := uint32(0)
	numlines := 0
	for scanner.Scan() {
		line := scanner.Text()
		ip := getPartitionIp(line, mapleIps)
		conn, exists := ipsToConnections[ip]
		if !exists {
			log.Printf(ip)
			conn, _ = sdfsutils.OpenTCPConnection(ip, mapleutils.MAPLE_JUICE_PORT)
			ipsToConnections[ip] = conn

			mapleTask.NodeDesignation = nodeDesignation
			conn.Write(mapleTask.Marshal())
			conn.Write([]byte{'\n'})

			nodeDesignation++
			sdfsutils.ReadSmallAck(conn)
		}
		conn.Write([]byte(line))
		conn.Write([]byte{'\n'})
		numlines += 1
	}
	fmt.Printf("Read %d lines in maple task\n", numlines)
	return ipsToConnections
}

func getMapleIps(nMaples uint32) []string {
	kRandomIpAddrs := gossiputils.RandomKIpAddrs(int(nMaples), true)
	return kRandomIpAddrs
}

func getPartitionIp(key string, ips []string) string {
	numPartitions := uint32(len(ips))
	return ips[hashFunction(key, numPartitions)]
}

func hashFunction(key string, numPartitions uint32) uint32 {
	hash := crc32.ChecksumIEEE([]byte(key))
	return uint32(hash) % numPartitions
}

// maple map_executable 1 prefix mapTestDir

func generateRandomHash() (string, error) {
	randomNum, err := rand.Int(rand.Reader, big.NewInt(100000))
	if err != nil {
		return "", err
	}

	randomStr := randomNum.String()

	hasher := fnv.New32()

	hasher.Write([]byte(randomStr))

	hashSum := hasher.Sum32() % 100000

	hashString := fmt.Sprintf("%05d", hashSum)

	return hashString, nil
}
