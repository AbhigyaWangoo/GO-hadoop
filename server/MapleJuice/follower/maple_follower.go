package maplejuice

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"

	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func HandleMapleRequest(Task *maplejuiceutils.MapleJuiceTask, MapleConn net.Conn) {
	// a function to handle a single maple task request
	execOutputFp := getExecutableOutput(MapleConn, Task.SdfsPrefix, Task.SdfsExecFile, Task.ExecFileArguments)
	blockIdx := Task.NodeDesignation
	numMJTasks := Task.NumberOfMJTasks
	execOutputFp.Seek(0, 0)

	log.Println("Block idx: ", blockIdx)

	putAcksToSend := readAndStoreKeyValues(execOutputFp, blockIdx, Task.SdfsPrefix, numMJTasks)

	for _, ack := range putAcksToSend {
		masterConn := sdfsutils.SendAckToMaster(ack)
		(*masterConn).Close()
	}

	remoteAddr := MapleConn.RemoteAddr()

	// Convert to a TCP address
	tcpAddr, _ := remoteAddr.(*net.TCPAddr)

	sdfsutils.OpenTCPConnection(tcpAddr.IP.String(), maplejuiceutils.MAPLE_JUICE_ACK_PORT)

	// 1. Take Maple task, Retrieve exec file from sdfs, and [dataset lines] from connection
	// 2. Run executable on each line of the [dataset lines]
	// 3. From the resultant [K, V], store each unique K, V in Task.NodeDesignation_Task.SdfsPrefix_K locally, MAKE SURE TO OPEN FILE IF DNE, OR IN APPEND MODE
	// 4. send ack to sdfs master for locally created files.
}

func readAndStoreKeyValues(inputFp *os.File, blockIdx uint32, sdfsPrefix string, numberOfMJTasks uint32) []sdfsutils.Task {
	// Create a scanner to read the file line by line
	keyToFp := make(map[string]*os.File)
	putAcksToSend := make([]sdfsutils.Task, 0)

	scanner := bufio.NewScanner(inputFp)
	for scanner.Scan() {
		line := scanner.Text()
		key, value := getKeyValueFromLine(line)
		_, exists := keyToFp[key]
		if !exists {
			blockToOpenPath := "server/sdfs/sdfsFileSystemRoot/" + strconv.Itoa(int(blockIdx)) + "_" + sdfsPrefix + "_" + key
			keyToFp[key] = maplejuiceutils.OpenFile(blockToOpenPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
			defer keyToFp[key].Close()
		}
		keyValFormatted := "[" + key + ": " + value + "]"
		keyToFp[key].Write([]byte(keyValFormatted))
		keyToFp[key].Write([]byte{'\n'})
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	for key, _ := range keyToFp {
		// task := sdfsutils.Task{
		// 	DataTargetIp        [19]byte
		// 	AckTargetIp         [19]byte
		// 	ConnectionOperation BlockOperation // READ, WRITE, GET_2D, OR DELETE from sdfs utils
		// 	FileName            [1024]byte
		// 	OriginalFileSize    int64
		// 	BlockIndex          int64
		// 	DataSize            int64 // TODO change me to int64
		// 	IsAck               bool
		// }
		fileName := sdfsPrefix + "_" + key
		task := sdfsutils.Task{
			DataTargetIp:        sdfsutils.New19Byte(gossiputils.Ip),
			AckTargetIp:         sdfsutils.New19Byte(gossiputils.Ip),
			ConnectionOperation: sdfsutils.WRITE,
			FileName:            sdfsutils.New1024Byte(fileName),
			OriginalFileSize:    sdfsutils.BLOCK_SIZE * int64(numberOfMJTasks),
			BlockIndex:          int64(blockIdx),
			DataSize:            0,
			IsAck:               true,
		}
		putAcksToSend = append(putAcksToSend, task)
	}

	return putAcksToSend

	// Check for errors
	// outputFp := maplejuiceutils.OpenFile(outputFpName, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	// defer outputFp.Close()
}

func getKeyValueFromLine(line string) (key string, value string) {

	regexPattern := `\[(?P<key>(.+)):\s*(?P<value>(.+))\]`

	regex := regexp.MustCompile(regexPattern)

	matches := regex.FindStringSubmatch(line)

	if len(matches) > 0 {
		k := matches[regex.SubexpIndex("key")]
		v := matches[regex.SubexpIndex("value")]

		return k, v
	} else {
		log.Printf("No match found.")
		return "An error occurred", ""
	}
}

func getExecutableOutput(conn net.Conn, sdfsPrefix string, executableFileName string, execArgs []string) *os.File {
	execOutputFileName := sdfsPrefix + "_execOutput"
	execOutputFp := maplejuiceutils.OpenFile(execOutputFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC)

	maplejuiceutils.ReadAllDataFromConn(conn, sdfsPrefix)

	cmd := exec.Command("./"+executableFileName, append([]string{"-f", sdfsPrefix}, execArgs...)...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("Error creating stdout pipe:", err)
	}

	err = cmd.Start()
	if err != nil {
		log.Println("Error starting command:", err)
	}

	scanner := bufio.NewScanner(stdoutPipe)
	for scanner.Scan() {
		line := scanner.Text()
		execOutputFp.Write([]byte(line))
		execOutputFp.Write([]byte{'\n'})
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading from stdout pipe:", err)
	}

	err = cmd.Wait()
	if err != nil {
		fmt.Println("Error waiting for command to finish:", err)
	}

	return execOutputFp
}
