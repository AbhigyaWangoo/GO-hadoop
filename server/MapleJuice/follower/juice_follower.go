package maplejuice

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"

	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfs "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func HandleJuiceRequest(Task *maplejuiceutils.MapleJuiceTask, conn *net.Conn) {
	fmt.Println("Entering handle juice request for ", Task.SdfsPrefix)
	SdfsFilename := Task.SdfsPrefix // SdfsFilename is the one to pull from SDFS, and run the juice task on.
	juice_exec := Task.SdfsExecFile
	dst_file := Task.SdfsDst

	// CLI GET file locally
	sdfs.CLIGet(SdfsFilename, SdfsFilename)

	// Run exec file on input file
	cmd := exec.Command(juice_exec, "-i", SdfsFilename)
	output, err := cmd.CombinedOutput()
	// _, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Error running command:", err)
		return
	}

	ParseOutput(Task.NodeDesignation, string(output), dst_file)
}

func ParseOutput(nodeIdx uint32, output string, dstSdfsFile string) error {
	// Take the output, and append it to the dst sdfs file.
	nodeIdxStr := strconv.FormatUint(uint64(nodeIdx), 10)
	oFileName := sdfsutils.FILESYSTEM_ROOT + nodeIdxStr + "_" + dstSdfsFile
	fmt.Println("Writing juice node to loacl fs: ", oFileName)
	file, err := os.Create(oFileName)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return err
	}
	defer file.Close()
	fmt.Println("Created local file")

	writer := bufio.NewWriter(file)

	// Write data to the file
	_, err = writer.WriteString(output)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}
	fmt.Println("wrote local juice data to file")

	// Flush the writer to ensure all data is written to the file
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing writer:", err)
		return err
	}

	var resultString string
	lastUnderscoreIndex := strings.LastIndex(dstSdfsFile, "_")
	if lastUnderscoreIndex != -1 {
		// Remove characters starting from the last '_'
		resultString := dstSdfsFile[:lastUnderscoreIndex]
		fmt.Println(resultString)
	} else {
		// If '_' is not found, error out
		return errors.New("No underscored found on the dstSdfsFile")
	}
	FileSize, err := sdfs.GetFileSizeByPrefix(resultString)

	// Send ack to master
	SdfsAck := sdfsutils.Task{
		DataTargetIp:        sdfsutils.New19Byte(gossiputils.Ip),
		AckTargetIp:         sdfsutils.New19Byte(sdfsutils.LEADER_IP),
		ConnectionOperation: sdfsutils.WRITE,
		FileName:            sdfsutils.New1024Byte(oFileName),
		OriginalFileSize:    int64(FileSize), // TODO not sure how I could even assign this info...
		BlockIndex:          int64(nodeIdx),
		DataSize:            int64(len(output)),
		IsAck:               true,
	}

	sdfsutils.SendAckToMaster(SdfsAck)
	fmt.Println("Sent ack to master from juice follower")
	
	return nil
}
