package maplejuice

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"

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
	fmt.Println("Got file in juice follower: ", Task.SdfsPrefix)

	// Run exec file on input file
	cmd := exec.Command(juice_exec, append([]string{"-i", SdfsFilename}, Task.ExecFileArguments...)...)

	output, err := cmd.CombinedOutput()
	fmt.Println("Ran juice cmd", juice_exec)
	// _, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Error running command:", err)
		return
	}

	ParseOutput(Task.NodeDesignation, string(output), dst_file, Task.NumberOfMJTasks*uint32(sdfsutils.BLOCK_SIZE))
	fmt.Println("parsed output on juice task")
}

func ParseOutput(nodeIdx uint32, output string, dstSdfsFile string, FileSize uint32) error {
	// Take the output, and append it to the dst sdfs file.
	nodeIdxStr := strconv.FormatUint(uint64(nodeIdx), 10)
	oFileName := sdfsutils.FILESYSTEM_ROOT + nodeIdxStr + "_" + dstSdfsFile

	fmt.Println("Writing juice node to loacl fs: ", oFileName)
	file := maplejuiceutils.OpenFile(oFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR)
	defer file.Close()

	fmt.Println("Created local file")

	writer := bufio.NewWriter(file)

	// Write data to the file
	_, err := writer.WriteString(output)
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


	// Send ack to master
	SdfsAck := sdfsutils.Task{
		DataTargetIp:        sdfsutils.New19Byte(gossiputils.Ip),
		AckTargetIp:         sdfsutils.New19Byte(gossiputils.Ip),
		ConnectionOperation: sdfsutils.WRITE,
		FileName:            sdfsutils.New1024Byte(dstSdfsFile),
		OriginalFileSize:    int64(FileSize), // TODO not sure how I could even assign this info...
		BlockIndex:          int64(nodeIdx),
		DataSize:            int64(len(output)),
		IsAck:               true,
	}

	sdfsutils.SendAckToMaster(SdfsAck)
	fmt.Println("Sent ack to master from juice follower")

	return nil
}
