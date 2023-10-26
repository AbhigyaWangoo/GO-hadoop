package sdfs

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"unsafe"

	gossipUtils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func RequestBlockMappings(FileName string) [][]string {
	// 1. Create a task with the GET_2D block operation, and send to current master. If timeout/ doesn't work, send to 1st submaster, second, and so on.
	// 2. Listen for 2d array on responding connection. Read 2d array and return it.

	var rv [][]string

	return rv
}

func InitiatePutCommand(LocalFilename string, SdfsFilename string) {
	fmt.Printf("localfilename: %s sdfs: %s\n", LocalFilename, SdfsFilename)

	// IF CONNECTION CLOSES WHILE WRITING, WE NEED TO REPICK AN IP ADDR. Can have a seperate function to handle this on failure cases.
	// Ask master when its ok to start writing
	masterConnection, err := utils.OpenTCPConnection(utils.MASTER_IP, utils.SDFS_PORT)
	if err != nil {
		fmt.Errorf("error opening master connection: ", err)
	}
	var sentinalIp [16]byte
	copy(sentinalIp[:], "-1")

	var sdfsFilename [1024]byte
	copy(sdfsFilename[:], []byte(SdfsFilename))

	checkCanPut := utils.Task{
		DataTargetIp:        sentinalIp,
		AckTargetIp:         sentinalIp,
		ConnectionOperation: utils.WRITE,
		FileName:            sdfsFilename,
		BlockIndex:          -1,
		DataSize:            -1,
		IsAck:               false,
	}

	marshaledData, err := json.Marshal(checkCanPut)
	if err != nil {
		fmt.Errorf("error marshaling data: ", err)
	}
	masterConnection.Write(marshaledData)
	buffer := make([]byte, unsafe.Sizeof(checkCanPut))
	masterConnection.Read(buffer) // Don't need to check what was read, if something is read at all it's an ack

	// 1. Create 2d array of ip addressses
	// 		Call InitializeBlockLocationsEntry(), which should init an empty array for a filename.

	fileSize := utils.GetFileSize("test/" + LocalFilename)

	n, m := utils.CeilDivide(fileSize, int64(utils.BLOCK_SIZE)), utils.REPLICATION_FACTOR

	// locationsToWrite := InitializeBlockLocationsEntry(SdfsFilename, fileInfo.Size())

	for current_block := int64(0); current_block < n; current_block++ {
		allMemberIps := gossipUtils.MembershipMap.Keys()
		remainingIps := make([]string, len(allMemberIps))
		copy(remainingIps, allMemberIps)
		for current_replica := 0; current_replica < m; current_replica++ {
			ipToSendBlock, remainingIps := PopRandomElementInArray(remainingIps)
			
		}
	}

	// 2. For i = 0; i < num_blocks; i ++
	// 		2.a. Construct a List of FollowerTasks, with the ack target as the master and DataTargetIp as empty
	// 		2.b. Open Connections to all ips in 2darr[i], and write tasks to all ips.
	// 		2.c. buffered read localfilename[i:i+block_size] (4kb at a time should work, check utils for KB variable)
	// 		2.d. IN BUFFERED READ FUNCTION -> for ip in 2darr[i]:
	// 				2.d.a. Spawn a thread to write current read block to ip with connections previously opened
	//

}

func InitiateGetCommand(sdfs_filename string, localfilename string) {
	fmt.Printf("localfilename: %s sdfs: %s\n", localfilename, sdfs_filename)

	// IF CONNECTION CLOSES WHILE READING, WE NEED TO REPICK AN IP ADDR TO READ FROM. Can have a seperate function to handle this on failure cases.

	// 1. Query master for 2d array of ip addresses (2darr)
	// 2. For i = 0; i < num_blocks; i ++
	// 		### at the beginning of every loop, need to fseek(i*block_size) ###
	// 		2.a. Randomly pick IP addresses from 2darr[i] until you can open a connection to one successfully. If impossible, re-request 2d array.
	// 		2.b. Construct a FollowerTask with the operation=READ, blockidx=i, filename=sdfs_filename, acktarget=master, datatarget="" (IMPORTANT, IF DATATARGET IS EMPTY, IT MEANS JUST SEND DATA BACK ON THE SAME CONNECTION)
	// 		2.c. buffered read from connection (4kb at a time should work, check utils for KB variable)
	// 		2.d. IN BUFFERED READ FUNCTION -> write buffered array to localfilename.
}

func InitiateDeleteCommand(sdfs_filename string) {
	fmt.Printf("sdfs: %s\n", sdfs_filename)
	// IF CONNECTION CLOSES WHILE READING, its all good. We can assume memory was wiped

	// 1. Query master for 2d array of ip addresses (2darr)
	// 2. For i = 0; i < num_blocks; i ++
	// 3. 		For j = 0; j < num_replicas; j++
	// 				3.a. Create a delete task struct, with master as ack target, and send to ip addr.

}

func InitiateLsCommand(sdfs_filename string) {

}

func InitiateStoreCommand() {

}

func GetMaster() string {
	// Get master IP from Gossip Mmebership Map
	return "not impl"
}

func PopRandomElementInArray(array []string) (string, []string) {
	// Get a random index using crypto/rand
	max := big.NewInt(int64(len(array)))
	randomIndexBig, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	randomIndex := randomIndexBig.Int64()

	return array[randomIndex], append(array[:randomIndex], array[randomIndex+1:]...)
}
