package sdfs

import (
	"bufio"
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"

	gossipUtils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func RequestBlockMappings(FileName string) ([][]string, error) {
	// 1. Create a task with the GET_2D block operation, and send to current master. If timeout/ doesn't work, send to 1st submaster, second, and so on.
	// 2. Listen for 2d array on responding connection. Read 2d array and return it.

	var task utils.Task
	task.DataTargetIp = utils.New19Byte("127.0.0.1")
	task.AckTargetIp = utils.New19Byte("127.0.0.1")
	task.OriginalFileSize = 0
	task.BlockIndex = 0
	task.DataSize = 0
	task.ConnectionOperation = utils.GET_2D
	task.FileName = utils.New1024Byte(FileName)
	task.IsAck = true
	task.AckTargetIp = utils.New19Byte("127.0.0.1")

	conn := utils.SendAckToMaster(task)
	defer (*conn).Close()
	locations, err := utils.UnmarshalBlockLocationArr(*conn)

	if err != nil {
		return nil, err // Returning an empty array on failure case
	}

	return locations, nil
}

func InitiatePutCommand(LocalFilename string, SdfsFilename string) {
	fmt.Printf("localfilename: %s sdfs: %s\n", LocalFilename, SdfsFilename)

	// IF CONNECTION CLOSES WHILE WRITING, WE NEED TO REPICK AN IP ADDR. Can have a seperate function to handle this on failure cases.
	// Ask master when its ok to start writing

	dir, _ := os.Getwd()
	log.Println(dir)

	fileSize, _ := utils.GetFileSize(LocalFilename)

	numberBlocks := utils.CeilDivide(fileSize, utils.BLOCK_SIZE)

	// locationsToWrite := InitializeBlockLocationsEntry(SdfsFilename, fileInfo.Size())

	fmt.Println("Num blocks:", numberBlocks)
	fmt.Println("file size:", fileSize)
	fmt.Println("block size:", int64(utils.BLOCK_SIZE))
	// currentBlock := int64(0)
	for currentBlock := int64(0); currentBlock < numberBlocks; currentBlock++ {

		// go func(currentBlock int64) {

		allMemberIps := gossipUtils.MembershipMap.Keys()
		remainingIps := utils.CreateConcurrentStringSlice(allMemberIps)

		file, err := os.Open(LocalFilename)

		if err != nil {
			log.Fatalf("error opening local file: %v\n", err)
		}
		startIdx, lengthToWrite := utils.GetBlockPosition(currentBlock, fileSize)

		for currentReplica := int64(0); currentReplica < utils.REPLICATION_FACTOR; currentReplica++ {
			fmt.Printf("start index: %d length to write: %d\n", startIdx, lengthToWrite)

			for {
				if remainingIps.Size() == 0 {
					break
				}

				if ip, ok := remainingIps.PopRandomElement().(string); ok {
					member, _ := gossipUtils.MembershipMap.Get(ip)

					if ip == gossipUtils.Ip || member.State == gossipUtils.DOWN {
						continue
					}

					conn, err := utils.OpenTCPConnection(ip, utils.SDFS_PORT)
					if err != nil {
						log.Fatalf("error opening follower connection: %v\n", err)
						continue
					}
					// defer conn.Close()
					blockWritingTask := utils.Task{
						DataTargetIp:        utils.New19Byte(ip),
						AckTargetIp:         utils.New19Byte(utils.LEADER_IP),
						ConnectionOperation: utils.WRITE,
						FileName:            utils.New1024Byte(SdfsFilename),
						OriginalFileSize:    fileSize,
						BlockIndex:          currentBlock,
						DataSize:            lengthToWrite,
						IsAck:               false,
					}
					fmt.Printf("start index: %d length to write: %d\n", startIdx, lengthToWrite)
					fmt.Printf("Expecting size of: %d\n", blockWritingTask.DataSize)

					// log.Printf(string(blockWritingTask.Marshal()))
					// log.Printf(unsafe.Sizeof(blockWritingTask.Marshal()))
					marshalledBytesWritten, writeError := conn.Write(blockWritingTask.Marshal())
					conn.Write([]byte{'\n'})
					if writeError != nil {
						log.Fatalf("Could not write struct to connection in client put: %v\n", writeError)
					}

					// log.Println("HEYEUEEYEYE")
					// for {

					// }
					utils.ReadSmallAck(conn)

					totalBytesWritten, writeErr := utils.BufferedWriteToConnection(conn, file, lengthToWrite, startIdx)
					// utils.BufferedReadAndWrite(buffConn, file, lengthToWrite, true, startIdx)
					fmt.Println("------BYTES_WRITTEN------: ", totalBytesWritten)
					fmt.Println("------BYTES_WRITTEN marshalled------: ", marshalledBytesWritten)

					if writeErr != nil { // If failure to write full block, redo loop
						fmt.Println("connection broke early, rewrite block: ", writeErr)
						continue
					}
					utils.ReadSmallAck(conn)

					break
				}
			}
		}
		file.Close()
		// } (currentBlock)
	}

	// 2. For i = 0; i < num_blocks; i ++
	// 		2.a. Construct a List of FollowerTasks, with the ack target as the master and DataTargetIp as empty
	// 		2.b. Open Connections to all ips in 2darr[i], and write tasks to all ips.
	// 		2.c. buffered read localfilename[i:i+block_size] (4kb at a time should work, check utils for KB variable)
	// 		2.d. IN BUFFERED READ FUNCTION -> for ip in 2darr[i]:
	// 				2.d.a. Spawn a thread to write current read block to ip with connections previously opened
	//

}

func InitiateGetCommand(sdfsFilename string, localfilename string) {
	fmt.Printf("localfilename: %s sdfs: %s\n", localfilename, sdfsFilename)

	// IF CONNECTION CLOSES WHILE READING, WE NEED TO REPICK AN IP ADDR TO READ FROM. Can have a seperate function to handle this on failure cases.

	// 1. Query master for 2d array of ip addresses (2darr)
	// 2. For i = 0; i < num_blocks; i ++
	// 		### at the beginning of every loop, need to fseek(i*block_size) ###
	// 		2.a. Randomly pick IP addresses from 2darr[i] until you can open a connection to one successfully. If impossible, re-request 2d array.
	// 		2.b. Construct a FollowerTask with the operation=READ, blockidx=i, filename=sdfs_filename, acktarget=master, datatarget="" (IMPORTANT, IF DATATARGET IS EMPTY, IT MEANS JUST SEND DATA BACK ON THE SAME CONNECTION)
	// 		2.c. buffered read from connection (4kb at a time should work, check utils for KB variable)
	// 		2.d. IN BUFFERED READ FUNCTION -> write buffered array to localfilename.

	fp, err := os.OpenFile(localfilename, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("unable to open local file, ", err)
	}

	task := utils.Task{
		DataTargetIp:        utils.New19Byte("-1"),
		AckTargetIp:         utils.New19Byte("-1"),
		ConnectionOperation: utils.GET_2D,
		FileName:            utils.New1024Byte(sdfsFilename),
		OriginalFileSize:    0,
		BlockIndex:          0,
		DataSize:            0,
		IsAck:               true,
	}

	leaderIp := gossiputils.GetLeader()
	conn, err := utils.OpenTCPConnection(leaderIp, utils.SDFS_PORT)
	if err != nil {
		log.Fatalln("unable to open connection to master: ", err)
	}
	defer conn.Close()
	// buffConn := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	utils.SendTaskOnExistingConnection(task, conn)
	blockLocationArr, BlockLocErr := utils.UnmarshalBlockLocationArr(conn)
	if BlockLocErr != nil {
		fmt.Printf("File probably dne on get command. returning.")
		return
	}

	fmt.Println("Unmarshalled block location arr: ", blockLocationArr)
	for blockIdx, replicas := range blockLocationArr {
		for {
			randomReplicaIp, err := PopRandomElementInArray(&replicas)
			if err != nil {
				log.Fatalf("All replicas down ):")
			}
			if randomReplicaIp == "w" {
				continue
			}
			task := utils.Task{
				DataTargetIp:        utils.New19Byte(gossipUtils.Ip),
				AckTargetIp:         utils.New19Byte(gossipUtils.Ip),
				ConnectionOperation: utils.READ,
				FileName:            utils.New1024Byte(sdfsFilename),
				OriginalFileSize:    0,
				BlockIndex:          int64(blockIdx),
				DataSize:            0,
				IsAck:               false,
			}
			replicaConn, err := utils.OpenTCPConnection(randomReplicaIp, utils.SDFS_PORT)
			if err != nil {
				log.Printf("unable to connect to replica, ", err)
				continue
			}
			err = utils.SendTaskOnExistingConnection(task, replicaConn)
			if err != nil {
				log.Printf("unable to send task to replica, ", err)
				continue
			}

			utils.ReadSmallAck(replicaConn)
			log.Printf("Unmarshaling task\n")

			blockMetadata, _ := utils.Unmarshal(replicaConn)
			utils.SendSmallAck(replicaConn)

			log.Printf("Number of bytes to read from connection: ", blockMetadata.DataSize)
			utils.BufferedReadFromConnection(replicaConn, fp, blockMetadata.DataSize)
			replicaConn.Close()
			// utils.BufferedReadAndWrite(replicaConnBuf, fp, blockMetadata.DataSize, false, 0)
			break
		}
	}
}

func InitiateDeleteCommand(sdfsFilename string) {
	fmt.Printf("sdfs: %s\n", sdfsFilename)
	// IF CONNECTION CLOSES WHILE READING, its all good. We can assume memory was wiped

	// 1. Query master for 2d array of ip addresses (2darr)
	mappings, mappingsErr := RequestBlockMappings(sdfsFilename)
	if mappingsErr != nil {
		fmt.Printf("File did not exist and thus cannot be deleted.")
	}
	fmt.Println("Mappings: ", mappings)

	var task utils.Task
	task.IsAck = false
	task.ConnectionOperation = utils.DELETE
	task.FileName = utils.New1024Byte(sdfsFilename)

	for i := 0; i < len(mappings); i++ {
		for j := 0; j < len(mappings[i]); j++ {

			if mappings[i][j] == utils.WRITE_OP || mappings[i][j] == utils.DELETE_OP {
				continue
			}

			// Create a delete task struct, with master as ack target, and send to ip addr.
			blockIp := mappings[i][j]
			member, ok := gossipUtils.MembershipMap.Get(blockIp)
			if !ok || member.State == gossiputils.DOWN {
				fmt.Printf("No need to delete on node %s, it's already down. Continuing...", blockIp)
				continue
			}

			task.BlockIndex = int64(i)

			conn, err := utils.OpenTCPConnection(blockIp, utils.SDFS_PORT)
			if err != nil {
				log.Fatalf("Couldn't open tcp conn to leader %v\n", err)
			}
			buffConn := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

			data := task.Marshal()
			_, errMWrite := buffConn.Write(data)
			buffConn.Write([]byte{'\n'})
			buffConn.Flush()

			if errMWrite != nil {
				log.Fatalf("Couldn't write marshalled delete task %v\n", errMWrite)
			}

			fmt.Println("Finished delete task")
		}
	}
}

func InitiateLsCommand(sdfs_filename string) {

	// 1. Query master for 2d array of ip addresses (2darr)
	mappings, mappingsErr := RequestBlockMappings(sdfs_filename)
	IpAddrs := make(map[string]bool)
	var result []string
	if mappingsErr != nil {
		fmt.Printf("File did not exist and thus cannot be deleted.")
	}

	for _, slice := range mappings {
		for _, str := range slice {
			if !IpAddrs[str] {
				IpAddrs[str] = true
				result = append(result, str)
			}
		}
	}

	fmt.Println(result)
}

func InitiateStoreCommand() {
	// utils.FILESYSTEM_ROOT
	items, _ := ioutil.ReadDir(utils.FILESYSTEM_ROOT)
	for _, item := range items { 
		if !item.IsDir() {
			fmt.Println(item.Name())
		}
	}

}

func PopRandomElementInArray(array *[]string) (string, error) {
	// Get a random index using crypto/rand
	if len(*array) == 0 {
		return "", errors.New("No more elements to pop")
	}
	max := big.NewInt(int64(len(*array)))
	randomIndexBig, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	randomIndex := randomIndexBig.Int64()

	randomElement := (*array)[randomIndex]
	*array = append((*array)[:randomIndex], (*array)[randomIndex+1:]...)
	return randomElement, nil

	// [randomIndex], append(array[:randomIndex], array[randomIndex+1:]...)
}
