package sdfs

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"

	gossipUtils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func RequestBlockMappings(FileName string) [][]string {
	// 1. Create a task with the GET_2D block operation, and send to current master. If timeout/ doesn't work, send to 1st submaster, second, and so on.
	// 2. Listen for 2d array on responding connection. Read 2d array and return it.

	var task utils.Task
	task.DataTargetIp = utils.New16Byte("127.0.0.1")
	task.AckTargetIp = utils.New16Byte("127.0.0.1")
	task.OriginalFileSize = 0
	task.BlockIndex = 0
	task.DataSize = 0
	task.ConnectionOperation = utils.GET_2D
	task.FileName = utils.New1024Byte(FileName)
	task.FileNameLength = len(FileName)
	task.IsAck = true
	task.AckTargetIp = utils.New16Byte("127.0.0.1")

	conn := SendAckToMaster(task)
	locations := utils.UnmarshalBlockLocationArr(*conn)

	return locations
}

func InitiatePutCommand(LocalFilename string, SdfsFilename string) {
	fmt.Printf("localfilename: %s sdfs: %s\n", LocalFilename, SdfsFilename)

	// IF CONNECTION CLOSES WHILE WRITING, WE NEED TO REPICK AN IP ADDR. Can have a seperate function to handle this on failure cases.
	// Ask master when its ok to start writing
	// masterConnection, err := utils.OpenTCPConnection(utils.LEADER_IP, utils.SDFS_PORT)
	// if err != nil {
	// 	fmt.Errorf("error opening master connection: ", err)
	// }

	// checkCanPut := utils.Task{
	// 	DataTargetIp:        utils.New16Byte("-1"),
	// 	AckTargetIp:         utils.New16Byte("-1"),
	// 	ConnectionOperation: utils.WRITE,
	// 	FileName:            utils.New1024Byte(SdfsFilename),
	// 	BlockIndex:          -1,
	// 	DataSize:            -1,
	// 	IsAck:               false,
	// }

	// masterConnection.Write(checkCanPut.Marshal())
	// buffer := make([]byte, unsafe.Sizeof(checkCanPut))
	// masterConnection.Read(buffer) // Don't need to check what was read, if something is read at all it's an ack

	// 1. Create 2d array of ip addressses
	// 		Call InitializeBlockLocationsEntry(), which should init an empty array for a filename.

	// pathToLocalFile := "test/" + LocalFilename

	dir, _ := os.Getwd()
	log.Printf(dir)

	fileSize := utils.GetFileSize(LocalFilename)

	numberBlocks, numberReplicas := utils.CeilDivide(fileSize, int64(utils.BLOCK_SIZE)), utils.REPLICATION_FACTOR

	// locationsToWrite := InitializeBlockLocationsEntry(SdfsFilename, fileInfo.Size())

	for currentBlock := int64(0); currentBlock < numberBlocks; currentBlock++ {
		go func(currentBlock int64) {
			allMemberIps := gossipUtils.MembershipMap.Keys()
			remainingIps := utils.CreateConcurrentStringSlice(allMemberIps)
			startIdx, lengthToWrite := utils.GetBlockPosition(currentBlock, fileSize)
			file, err := os.Open(LocalFilename)
			if err != nil {
				log.Fatalf("error opening local file: %v\n", err)
			}
			defer file.Close()
			for currentReplica := 0; currentReplica < numberReplicas; currentReplica++ {
				for {
					log.Printf("TEST")
					if remainingIps.Size() == 0 {
						break
					}
					if ip, ok := remainingIps.PopRandomElement().(string); ok {
						log.Printf("ip")
						member, _ := gossipUtils.MembershipMap.Get(ip)
						if ip == gossipUtils.Ip || member.State == gossipUtils.DOWN {
							continue
						}
						conn, err := utils.OpenTCPConnection(ip, utils.SDFS_PORT)
						if err != nil {
							log.Fatalf("error opening follower connection: %v\n", err)
							continue
						}
						defer conn.Close()
						blockWritingTask := utils.Task{
							DataTargetIp:        utils.New16Byte(""),
							AckTargetIp:         utils.New16Byte(utils.LEADER_IP),
							ConnectionOperation: utils.WRITE,
							FileName:            utils.New1024Byte(SdfsFilename),
							FileNameLength:      len(SdfsFilename),
							OriginalFileSize:    int(fileSize),
							BlockIndex:          int(currentBlock),
							DataSize:            uint32(lengthToWrite),
							IsAck:               false,
						}
						// log.Printf(string(blockWritingTask.Marshal()))
						// log.Printf(unsafe.Sizeof(blockWritingTask.Marshal()))
						marshalledBytesWritten, writeError := conn.Write(blockWritingTask.Marshal())
						conn.Write([]byte{'\n'})
						if writeError != nil {
							log.Fatalf("Could not write struct to connection in client put: %v\n", writeError)
						}

						file.Seek(0, int(startIdx))
						totalBytesWritten, err := utils.BufferedReadAndWrite(conn, file, uint32(lengthToWrite), true)
						fmt.Println("------BYTES_WRITTEN------: ", totalBytesWritten)
						fmt.Println("------BYTES_WRITTEN marshalled------: ", marshalledBytesWritten)
						if err != nil { // If failure to write full block, redo loop
							log.Fatalf("connection broke early, rewrite block")
							continue
						}
						break
					}
				}
			}
		}(currentBlock)
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
		log.Fatalf("unable to open local file, ", err)
	}
	task := utils.Task{
		DataTargetIp:        utils.New16Byte("-1"),
		AckTargetIp:         utils.New16Byte("-1"),
		ConnectionOperation: utils.GET_2D,
		FileName:            utils.New1024Byte(sdfsFilename),
		OriginalFileSize:    -1,
		BlockIndex:          -1,
		DataSize:            0,
		IsAck:               false,
	}

	leaderIp := utils.GetLeader()
	conn, err := utils.OpenTCPConnection(leaderIp, utils.SDFS_PORT)
	defer conn.Close()
	if err != nil {
		log.Fatalf("unable to open connection to master: ", err)
	}
	utils.SendTaskOnExistingConnection(task, conn)
	blockLocationArr := utils.UnmarshalBlockLocationArr(conn)
	for blockIdx, replicas := range blockLocationArr {
		for {
			randomReplicaIp, err := PopRandomElementInArray(&replicas)
			if err != nil {
				log.Fatalf("All replicas down ):")
			}
			task := utils.Task{
				DataTargetIp:        utils.New16Byte(gossipUtils.Ip),
				AckTargetIp:         utils.New16Byte(gossipUtils.Ip),
				ConnectionOperation: utils.READ,
				FileName:            utils.New1024Byte(sdfsFilename),
				FileNameLength:      len(sdfsFilename),
				OriginalFileSize:    0,
				BlockIndex:          blockIdx,
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

			blockMetadata, _ := utils.Unmarshal(replicaConn)
			utils.BufferedReadAndWrite(replicaConn, fp, blockMetadata.DataSize, false)
			break
		}
	}

	// utils.

}

func InitiateDeleteCommand(sdfs_filename string) {
	fmt.Printf("sdfs: %s\n", sdfs_filename)
	// IF CONNECTION CLOSES WHILE READING, its all good. We can assume memory was wiped

	mappings := RequestBlockMappings(sdfs_filename)
	fmt.Println("Mappings: ", mappings)
	
	var task utils.Task
	task.IsAck = false
	task.ConnectionOperation = utils.DELETE
	task.FileName = utils.New1024Byte(sdfs_filename)
	task.FileNameLength = len(sdfs_filename)

	for i := 0; i < len(mappings); i++ {
        for j := 0; j < len(mappings[i]); j++ {
			blockIp := mappings[i][j]

			task.BlockIndex = i

			conn, err := utils.OpenTCPConnection(blockIp, utils.SDFS_PORT) // TODO Hardcoded, change me
			if err != nil {
				log.Fatalf("Couldn't open tcp conn to leader %v\n", err)
			}
			
			data := task.Marshal()
			_, errMWrite := conn.Write(data)
			if errMWrite != nil {
				log.Fatalf("Couldn't write marshalled delete task %v\n", errMWrite)
			}
			
			fmt.Println("Finished delete task")
		}
    }

	// 1. Query master for 2d array of ip addresses (2darr)
	// 2. For i = 0; i < num_blocks; i ++
	// 3. 		For j = 0; j < num_replicas; j++
	// 				3.a. Create a delete task struct, with master as ack target, and send to ip addr.

}

func InitiateLsCommand(sdfs_filename string) {

}

func InitiateStoreCommand() {

}

// func GetSubmasters() []string {
// 	gossipUtils.MembershipMap.Keys()
// }

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
