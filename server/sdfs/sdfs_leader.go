package sdfs

import (
	"errors"
	"fmt"
	"log"
	"net"

	cmap "github.com/orcaman/concurrent-map/v2"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

var BlockLocations cmap.ConcurrentMap[string, [][]string] = cmap.New[[][]string]()           // filename : [[ip addr, ip addr, ], ], index 2d arr by block index
var FileToOriginator cmap.ConcurrentMap[string, []string] = cmap.New[[]string]()             // filename : [ClientIpWhoCreatedFile, ClientCreationTime]
var FileToBlocks cmap.ConcurrentMap[string, [][2]interface{}] = cmap.New[[][2]interface{}]() // IPaddr : [[blockidx, filename]]

// Initializes a new entry in BlockLocations, so the leader can begin listening for block acks.
func InitializeBlockLocationsEntry(Filename string, FileSize int64) {
	n, m := utils.CeilDivide(FileSize, utils.BLOCK_SIZE), utils.REPLICATION_FACTOR // Size of the 2D array (n rows, m columns)
	newEntry := make([][]string, n)                                                // Create a slice of slices (2D array)

	// Populate the 2D array with arbitrary values
	var i int64
	for i = 0; i < n; i++ {
		newEntry[i] = make([]string, m)
		for j := int64(0); j < m; j++ {
			// Assign sentinal values to the 2D array
			newEntry[i][j] = utils.WRITE_OP
		}
	}

	BlockLocations.Set(Filename, newEntry)
}

// Master functions
func RouteToSubMasters(IncomingAck utils.Task) {
	// Route an incoming ack that makes a change to the membership list to the submasters.(Bully git issue) put test/500mb.txt 500
	kLeaders := gossiputils.GetKLeaders()
	for _, leader := range kLeaders {
		if leader != gossiputils.Ip {
			utils.SendTask(IncomingAck, leader, true)
		}
	}
}

func HandleAck(IncomingAck utils.Task, conn *net.Conn) error {

	if !IncomingAck.IsAck {
		return errors.New("ack passed to master for processing was not actually an ack")
	}

	fileName := utils.BytesToString(IncomingAck.FileName[:])
	ackSourceIp := utils.BytesToString(IncomingAck.AckTargetIp[:])

	if IncomingAck.ConnectionOperation == utils.WRITE {

		fmt.Printf("Got ack for write, the ack source is >{%s}<\n", ackSourceIp)
		fmt.Println("Got ack for write, filename is ", fileName)
		fmt.Println("Got ack for write, File size is ", IncomingAck.OriginalFileSize)

		// RouteToSubMasters(IncomingAck)

		if !BlockLocations.Has(fileName) {
			fmt.Println("Never seen before filename, creating block locations entry")
			InitializeBlockLocationsEntry(fileName, IncomingAck.OriginalFileSize)
		}

		blockMap, _ := BlockLocations.Get(fileName)
		for i := int64(0); i < utils.REPLICATION_FACTOR; i++ {
			if blockMap[IncomingAck.BlockIndex][i] == utils.WRITE_OP {
				blockMap[IncomingAck.BlockIndex][i] = ackSourceIp
				break
			}
		}
		BlockLocations.Set(fileName, blockMap)

		if mapping, ok := FileToBlocks.Get(ackSourceIp); ok { // IPaddr : [[blockidx, filename]]
			mapping = append(mapping, [2]interface{}{IncomingAck.BlockIndex, fileName})
			fmt.Println("Appending a new file+blockidx for ip addr ", ackSourceIp)
			fmt.Println("mapping: ", mapping)
			FileToBlocks.Set(ackSourceIp, mapping)
		} else {
			initialMapping := make([][2]interface{}, 1)
			initialMapping[0] = [2]interface{}{IncomingAck.BlockIndex, fileName}

			fmt.Println("Creating a new file+blockidx for ip addr ", ackSourceIp)
			fmt.Println("mapping: ", initialMapping)

			FileToBlocks.Set(ackSourceIp, initialMapping)
		}
	} else if IncomingAck.ConnectionOperation == utils.GET_2D {
		Handle2DArrRequest(fileName, *conn)
	} else if IncomingAck.ConnectionOperation == utils.DELETE {
		if !BlockLocations.Has(fileName) {
			return errors.New("Never seen before filename, dropping delete operation")
		}

		blockMap, _ := BlockLocations.Get(fileName)
		row := blockMap[IncomingAck.BlockIndex]
		for i := int64(0); i < utils.REPLICATION_FACTOR; i++ {
			if row[i] == ackSourceIp {
				blockMap[IncomingAck.BlockIndex][i] = utils.DELETE_OP
			}
		}

		if mapping, ok := FileToBlocks.Get(ackSourceIp); ok { // IPaddr : [[blockidx, filename]]
			var idx uint

			for i, pair := range mapping {
				if pair[0] == int(IncomingAck.BlockIndex) && pair[1] == utils.BytesToString(IncomingAck.FileName[:]) {
					idx = uint(i)
					break
				}
			}
			// TODO need to delete from IPaddr, the value [blockidx, filename].

			fmt.Println("Mapping before delete: ", mapping)
			mapping = append(mapping[:idx], mapping[idx+1:]...)
			fmt.Println("Mapping after delete: ", mapping)

			FileToBlocks.Set(ackSourceIp, mapping)
		}
	}

	// 1. Ack for Write operation
	// 		1.a. Forward ack to submaster
	// 		1.b. Navigate to entry in fname:2darr mapping given the IncomingAck.filename and IncomingAck.blockidx, and src IP from IncomingAck.DataTargetIp, and add ip. Ensure there is a WRITE_OP at that location.
	// 		1.c If filename -> 2d arr mapping does not exist, initialize it with empty 2d arr, and add rows based on block idx. For acks that have not arrived, set those entires as WRITE_OPs.
	// 2. Ack for Delete operation
	// 		2.a. Forward ack to submaster
	// 		2.b. Navigate to entry in fname:2darr mapping given the IncomingAck.filename and IncomingAck.blockidx, and src IP from IncomingAck.DataTargetIp, and delete IP. Replace deleted IP with DELETE_OP constant.

	return nil
}

func Handle2DArrRequest(Filename string, conn net.Conn) {
	// Reply to a connection with the 2d array for the provided filename.
	arr, exists := BlockLocations.Get(Filename)
	allDs := true
	for i := 0; i < len(arr); i++ {
		for j := 0; j < len(arr[i]); j++ {
			if arr[i][j] != utils.DELETE_OP {
				allDs = false
				break
			}
		}
		if !allDs {
			break
		}
	}

	if allDs {
		BlockLocations.Remove(Filename)
		fmt.Println("Block location filename dne. Continuing")
		var empty [][]string
		arr = empty
	}

	if !exists {
		fmt.Println("Block location filename dne. Continuing")
	}

	marshalledArray := utils.MarshalBlockLocationArr(arr)
	_, err := conn.Write(marshalledArray)
	if err != nil {
		log.Fatalf("Error writing 2d arr to conn: %v\n", err)
	}
}

func HandleDown(DownIpAddr string) {
	// Remove IP addr from BlockLocations. set all with a 'd'.
	for keyval := range BlockLocations.IterBuffered() {
		sdfsFilename := keyval.Key
		blockLocations := keyval.Val

		for blockIdx := range blockLocations {
			for i := 0; i < int(utils.REPLICATION_FACTOR); i++ {
				if blockLocations[blockIdx][i] == DownIpAddr {
					blockLocations[blockIdx][i] = utils.WRITE_OP
				}
			}
		}

		BlockLocations.Set(sdfsFilename, blockLocations)
	}

	// Remove IP addr from FileToblocks. Delete all entries associated.
	FileToBlocks.Pop(DownIpAddr)
}

func HandleReReplication(DownIpAddr string) {

	fmt.Println("Entering re replication.")

	if blocksToRereplicate, ok := FileToBlocks.Get(DownIpAddr); ok {

		fmt.Println("Handling re-replication on blocks: ", blocksToRereplicate)

		for _, blockMetadata := range blocksToRereplicate {

			if fileName, ok := blockMetadata[1].(string); ok {
				fmt.Println("Handling re-replication file: ", fileName)

				if blockIdx, ok := blockMetadata[0].(int64); ok {
					fmt.Println("Handling re-replication block: ", blockIdx)

					if blockLocations, ok := BlockLocations.Get(fileName); ok {
						fmt.Println("Block locations for found at", blockLocations[blockIdx])

						locations := blockLocations[blockIdx]
						for _, ip := range locations {
							if ip == DownIpAddr || ip == utils.WRITE_OP || ip == utils.DELETE_OP {
								continue
							}

							allIps := gossiputils.MembershipMap.Keys()
							locationSet := make(map[string]bool)
							for _, item := range locations {
								locationSet[item] = true
							}

							// fmt.Println("All ips: ", allIps)
							// fmt.Println("Current locations set: ", locationSet)

							var replicationT string
							for {
								replicationTarget, err := PopRandomElementInArray(&allIps)
								// fmt.Println("Potential replication target: ", replicationTarget)

								if err != nil {
									fmt.Println("Error popping random ip from all ips. Continuing.", err)
									return
								}
								member, ok := gossiputils.MembershipMap.Get(replicationTarget)

								// fmt.Println("picked ip already has block: ", locationSet[replicationTarget])
								// fmt.Println("Member exists ", ok)
								// fmt.Println("Member state is alive ", member.State == gossiputils.ALIVE)
								if !locationSet[replicationTarget] && ok && member.State == gossiputils.ALIVE {
									replicationT = replicationTarget
									break
								}
							}

							fmt.Println("Replication Target ", replicationT)

							// TODO potential bug, if there is a connection that is down, we should try to pick a new one right away, not continue alltogether.
							conn, err := utils.OpenTCPConnection(ip, utils.SDFS_PORT)
							if err != nil {
								log.Println("unable to open connection: ", err)
								continue
							}
							defer conn.Close()

							fmt.Println("Openeed connection to ", ip)

							task := utils.Task{
								DataTargetIp:        utils.New19Byte(replicationT),
								AckTargetIp:         utils.New19Byte(gossiputils.Ip),
								ConnectionOperation: utils.WRITE,
								FileName:            utils.New1024Byte(fileName),
								OriginalFileSize:    0,
								BlockIndex:          blockIdx,
								DataSize:            0,
								IsAck:               false,
							}

							err = utils.SendTaskOnExistingConnection(task, conn)
							if err != nil {
								log.Printf("unable to send task on existing connection: ", err)
								continue
							}
							break
						}
					}
				}
			}
		}
	}

	fmt.Println("Cleaning downed node data.")
	HandleDown(DownIpAddr)
	fmt.Println("Cleaned downed node data.")
}
