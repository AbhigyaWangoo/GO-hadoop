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
	n, m := utils.CeilDivide(FileSize, int64(utils.BLOCK_SIZE)), utils.REPLICATION_FACTOR // Size of the 2D array (n rows, m columns)
	newEntry := make([][]string, n)                                                       // Create a slice of slices (2D array)

	// Populate the 2D array with arbitrary values
	var i int64
	for i = 0; i < n; i++ {
		newEntry[i] = make([]string, m)
		for j := 0; j < m; j++ {
			// Assign sentinal values to the 2D array
			newEntry[i][j] = utils.WRITE_OP
		}
	}

	BlockLocations.Set(Filename, newEntry)
}

// Master functions
func RouteToSubMasters(IncomingAck utils.Task) {
	// Route an incoming ack that makes a change to the membership list to the submasters.(Bully git issue)
	kLeaders := utils.GetKLeaders()
	for _, leader := range kLeaders[1:] {
		utils.SendTask(IncomingAck, leader, true)
	}
}

// checks current machine's IP addr in gossip's MembershipMap, returns whether current machine is leader, subleader, or follower
func MachineType() gossiputils.SdfsNodeType {
	kleaders := utils.GetKLeaders()
	leader := utils.GetLeader()

	thisIp := gossiputils.Ip

	if thisIp == leader {
		return gossiputils.LEADER
	}

	for i := 0; i < len(kleaders); i++ {
		if thisIp == kleaders[i] {
			return gossiputils.SUB_LEADER
		}
	}

	return gossiputils.FOLLOWER
}

func HandleAck(IncomingAck utils.Task, conn net.Conn) error {
	
	if !IncomingAck.IsAck {
		return errors.New("ack passed to master for processing was not actually an ack")
	}

	fileName := utils.BytesToString(IncomingAck.FileName[:IncomingAck.FileNameLength])
	ackSourceIp := utils.BytesToString(IncomingAck.AckTargetIp[:19])
	
	if IncomingAck.ConnectionOperation == utils.WRITE {
		
		fmt.Printf("Got ack for write, the ack source is >{%s}<\n", ackSourceIp)
		fmt.Println("Got ack for write, filename is ", fileName)
		fmt.Println("Got ack for write, File size is ", IncomingAck.OriginalFileSize)
		
		// RouteToSubMasters(IncomingAck)
		
		if !BlockLocations.Has(fileName) {
			fmt.Println("Never seen before filename, creating block locations entry")
			InitializeBlockLocationsEntry(fileName, int64(IncomingAck.OriginalFileSize))
		}

		blockMap, _ := BlockLocations.Get(fileName)
		for i := 0; i < utils.REPLICATION_FACTOR; i++ {
			if blockMap[IncomingAck.BlockIndex][i] == utils.WRITE_OP {
				blockMap[IncomingAck.BlockIndex][i] = ackSourceIp
				break
			}
		}
		BlockLocations.Set(fileName, blockMap)

		if mapping, ok := FileToBlocks.Get(ackSourceIp); ok { // IPaddr : [[blockidx, filename]]
			mapping = append(mapping, [2]interface{}{IncomingAck.BlockIndex, fileName})
			FileToBlocks.Set(ackSourceIp, mapping)
		} else {
			initialMapping := make([][2]interface{}, 1)
			initialMapping[0] = [2]interface{}{IncomingAck.BlockIndex, fileName}
			FileToBlocks.Set(ackSourceIp, initialMapping)
		}
	} else if IncomingAck.ConnectionOperation == utils.GET_2D {
		Handle2DArrRequest(fileName, conn)
	} else if IncomingAck.ConnectionOperation == utils.DELETE {
		if !BlockLocations.Has(fileName) {
			return errors.New("Never seen before filename, dropping delete operation")
		}
		
		blockMap, _ := BlockLocations.Get(fileName)
		row := blockMap[IncomingAck.BlockIndex]
		for i := 0; i < utils.REPLICATION_FACTOR; i++ { 
			if row[i] == ackSourceIp {
				blockMap[IncomingAck.BlockIndex][i] = utils.DELETE_OP
			}
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
	if !exists {
		log.Fatalln("Block location filename dne")
	}

	marshalledArray := utils.MarshalBlockLocationArr(arr)
	_, err := conn.Write(marshalledArray)
	if err != nil {
		log.Fatalf("Error writing 2d arr to conn: %v\n", err)
	}
}

func HandleReReplication(DownIpAddr string) {

}
