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
	return gossiputils.LEADER
}

func HandleAck(IncomingAck utils.Task, conn net.Conn) error {
	if !IncomingAck.IsAck {
		return errors.New("ack passed to master for processing was not actually an ack")
	}
	fileName := utils.BytesToString(IncomingAck.FileName[:])
	// fmt.Printf("Hi: ", fileName)
	ackSourceIp := utils.BytesToString(IncomingAck.DataTargetIp[:])
	if IncomingAck.ConnectionOperation == utils.WRITE {
		utils.SendSmallAck(conn)
		RouteToSubMasters(IncomingAck)
		if !BlockLocations.Has(fileName) {
			InitializeBlockLocationsEntry(fileName, int64(IncomingAck.OriginalFileSize))
			log.Printf("WRITTTEEEE")
		}

		blockMap, _ := BlockLocations.Get(fileName)
		for i := 0; i < utils.REPLICATION_FACTOR; i++ {
			if blockMap[IncomingAck.BlockIndex][i] == utils.WRITE_OP {
				blockMap[IncomingAck.BlockIndex][i] = ackSourceIp
				break
			}
		}
		log.Printf("WRITTTEEEE")
		BlockLocations.Set(fileName, blockMap)
		log.Printf(utils.BytesToString(IncomingAck.DataTargetIp[:]))
		sourceIp := utils.BytesToString(IncomingAck.DataTargetIp[:])
		if !FileToBlocks.Has(sourceIp) {
			log.Printf("WRITTTEEEE")
			initialMapping := make([][2]interface{}, 1)
			initialMapping[0] = [2]interface{}{IncomingAck.BlockIndex, ackSourceIp}
			FileToBlocks.Set(ackSourceIp, initialMapping)
		} else {
			mapping, _ := FileToBlocks.Get(sourceIp)
			log.Printf("WRITTTEEEE")
			mapping = append(mapping, [2]interface{}{IncomingAck.BlockIndex, ackSourceIp})
			FileToBlocks.Set(ackSourceIp, mapping)
		}
		// if mapping, ok := FileToBlocks.Get(utils.BytesToString(IncomingAck.DataTargetIp[:])); ok {
		// 	log.Printf("WRITTTEEEE")
		// 	mapping = append(mapping, [2]interface{}{IncomingAck.BlockIndex, ackSourceIp})
		// 	FileToBlocks.Set(ackSourceIp, mapping)
		// } else {
		// 	log.Printf("WRITTTEEEE")
		// 	initialMapping := make([][2]interface{}, 1)
		// 	initialMapping[0] = [2]interface{}{IncomingAck.BlockIndex, ackSourceIp}
		// 	FileToBlocks.Set(ackSourceIp, initialMapping)
		// }
	} else if IncomingAck.ConnectionOperation == utils.GET_2D {
		fmt.Printf("HELLOOOO")
		// fileName := utils.BytesToString(IncomingAck.FileName)
		// task := utils.Task{
		// 	DataTargetIp:        utils.New16Byte("-1"),
		// 	AckTargetIp:         utils.New16Byte("-1"),
		// 	ConnectionOperation: utils.READ,
		// 	FileName:            IncomingAck.FileName,
		// 	OriginalFileSize:    -1,
		// 	BlockIndex:          -1,
		// 	DataSize:            0,
		// 	IsAck:               false,
		// }
		// utils.SendTaskOnExistingConnection(task, conn)
		// fileName = "1mb.log"
		Get2dArr(fileName, conn)
		// conn.Close()
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

func Get2dArr(Filename string, conn net.Conn) {
	// Reply to a connection with the 2d array for the provided filename. Hardcoded for now.

	// InitializeBlockLocationsEntry(Filename, FileSize) // TODO HARDCODED, CHANGE ME

	// a := [][]string{
	// 	{"0", "1", "2", "3"},
	// 	{"4", "5", "6", "7"},
	// }

	// fmt.Println("Filename: ", Filename)
	// BlockLocations.Set("1mb.log", a)
	arr, exists := BlockLocations.Get(Filename)
	if !exists {
		log.Fatalln("Block location filename dne")
	}

	marshalledArray := utils.MarshalBlockLocationArr(arr)
	_, err := conn.Write(marshalledArray)
	if err != nil {
		log.Fatalf("Error writing 2d arr to conn: %v\n", err)
	}
	fmt.Printf(string(marshalledArray))
}

func HandleReReplication(DownIpAddr string) {

}

// 1048576 - 1047287 - 3584
