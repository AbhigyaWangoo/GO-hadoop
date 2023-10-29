package sdfs

import (
	"fmt"
	"net"

	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitializeSdfsProcess() {

	// Initialize set of which files are being written/read from. This is to avoid concurrent access of file pointers.
	FileSet = make(map[string]bool)

	tcpConn, listenError := utils.ListenOnTCPConnection(utils.SDFS_PORT)
	if listenError != nil {
		fmt.Printf("Error listening on port %s", utils.SDFS_PORT)
		return
	}
	defer tcpConn.Close()

	fmt.Println("sdfs client is listening on local machine")

	for {
		// Read data from the TCP connection
		conn, err := tcpConn.Accept()
		// conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

		if err != nil {
			fmt.Println("Error accepting tcp connection:", err)
			continue
		}

		go HandleConnection(conn)
	}
}

func HandleConnection(conn net.Conn) {

	fmt.Println("Recieved new connection!")

	// Create a decoder for the connection
	// decoder := gob.NewDecoder(conn)

	// Decode the FollowerTask instance
	task := utils.Unmarshal(conn)

	// task := utils.Task{
	// 	DataTargetIp:        utils.New16Byte("192.168.0.1"),
	// 	AckTargetIp:         utils.New16Byte(utils.LEADER_IP),
	// 	ConnectionOperation: utils.WRITE, // Assuming BlockOperation is a string alias
	// 	FileName:            utils.New1024Byte("1_mb_put.txt"),
	// 	FileNameLength:      12,
	// 	BlockIndex:          0,
	// 	DataSize:            1048576,
	// 	IsAck:               false,
	// }

	// if task.isack && we're a master node, spawn a seperate master.handleAck
	if task.IsAck {
		machineType := MachineType()
		if machineType == gossiputils.LEADER {
			//
		}
	}

	if task.ConnectionOperation == utils.DELETE {
		HandleDeleteConnection(*task)
	} else if task.ConnectionOperation == utils.WRITE {
		HandlePutConnection(*task, conn)
	} else if task.ConnectionOperation == utils.READ {
		HandleGetConnection(*task)
	} else {
		fmt.Printf("Error: inbound task from ip %s has no specific type", conn.RemoteAddr().String())
	}
}
