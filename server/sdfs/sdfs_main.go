package sdfs

import (
	"fmt"
	"log"
	"net"
	"time"

	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitializeSdfsProcess() {

	// Initialize set of which files are being written/read from. This is to avoid concurrent access of file pointers.
	utils.FileSet = make(map[string]bool)

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

	// Decode the FollowerTask instance
	task, _ := utils.Unmarshal(conn)
	// defer conn.Close()

	// if task.isack && we're a master node, spawn a seperate master.handleAck
	if task.IsAck {
		fmt.Println("Recieved new ack connection!")
		machineType := gossiputils.MachineType()

		if machineType == gossiputils.LEADER && task.ConnectionOperation != utils.GET_2D {
			fmt.Printf("Recieved ack for %s at master\n", utils.BytesToString(task.FileName[:]))

			RouteToSubMasters(*task)
		} else if machineType == gossiputils.SUB_LEADER {
			fmt.Printf("Recieved ack for %s at SUBmaster\n", utils.BytesToString(task.FileName[:]))
		}

		HandleAck(*task, &conn)

	} else if task.ConnectionOperation == utils.DELETE {
		HandleDeleteConnection(*task)
	} else if task.ConnectionOperation == utils.WRITE || task.ConnectionOperation == utils.READ {
		HandleStreamConnection(*task, conn)
	} else if task.ConnectionOperation == utils.FORCE_GET {
		startTime := time.Now()
		fileName := utils.BytesToString(task.FileName[:])
		locations, locationErr := SdfsClientMain(fileName)
		if locationErr != nil {
			fmt.Println("Error with sdfsclient main. Aborting Get command: ", locationErr)
			return
		}
		InitiateGetCommand(fileName, fileName, locations)
		elapsedTime := time.Since(startTime)
		log.Printf("Force GET completed in: %s", elapsedTime)
	} else {
		fmt.Printf("Error: inbound task from ip %s has no specific type\n", conn.RemoteAddr().String())
	}
	// conn.Close()
}
