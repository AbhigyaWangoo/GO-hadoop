package sdfs

import (
	"encoding/gob"
	"fmt"
	"net"

	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitializeSdfsProcess() {

	// Initialize set of which files are being written/read from. This is to avoid concurrent access of file pointers.
	FileSet = make(map[int]bool)

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
			fmt.Println("Error reading:", err)
			continue
		}

		go HandleConnection(conn)
	}
}

func HandleConnection(conn net.Conn) {
	// Create a decoder for the connection
	decoder := gob.NewDecoder(conn)

	// Decode the FollowerTask instance
	var task utils.Task
	err := decoder.Decode(&task)
	if err != nil {
		fmt.Println("Error decoding:", err)
		return
	}

	// if task.isack && we're a master node, spawn a seperate master.handleAck

	if task.ConnectionOperation == utils.DELETE {
		HandleDeleteConnection(task)
	} else if task.ConnectionOperation == utils.WRITE {
		HandlePutConnection(task, conn)
	} else if task.ConnectionOperation == utils.READ {
		HandleGetConnection(task)
	} else {
		fmt.Printf("Error: inbound task from ip %s has no specific type", conn.RemoteAddr().String())
	}
}