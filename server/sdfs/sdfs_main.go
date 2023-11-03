package sdfs

import (
	"bufio"
	"fmt"

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

		bufferReader := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

		go HandleConnection(bufferReader)
	}
}

func HandleConnection(conn *bufio.ReadWriter) {

	// Decode the FollowerTask instance
	task, _ := utils.Unmarshal(conn)
	// defer conn.Close()

	// if task.isack && we're a master node, spawn a seperate master.handleAck
	if task.IsAck {
		fmt.Println("Recieved new ack connection!")
		machineType := MachineType()
		if machineType == gossiputils.LEADER {
			fmt.Printf("Recieved ack for %s at master\n", utils.BytesToString(task.FileName[:]))
			HandleAck(*task, conn)

			RouteToSubMasters(*task)
		}
	} else if task.ConnectionOperation == utils.DELETE {
		HandleDeleteConnection(*task)
	} else if task.ConnectionOperation == utils.WRITE || task.ConnectionOperation == utils.READ {
		HandleStreamConnection(*task, conn)
	} else {
		// fmt.Printf("Error: inbound task from ip %s has no specific type", )
	}
	// conn.Close()
}
