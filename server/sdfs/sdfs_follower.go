package sdfs

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"

	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

var FileSet map[int]bool

type FollowerTask struct {
	DataTargetIp      string
	AckTargetIp       string
	ConnectionCommand utils.Command // PUT, GET, OR DELETE from sdfs utils
	FileName          string
	BlockIndex        int
	DataSize          int
}

func InitializeFollower() {

	// Initialize set of which files are being written/read from. This is to avoid concurrent access of file pointers.
	FileSet = make(map[int]bool)

	serverAddr, resolveErr := net.ResolveTCPAddr("tcp", utils.SDFS_PORT)
	if resolveErr != nil {
		fmt.Println("Error resolving address:", resolveErr)
		os.Exit(1)
	}

	tcpConn, listenErr := net.ListenTCP("tcp", serverAddr)
	if listenErr != nil {
		fmt.Println("Error listening:", listenErr)
		os.Exit(1)
	}
	defer tcpConn.Close()

	fmt.Println("sdfs client is listening on", serverAddr)

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
	var task FollowerTask
	err := decoder.Decode(&task)
	if err != nil {
		fmt.Println("Error decoding:", err)
		return
	}

	if task.ConnectionCommand == utils.DELETE {
		HandleDeleteConnection(task)
	} else if task.ConnectionCommand == utils.WRITE {
		HandlePutConnection(task, conn)
	} else if task.ConnectionCommand == utils.READ {
		HandleGetConnection(task)
	} else {
		fmt.Printf("Error: inbound task from ip %s has no specific type", conn.RemoteAddr().String())
	}
}

func HandlePutConnection(Task FollowerTask, conn net.Conn) {
	// TODO: given the filename.blockidx, this function needs to buffered read a Task.DataSize amount of data from the
	// connection, all as one block (buffered read, ofc) and write it to the file sdfs/data/filename.blockidx. Once that block is
	// Successfully written, this function should alert the Task.AckTargetIp that this operation was successfully completed in addition
	// to terminating the connection. Additionally, if another thread is currently reading/writing to a block, this should block until 
	// that operation is done. When this thread does end up in the middle of a write, it must mark that particular file as being written to 
	// in the FileSet map.
	// Used for the PUT command
	fmt.Println("Recieved a request to write to this node")
}

func HandleDeleteConnection(Task FollowerTask) {
	// TODO: given the filename.blockidx, this function needs to delete the provided file from sdfs/data/filename.blockidx. Once
	// that block is successfully deleted, this function should alert the Task.AckTargetIp that this operation was successfully
	// completed in addition to terminating the connection. Additionally, if another thread is currently reading/writing to a block, this should block until 
	// that operation is done. When the thread does end up in the middle of a buffered read, it must mark that particular file as being read from to 
	// in the map.

	// On a failure case, like block dne, do not send the ack.

	// Used for delete command
	fmt.Println("Recieved a request to delete some block on this node")
}

func HandleGetConnection(Task FollowerTask) {
	// TODO: provided with a task struct, this function should take the filename.blockidx specified by the task struct,
	// buffered read + buffered write it to the Task.DataTargetIp provided in the form of a WRITE Task. Also, this function needs 
	// to send an ack to the Task.AckTargetIp. Additionally, if another thread is currently reading/writing to a block, this should block until 
	// that operation is done. When the thread does end up in the middle of a buffered read, it must mark that particular file as being read from to 
	// in the map.
	// Used for Get command, and Re-replication. Master will send a put command, with itself as the ack target, the machine to 
	// replicate to as the Task.DataTargetIp
	fmt.Println("Recieved a request to get some block from this node")
}
