package sdfs

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

var FileSet map[string]bool

func HandleStreamConnection(Task utils.Task, conn net.Conn) error {
	// TODO for rereplication, if the src in the conn object == master, then we have to open a new connection to send data over that connection. The
	// new conection should point to datatargetip

	fmt.Println("Entering edit connection")
	defer conn.Close()

	var FileName string = utils.BytesToString(Task.FileName[:Task.FileNameLength])
	var flags int

	if Task.ConnectionOperation == utils.WRITE {
		flags = os.O_CREATE | os.O_WRONLY
	} else if Task.ConnectionOperation == utils.READ {
		flags = os.O_CREATE | os.O_RDONLY
	}

	localFilename, fp, err := utils.GetFilePtr(FileName, strconv.Itoa(Task.BlockIndex), flags)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	utils.MuLocalFs.Lock()
	for FileSet[localFilename] {
		utils.CondLocalFs.Wait()
	}

	FileSet[localFilename] = true
	fromLocal := Task.ConnectionOperation == utils.READ
	nread, bufferedErr := utils.BufferedReadAndWrite(conn, fp, Task.DataSize, fromLocal)
	if bufferedErr != nil {
		fmt.Println("Error:", bufferedErr)
		FileSet[localFilename] = false

		utils.MuLocalFs.Unlock()
		utils.CondLocalFs.Signal()

		if !fromLocal {
			os.Remove(localFilename) // Remove file if it failed half way through
		}

		// Close the connection with an error
		return bufferedErr
	}

	log.Println("Nread: ", nread)

	FileSet[localFilename] = false
	utils.MuLocalFs.Unlock()
	utils.CondLocalFs.Signal()

	SendAckToMaster(Task)

	return nil
}

func HandleDeleteConnection(Task utils.Task) error {
	// TODO: given the filename.blockidx, this function needs to delete the provided file from sdfs/data/filename.blockidx. Once
	// that block is successfully deleted, this function should alert the Task.AckTargetIp that this operation was successfully
	// completed in addition to terminating the connection. Additionally, if another thread is currently reading/writing to a block, this should block until
	// that operation is done. When the thread does end up in the middle of a buffered read, it must mark that particular file as being read from to
	// in the map.

	localFilename := utils.GetFileName(utils.BytesToString(Task.FileName[:Task.FileNameLength]), fmt.Sprint(Task.BlockIndex))

	utils.MuLocalFs.Lock()
	for FileSet[localFilename] {
		utils.CondLocalFs.Wait()
	}
	FileSet[localFilename] = true

	// On a failure case, like block dne, do not send the ack.
	if err := os.Remove(localFilename); err != nil {
		fmt.Println("Unable to process delete request: ", err)
		return err
	}

	FileSet[localFilename] = false
	utils.MuLocalFs.Unlock()
	utils.CondLocalFs.Signal()

	SendAckToMaster(Task)

	// Used for delete command
	fmt.Println("Recieved a request to delete some block on this node")
	return nil
}

func SendAckToMaster(Task utils.Task) *net.Conn {
	leaderIp := utils.GetLeader()

	fmt.Printf("detected Leader ip: %s\n", leaderIp)

	val, ok := gossiputils.MembershipMap.Get(leaderIp)
	if ok && (val.State == gossiputils.ALIVE || val.State == gossiputils.SUSPECTED) {
		fmt.Printf("sending Leader ip\n")

		conn, _ := utils.SendTask(Task, utils.BytesToString(Task.AckTargetIp), true)

		return conn
	} else {
		newLeader := utils.GetLeader()
		conn, _ := utils.SendTask(Task, newLeader, true)

		return conn
	}
}
