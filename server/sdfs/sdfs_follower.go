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

func HandlePutConnection(Task utils.Task, conn net.Conn) error {
	// TODO: given the filename.blockidx, this function needs to buffered read a Task.DataSize amount of data from the
	// connection, all as one block (buffered read, ofc) and write it to the file sdfs/data/filename.blockidx. Once that block is
	// Successfully written, this function should alert the Task.AckTargetIp that this operation was successfully completed in addition
	// to terminating the connection. Additionally, if another thread is currently reading/writing to a block, this should block until
	// that operation is done. When this thread does end up in the middle of a write, it must mark that particular file as being written to
	// in the FileSet map.
	// Used for the PUT command
	fmt.Println("Entering put connection")
	defer conn.Close()

	var FileName string = utils.BytesToString(Task.FileName)

	fmt.Println("Filename: ", FileName)

	fp, err := utils.GetFilePtr(FileName, strconv.Itoa(Task.BlockIndex), os.O_CREATE|os.O_WRONLY)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	localFilename := utils.GetFileName(FileName, fmt.Sprint(Task.BlockIndex))

	// utils.MuLocalFs.Lock()
	// for FileSet[localFilename] {
	// 	utils.CondLocalFs.Wait()
	// }

	FileSet[localFilename] = true
	nread, bufferedErr := utils.BufferedReadAndWrite(conn, fp, Task.DataSize, false)
	if bufferedErr != nil {
		fmt.Println("Error:", bufferedErr)
		FileSet[localFilename] = false
		// utils.MuLocalFs.Unlock()
		// utils.CondLocalFs.Signal()
		return bufferedErr
	}

	log.Println("Nread: ", nread)

	FileSet[localFilename] = false
	// utils.MuLocalFs.Unlock()
	// utils.CondLocalFs.Signal()

	// SendAckToMaster(Task)
	return nil
}

func HandleDeleteConnection(Task utils.Task) error {
	// TODO: given the filename.blockidx, this function needs to delete the provided file from sdfs/data/filename.blockidx. Once
	// that block is successfully deleted, this function should alert the Task.AckTargetIp that this operation was successfully
	// completed in addition to terminating the connection. Additionally, if another thread is currently reading/writing to a block, this should block until
	// that operation is done. When the thread does end up in the middle of a buffered read, it must mark that particular file as being read from to
	// in the map.

	localFilename := utils.GetFileName(string(Task.FileName[:Task.FileNameLength]), fmt.Sprint(Task.BlockIndex))

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

func HandleGetConnection(Task utils.Task) {
	// TODO: provided with a task struct, this function should take the filename.blockidx specified by the task struct,
	// buffered read + buffered write it to the Task.DataTargetIp provided in the form of a WRITE Task. Also, this function needs
	// to send an ack to the Task.AckTargetIp. Additionally, if another thread is currently reading/writing to a block, this should block until
	// that operation is done. When the thread does end up in the middle of a buffered read, it must mark that particular file as being read from to
	// in the map.
	// Used for Get command, and Re-replication. Master will send a put command, with itself as the ack target, the machine to
	// replicate to as the Task.DataTargetIp

	// fp := GetFilePtr(Task.FileName, Task.BlockIndex, os.O_RDONLY | os.O_CREATE)
	// BufferedReadAndWriteToConnection(fp, Task)
	// SendAck(Task)

	fmt.Println("Recieved a request to get some block from this node")
}

func SendAckToMaster(Task utils.Task) {
	leaderIp := string(Task.AckTargetIp[:])
	
	fmt.Printf("detected Leader ip: %s\n", leaderIp)
	
	val, ok := gossiputils.MembershipMap.Get(leaderIp)
	if ok && (val.State == gossiputils.ALIVE || val.State == gossiputils.SUSPECTED) {
		fmt.Printf("sending Leader ip\n")
		utils.SendTask(Task, string(Task.AckTargetIp[:]), true)
	} else {
		newLeader := utils.GetLeader()
		utils.SendTask(Task, newLeader, true)
	}

}
