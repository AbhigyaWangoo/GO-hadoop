package sdfs

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"

	utils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

var FileSet map[string]bool

func HandleStreamConnection(Task utils.Task, conn *bufio.ReadWriter) error {
	// TODO for rereplication, if the src in the conn object == master, then we have to open a new connection to send data over that connection. The
	// new conection should point to datatargetip

	utils.SendSmallAck(conn)

	fmt.Println("Entering edit connection")

	var FileName string = utils.BytesToString(Task.FileName[:])
	var flags int

	if Task.ConnectionOperation == utils.WRITE {
		flags = os.O_CREATE | os.O_WRONLY
	} else if Task.ConnectionOperation == utils.READ {
		flags = os.O_CREATE | os.O_RDONLY
	}

	localFilename, fileSize, fp, err := utils.GetFilePtr(FileName, strconv.FormatUint(Task.BlockIndex, 10), flags)
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
	if fromLocal {
		Task.DataSize = uint64(fileSize)
		utils.SendTaskOnExistingConnection(Task, conn)
		utils.ReadSmallAck(conn)
	}

	fmt.Println("Amount of data to send back: ", Task.DataSize)
	nread, bufferedErr := utils.BufferedReadAndWrite(conn, fp, Task.DataSize, fromLocal)

	if bufferedErr != nil {
		fmt.Println("Error:", bufferedErr)
		FileSet[localFilename] = false

		utils.MuLocalFs.Unlock()
		utils.CondLocalFs.Signal()

		if !fromLocal {
			os.Remove(localFilename) // Remove file if it failed half way through
		}

		// Close the connection with an error here somehow.
		return bufferedErr
	}
	if !fromLocal {
		utils.SendSmallAck(conn)
	}

	log.Println("Nread: ", nread)

	FileSet[localFilename] = false
	utils.MuLocalFs.Unlock()
	utils.CondLocalFs.Signal()

	if Task.ConnectionOperation != utils.READ {
		utils.SendAckToMaster(Task)
	}

	return nil
}

func HandleDeleteConnection(Task utils.Task) error {
	// TODO: given the filename.blockidx, this function needs to delete the provided file from sdfs/data/filename.blockidx. Once
	// that block is successfully deleted, this function should alert the Task.AckTargetIp that this operation was successfully
	// completed in addition to terminating the connection. Additionally, if another thread is currently reading/writing to a block, this should block until
	// that operation is done. When the thread does end up in the middle of a buffered read, it must mark that particular file as being read from to
	// in the map.

	localFilename := utils.GetFileName(utils.BytesToString(Task.FileName[:]), fmt.Sprint(Task.BlockIndex))

	utils.MuLocalFs.Lock()
	for FileSet[localFilename] {
		utils.CondLocalFs.Wait()
	}
	FileSet[localFilename] = true

	// On a failure case, like block dne, do not send the ack.
	if err := os.Remove(localFilename); err != nil {
		if !os.IsNotExist(err) {
			fmt.Println("Error removing file:", err)

			FileSet[localFilename] = false
			utils.MuLocalFs.Unlock()
			utils.CondLocalFs.Signal()

			return err
		}
	}

	FileSet[localFilename] = false
	utils.MuLocalFs.Unlock()
	utils.CondLocalFs.Signal()

	utils.SendAckToMaster(Task)

	// Used for delete command
	fmt.Println("Recieved a request to delete some block on this node")
	return nil
}
