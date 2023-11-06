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

var readWriteHistory int = 0
var nActiveWriters uint
var nActiveReaders uint

func HandleStreamConnection(Task utils.Task, conn net.Conn) error {
	// TODO for rereplication, if the src in the conn object == master, then we have to open a new connection to send data over that connection. The
	// new conection should point to datatargetip

	utils.SendSmallAck(conn)

	fmt.Println("Entering edit connection")

	var FileName string = utils.BytesToString(Task.FileName[:])
	var TargetIp string = utils.BytesToString(Task.DataTargetIp[:])
	var flags int

	if TargetIp != gossiputils.Ip {
		fmt.Println("Recived replication request. Attempting to put specified block to target ip.")
		PutBlock(FileName, Task.BlockIndex, TargetIp, Task.OriginalFileSize)
		return nil
	}

	if Task.ConnectionOperation == utils.WRITE {
		flags = os.O_CREATE | os.O_WRONLY
		nActiveWriters++
	} else if Task.ConnectionOperation == utils.READ {
		flags = os.O_CREATE | os.O_RDONLY
		nActiveReaders++
	}

	localFilename, fileSize, fp, err := utils.GetFilePtr(FileName, strconv.FormatInt(Task.BlockIndex, 10), flags)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	isRead := Task.ConnectionOperation == utils.READ || Task.ConnectionOperation == utils.FORCE_GET
	isWrite := Task.ConnectionOperation == utils.WRITE

	utils.MuLocalFs.Lock()
	
	if isRead {
		nActiveReaders++
	} else if isWrite {
		fmt.Println("Incrementing writer count, first: ", nActiveWriters)
		nActiveWriters++
		fmt.Println("Incrementing writer count, second: ", nActiveWriters)
	}

	// For a reading thread to continue: A = isRead && (nActiveWriters == 0 || (nActiveWriters > 0 && readWriteHistory < 3))
	// For a writing thread to continue: B = isWrite && (nActiveReaders == 0 || (nActiveReaders > 0 && readWriteHistory > -3))
	// For conflict file pointers: C = !FileSet[localFilename]
	// All together: C && (A || B) -> !C || (!A && !B)

	for utils.FileSet[localFilename] || (!(isRead && (nActiveWriters == 0 || (nActiveWriters > 0 && readWriteHistory < 3))) && !(isWrite && (nActiveReaders == 0 || (nActiveReaders > 0 && readWriteHistory > -3)))) {
		utils.CondLocalFs.Wait()
	}

	utils.FileSet[localFilename] = true

	fromLocal := Task.ConnectionOperation == utils.READ
	if fromLocal {
		Task.DataSize = int64(fileSize)
		utils.SendTaskOnExistingConnection(Task, conn)
		utils.ReadSmallAck(conn)
	}

	fmt.Println("Amount of data to send back: ", Task.DataSize)
	var nread int64
	var bufferedErr error
	if !fromLocal { // PUT request
		nread, bufferedErr = utils.BufferedReadFromConnection(conn, fp, Task.DataSize)
	} else { // GET request
		nread, bufferedErr = utils.BufferedWriteToConnection(conn, fp, Task.DataSize, 0)
	}
	// utils.BufferedReadAndWrite(conn, fp, Task.DataSize, fromLocal, 0)

	if bufferedErr != nil {
		fmt.Println("Error:", bufferedErr)
		utils.FileSet[localFilename] = false

		if isRead {
			nActiveReaders--
			
			if readWriteHistory < 0 {
				readWriteHistory = 0
			} else {
				readWriteHistory++
			}
	
		} else if isWrite {
			nActiveWriters--
			fmt.Println("Decrementing writer count in error flow, ", nActiveWriters)
			if readWriteHistory > 0 {
				readWriteHistory = 0
			} else {
				readWriteHistory--
			}
		}
		
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

	utils.FileSet[localFilename] = false
	
	if isRead {
		nActiveReaders--
		
		if readWriteHistory < 0 {
			readWriteHistory = 0
		} else {
			readWriteHistory++
		}

	} else if isWrite {
		fmt.Println("Decrementing writer count, first ", nActiveWriters)
		nActiveWriters--
		fmt.Println("Decrementing writer count, second ", nActiveWriters)
		
		if readWriteHistory > 0 {
			readWriteHistory = 0
		} else {
			readWriteHistory--
		}
	}

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
	
	fmt.Println("This should be false: ", utils.FileSet[localFilename])
	fmt.Printf("nreaders: %d nwriters: %d\n", nActiveReaders, nActiveWriters)

	for utils.FileSet[localFilename] || !(nActiveWriters == 0 && nActiveReaders == 0) {
		utils.CondLocalFs.Wait()
	}
	utils.FileSet[localFilename] = true

	// On a failure case, like block dne, do not send the ack.
	if err := os.Remove(localFilename); err != nil {
		if !os.IsNotExist(err) {
			fmt.Println("Error removing file:", err)

			utils.FileSet[localFilename] = false
			utils.MuLocalFs.Unlock()
			utils.CondLocalFs.Signal()

			return err
		}
	}

	utils.FileSet[localFilename] = false
	utils.MuLocalFs.Unlock()
	utils.CondLocalFs.Signal()

	utils.SendAckToMaster(Task)

	// Used for delete command
	fmt.Println("Recieved a request to delete some block on this node")
	return nil
}
