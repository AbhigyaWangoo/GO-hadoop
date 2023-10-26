package sdfsutils

import (
	"fmt"
	"io"
	"net"
	"os"
)

type BlockOperation int

// Potentially use send, receive, write, delete types instead types instead
const (
	READ   BlockOperation = 0
	WRITE  BlockOperation = 1
	DELETE BlockOperation = 2
	GET_2D BlockOperation = 3
)

type Task struct {
	DataTargetIp        string
	AckTargetIp         string
	ConnectionOperation BlockOperation // READ, WRITE, GET_2D, OR DELETE from sdfs utils
	FileName            string
	BlockIndex          int
	DataSize            int // TODO change me to uint32
	IsAck               bool
}

const KB int = 1024
const MB int = KB * 1024
const SDFS_PORT string = "3541"
const SDFS_ACK_PORT string = "9682"
const FILESYSTEM_ROOT string = "sdfs/sdfsFileSystemRoot/"
const BLOCK_SIZE int = 128 * MB
const REPLICATION_FACTOR int = 4

var MASTER_IP string = "172.22.158.162"

// Opens a tcp connection to the provided ip address and port, and returns the connection object
func OpenTCPConnection(IpAddr string, Port string) (net.Conn, error) {
	// Concatenate IP address and port to form the address string
	address := IpAddr + ":" + Port

	// Attempt to establish a TCP connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		// Handle error if connection fails
		fmt.Println("Error:", err)
		return nil, err
	}

	// Connection successful, return connection object and nil error
	return conn, nil
}

func ListenOnTCPConnection(Port string) (net.Listener, error) {
	serverAddr, resolveErr := net.ResolveTCPAddr("tcp", "localhost:"+Port)
	if resolveErr != nil {
		fmt.Println("Error resolving address:", resolveErr)
		os.Exit(1)
	}

	tcpConn, listenErr := net.ListenTCP("tcp", serverAddr)
	if listenErr != nil {
		fmt.Println("Error listening:", listenErr)
		os.Exit(1)
	}

	return tcpConn, nil
}

func GetFileSize(filePath string) int64 {
	// Get file information
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		fmt.Println("Error:", err)
		return -1
	}

	// Get file size in bytes
	fileSize := fileInfo.Size()

	// Print the file size
	return fileSize
}

func CeilDivide(a, b int64) int64 {
	// Perform integer division
	quotient := a / b

	// Check if there is a remainder
	remainder := a % b
	if remainder > 0 {
		// If there is a remainder, round up the quotient
		quotient++
	}

	return quotient
}

func GetFileName(sdfs_filename string, blockidx string) string {
	return fmt.Sprintf("%s_%s", blockidx, sdfs_filename)
}

func GetFilePtr(sdfs_filename string, blockidx string, flags int) (*os.File, error) {
	// Specify the file path
	filePath := GetFileName(sdfs_filename, blockidx)

	// Open the file with read-write permissions and create it if it doesn't exist
	file, err := os.OpenFile(filePath, flags, 0666)
	if err != nil {
		// Handle the error if the file cannot be opened
		if os.IsNotExist(err) {
			fmt.Println("File does not exist.")
		}
	}

	return file, err
}

// This function will buffered read from (a connection if fromLocal is false, the filepointer if fromLocal is true), and 
// buffered write to (a connection if fromLocal is true, the filepointer if fromLocal is false)
func BufferedReadAndWrite(conn net.Conn, fp *os.File, size int, fromLocal bool) error {
	var bytes_read int = 0
	var total_bytes_read int = 0
	bufferSize := 4 * KB
	dataBuffer := make([]byte, bufferSize)

	fmt.Println("Entering buffered readwrite")

	for {
		if bytes_read == 0 && total_bytes_read == size {
			break
		}

		var nRead int = 0
		var readErr error = nil
		
		if fromLocal {
			nRead, readErr = fp.Read(dataBuffer)
		} else {
			nRead, readErr = conn.Read(dataBuffer)
		}

		// fmt.Printf("data: %s, num read: %d\n", string(dataBuffer[:nRead]), nRead)
		if readErr != nil {
			if readErr == io.EOF {
				if total_bytes_read < size {
					return io.ErrUnexpectedEOF
				}
				break // Connection closed by the other end
			}
			return readErr // Error while reading data
		}


		var nWritten int = 0 
		var writeErr error = nil
		
		if fromLocal {
			nWritten, writeErr = conn.Write(dataBuffer[:nRead])
		} else {
			nWritten, writeErr = fp.Write(dataBuffer[:nRead])
		}

		// fmt.Printf("num written: %d\n", nWritten)
		if nWritten < nRead {
			return io.ErrShortWrite
		} else if nWritten > nRead || writeErr != nil {
			return writeErr
		}

		bytes_read += nRead
		total_bytes_read += nRead

		// fmt.Println("P: ", nWritten)
		// fmt.Println("P: ", total_bytes_read)
	}

	return nil
}

func SendAck(task Task) error {
	conn, tcpOpenError := OpenTCPConnection(task.AckTargetIp, SDFS_PORT)
	if tcpOpenError != nil {
		return nil
	}
	defer conn.Close()

	task.IsAck = true

	// send ack to connection

	return nil
}