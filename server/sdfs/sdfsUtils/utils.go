package sdfsutils

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
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
	DataTargetIp        [16]byte
	AckTargetIp         [16]byte
	ConnectionOperation BlockOperation // READ, WRITE, GET_2D, OR DELETE from sdfs utils
	FileName            [1024]byte
	FileNameLength      int
	BlockIndex          int
	DataSize            uint32 // TODO change me to uint32
	IsAck               bool
}

const KB int = 1024
const MB int = KB * 1024
const SDFS_PORT string = "3541"
const SDFS_ACK_PORT string = "9682"
const FILESYSTEM_ROOT string = "server/sdfs/sdfsFileSystemRoot/"
const BLOCK_SIZE int = 128 * MB
const REPLICATION_FACTOR int = 4
const MAX_INT64 = 9223372036854775807

var MuLocalFs sync.Mutex
var CondLocalFs = sync.NewCond(&MuLocalFs)
var LEADER_IP string = "172.22.158.162"

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

	tcpConn, listenErr := net.Listen("tcp", ":"+Port)
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
	return fmt.Sprintf("%s%s_%s", FILESYSTEM_ROOT, blockidx, sdfs_filename)
}

func GetFilePtr(sdfs_filename string, blockidx string, flags int) (*os.File, error) {
	// Specify the file path
	filePath := GetFileName(sdfs_filename, blockidx)

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
func BufferedReadAndWrite(conn net.Conn, fp *os.File, size uint32, fromLocal bool) error {
	var total_bytes_read uint32 = 0
	bufferSize := 4 * KB
	dataBuffer := make([]byte, bufferSize)

	fmt.Println("Entering buffered readwrite. File size: ", size)

	for {
		if total_bytes_read == size {
			fmt.Println("Read all bytes")
			break
		}

		var nRead int = 0
		var readErr error = nil

		if fromLocal {
			nRead, readErr = fp.Read(dataBuffer)
		} else {
			nRead, readErr = conn.Read(dataBuffer)
		}

		if nRead == 0 {
			fmt.Println("Read no bytes")
			break
		}

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

		if nWritten < nRead {
			return io.ErrShortWrite
		} else if nWritten > nRead || writeErr != nil {
			return writeErr
		}

		total_bytes_read += uint32(nRead)
	}

	return nil
}

func SendTask(task Task, ipAddr string, ack bool) error {
	conn, tcpOpenError := OpenTCPConnection(ipAddr, SDFS_PORT)
	if tcpOpenError != nil {
		return nil
	}
	defer conn.Close()

	task.IsAck = ack
	arr := task.Marshal()
	bytes_written, err := conn.Write(arr)
	if err != nil {
		return err
	} else if bytes_written != len(arr) {
		return io.ErrShortWrite
	}

	return nil
}

func GetLeader() string {
	var oldestTime int64 = MAX_INT64
	var oldestMemberIp string

	allKeys := gossiputils.MembershipMap.Keys()
	for key := range allKeys {
		member, exist := gossiputils.MembershipMap.Get(allKeys[key])

		if exist && member.CreationTimestamp < int64(oldestTime) {
			oldestMemberIp = member.Ip
			oldestTime = member.CreationTimestamp
		}
	}

	return oldestMemberIp
}

func New16Byte(data string) [16]byte {
	var byteArr [16]byte
	copy(byteArr[:], []byte(data))
	return byteArr
}

func New1024Byte(data string) [1024]byte {
	var byteArr [1024]byte
	copy(byteArr[:], []byte(data))
	return byteArr
}

func BytesToString(data interface{}) string {
	if byteArr, ok := data.([]byte); ok {
		return strings.TrimRight(string(byteArr), "\x00")
	}
	return ""
}

func GetBlockPosition(blockNumber int64, fileSize int64) (int64, int64) {
	currentByteIdx := blockNumber * int64(BLOCK_SIZE)
	blockSize := GetMinInt64(fileSize-currentByteIdx, int64(BLOCK_SIZE))
	return currentByteIdx, blockSize
}

func GetMinInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (task Task) Marshal() []byte {
	marshaledTask, err := json.Marshal(task)
	if err != nil {
		log.Fatalf("error marshaling data: ", err)
	}
	return marshaledTask
}

func Unmarshal(conn net.Conn) *Task {
	var task Task
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&task)

	if err != nil {
		log.Fatalf("Error reading:", err)
	}

	return &task
}
