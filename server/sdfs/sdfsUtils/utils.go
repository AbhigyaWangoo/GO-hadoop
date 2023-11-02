package sdfsutils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
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

const (
	DELETE_OP string = "d"
	WRITE_OP  string = "w"
)

type Task struct {
	DataTargetIp        [19]byte
	AckTargetIp         [19]byte
	ConnectionOperation BlockOperation // READ, WRITE, GET_2D, OR DELETE from sdfs utils
	FileName            [1024]byte
	OriginalFileSize    int
	BlockIndex          int
	DataSize            uint32 // TODO change me to uint32
	IsAck               bool
}

const KB int = 1024
const MB int = KB * 1024
const SDFS_PORT string = "6000"
const SDFS_ACK_PORT string = "9682"
const FILESYSTEM_ROOT string = "server/sdfs/sdfsFileSystemRoot/"
const BLOCK_SIZE int = 5 * MB
const REPLICATION_FACTOR int = 4
const MAX_INT64 = 9223372036854775807
const NUM_LEADERS = 4

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

func GetFilePtr(sdfs_filename string, blockidx string, flags int) (string, int, *os.File, error) {
	// Specify the file path
	filePath := GetFileName(sdfs_filename, blockidx)
	fileSize := GetFileSize(filePath)
	fmt.Printf("File path to block", filePath)
	file, err := os.OpenFile(filePath, flags, 0666)
	if err != nil {
		// Handle the error if the file cannot be opened
		if os.IsNotExist(err) {
			fmt.Println("File does not exist.")
		}
	}

	return filePath, int(fileSize), file, err
}

// This function will buffered read from (a connection if fromLocal is false, the filepointer if fromLocal is true), and
// buffered write to (a connection if fromLocal is true, the filepointer if fromLocal is false)
func BufferedReadAndWrite(conn *bufio.ReadWriter, fp *os.File, size uint32, fromLocal bool) (uint32, error) {
	// connTCP, ok := conn.(*net.TCPConn)
	// if ok {
	// 	connTCP.SetLinger(0) // Set Linger option to flush data immediately
	// }
	var total_bytes_processed uint32 = 0
	var w *bufio.Writer
	var r *bufio.Reader
	var bufferSize uint32

	if fromLocal {
		w = conn.Writer
		r = bufio.NewReader(fp)
		bufferSize = size * 3 / 4
	} else {
		w = bufio.NewWriter(fp)
		r = conn.Reader
		bufferSize = uint32(5 * KB)
	}

	dataBuffer := make([]byte, bufferSize)

	fmt.Println("Entering buffered readwrite. buffer size: ", bufferSize)

	for {
		// fmt.Println("TRYING TO READ")
		if total_bytes_processed == size {
			fmt.Println("Read all bytes")
			break
		}

		var nRead int = 0
		var readErr error = nil
		var nWritten int64 = 0
		var writeErr error = nil

		for nRead < int(bufferSize) {
			tmp, tmperr := r.Read(dataBuffer)
			nRead += tmp
			readErr = tmperr

			tmpwritten, werr := r.WriteTo(w)
			nWritten += tmpwritten
			writeErr = werr
		}

		fmt.Println("N written: ", nWritten)
		fmt.Println("N read: ", nRead)

		if nRead == 0 && total_bytes_processed == size {
			fmt.Println("Read no bytes")
			break
		}

		if readErr != nil {
			if readErr == io.EOF {
				if total_bytes_processed < size {
					fmt.Println("bytes processed with EOF: ", total_bytes_processed)
					return total_bytes_processed, io.ErrUnexpectedEOF
				}
				break // Connection closed by the other end
			}
			return total_bytes_processed, readErr // Error while reading data
		}

		// for curbyte := 0; curbyte < nRead; curbyte++ {
		// 	writeErr = w.WriteByte(dataBuffer[curbyte])
		// 	nWritten++
		// }

		w.Flush()

		if nWritten < int64(nRead) {
			return total_bytes_processed, io.ErrShortWrite
		} else if nWritten > int64(nRead) || writeErr != nil {
			return total_bytes_processed, writeErr
		}

		if uint32(nWritten) != bufferSize {
			fmt.Println("wrote not buffer size: ", nRead)
		}

		total_bytes_processed += uint32(nWritten)
	}
	fp.Sync()
	log.Println("Processed x bytes: ", total_bytes_processed)

	return total_bytes_processed, nil
}

func SendTask(task Task, ipAddr string, ack bool) (*net.Conn, error) {
	conn, tcpOpenError := OpenTCPConnection(ipAddr, SDFS_PORT)
	if tcpOpenError != nil {
		return nil, nil
	}

	bufferConn := bufio.NewWriter(conn)

	task.IsAck = ack
	arr := task.Marshal()
	bytes_written, err := bufferConn.Write(arr)
	if err != nil {
		return nil, err
	} else if bytes_written != len(arr) {
		return nil, io.ErrShortWrite
	}
	bufferConn.Write([]byte{'\n'})
	bufferConn.Flush()

	fmt.Println("Sent task to leader ip:", ipAddr)

	return &conn, nil
}

func SendTaskOnExistingConnection(task Task, conn *bufio.ReadWriter) error {
	arr := task.Marshal()
	bytes_written, err := conn.Write(arr)
	if err != nil {
		return err
	} else if bytes_written != len(arr) {
		return io.ErrShortWrite
	}
	conn.Write([]byte{'\n'})
	conn.Flush()

	return nil
}

func SendAckToMaster(task Task) *net.Conn {
	leaderIp := GetLeader()

	fmt.Printf("detected Leader ip: %s\n", leaderIp)
	task.AckTargetIp = New19Byte(gossiputils.Ip)
	val, ok := gossiputils.MembershipMap.Get(leaderIp)
	if ok && (val.State == gossiputils.ALIVE || val.State == gossiputils.SUSPECTED) {
		conn, _ := SendTask(task, leaderIp, true)

		return conn
	} else {
		newLeader := GetLeader()
		conn, _ := SendTask(task, newLeader, true)

		return conn
	}
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

func GetKLeaders() []string {
	allKeys := gossiputils.MembershipMap.Keys()
	allMembers := make([]gossiputils.Member, 0)
	for _, key := range allKeys {
		member, _ := gossiputils.MembershipMap.Get(key)
		allMembers = append(allMembers, member)
	}
	sort.Slice(allMembers, func(i, j int) bool {
		return allMembers[i].CreationTimestamp < allMembers[j].CreationTimestamp
	})

	var kLeaders []string
	numLeaders := NUM_LEADERS
	for _, member := range allMembers {
		if numLeaders == 0 {
			break
		}
		kLeaders = append(kLeaders, member.Ip)
		numLeaders--
	}

	return kLeaders
}

func New19Byte(data string) [19]byte {
	var byteArr [19]byte
	copy(byteArr[:], []byte(data))
	return byteArr
}

func New1024Byte(data string) [1024]byte {
	var byteArr [1024]byte
	copy(byteArr[:], []byte(data))
	return byteArr
}

func BytesToString(data []byte) string {
	return strings.TrimRight(string(data), "\x00")
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
		log.Fatalf("error marshaling task: %v\n", err)
	}
	return marshaledTask
}

func Unmarshal(conn *bufio.ReadWriter) (*Task, uint32) {
	var task Task

	// Read from the connection until a newline is encountered
	data, err := conn.ReadBytes('\n')
	if err != nil {
		log.Fatalf("Error reading from connection: %v\n", err)
	}
	data = data[:len(data)-1]

	err = json.Unmarshal([]byte(data), &task)

	if err != nil {
		log.Fatalf("Error unmarshalling task: %v\n", err)
	}

	return &task, uint32(len(data))
}

func MarshalBlockLocationArr(array [][]string) []byte {
	jsonData, err := json.Marshal(array)
	if err != nil {
		log.Fatalf("error marshaling 2d arr: %v\n", err)
	}
	return jsonData
}

func UnmarshalBlockLocationArr(conn *bufio.ReadWriter) ([][]string, error) {
	var locations [][]string

	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&locations)

	if err != nil {
		log.Fatalf("Error unmarshalling 2d arr: %v\n", err)
		return nil, err
	}

	return locations, nil
}

func SendSmallAck(conn *bufio.ReadWriter) {
	_, err := conn.Write([]byte("A"))
	if err != nil {
		log.Fatalln("err: ", err)
	}
	conn.Flush()
}

func ReadSmallAck(conn *bufio.ReadWriter) {
	buffer := make([]byte, 1)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Print("Error reading from connection: ", err)
			break
		}
		if n > 0 {
			break
		}
	}
}
