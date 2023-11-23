package maplejuiceutils

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
)

type MapleJuiceType int
type PartitioningType int

const HASH PartitioningType = 0
const RANGE PartitioningType = 1
const MAPLE MapleJuiceType = 0
const JUICE MapleJuiceType = 1

const MAPLE_JUICE_PORT = "1234"

type MapleJuiceTask struct {
	Type            MapleJuiceType
	NodeDesignation uint32 // The 'index' of the node recieving the maplejuice task
	SdfsPrefix      string
	SdfsExecFile    string // The name of the executable that exists in sdfs
	NumberOfMJTasks uint32
	// We also need to somehow track
}

func (task MapleJuiceTask) Marshal() []byte {
	marshaledTask, err := json.Marshal(task)
	if err != nil {
		log.Fatalf("error marshaling task: %v\n", err)
	}
	return marshaledTask
}

func UnmarshalMapleJuiceTask(conn net.Conn) (*MapleJuiceTask, int64) {
	var task MapleJuiceTask

	buffConn := bufio.NewReader(conn)

	// Read from the connection until a newline is encountered
	data, err := buffConn.ReadBytes('\n')
	if err != nil {
		log.Fatalf("Error reading from connection: %v\n", err)
	}
	data = data[:len(data)-1]

	err = json.Unmarshal([]byte(data), &task)

	if err != nil {
		log.Fatalf("Error unmarshalling task: %v\n", err)
	}

	return &task, int64(len(data))
}

func ReadAllDataFromConn(conn net.Conn, outputFileName string) {
	fp := OpenFile(outputFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	defer fp.Close()
	n, err := io.Copy(fp, conn)
	log.Println("Number of bytes read: ", n)
	if err != nil {
		log.Println("Error copying data:", err)
	}

	log.Println("Data copied successfully")
}

func OpenFile(fileName string, permissions int) *os.File {
	fp, err := os.OpenFile(fileName, permissions, 0644)
	if err != nil {
		log.Fatalf("Error opening or creating the file:", err)
		return nil
	}
	return fp
}
