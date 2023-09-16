package distributedgrepclient

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var SERVER_TCP_PORT string = ":5432"
var CLIENT_TCP_PORT string = ":2345"
var MAX_TCP_DATA_LEN int = 80000000

var nodes map[string]int
var mu sync.Mutex

func InitializeClient() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run client.go <grep pattern>")
		return
	}
	machine_file := "../logs/machine.txt"

	content, read_err := os.ReadFile(machine_file)
	if read_err != nil {
		fmt.Println("SERVER:99 = Error:", read_err)
		return
	}

	// Convert the file content to an integer
	machine_number := string(content[0])
	grepPattern := os.Args[1]

	// HARDCODED IP ADDRESSES OF ALL MACHINES. TODO not sure if the ip addresses change if vm is powered off?
	nodes = make(map[string]int)
	nodes["172.22.156.162"] = 1
	nodes["172.22.158.162"] = 2
	nodes["172.22.94.162"] = 3
	nodes["172.22.156.163"] = 4
	nodes["172.22.158.163"] = 5
	nodes["172.22.94.163"] = 6
	nodes["172.22.156.164"] = 7
	nodes["172.22.158.164"] = 8
	nodes["172.22.94.164"] = 9
	nodes["172.22.156.165"] = 10

	// TODO seperate thread to execute grep on local file
	intValue, convert_err := strconv.Atoi(machine_number)
	if convert_err != nil {
		fmt.Println("Error converting string to int:", convert_err)
		return
	}

	ExecuteGrep(grepPattern, intValue)
	Client(grepPattern, machine_number)
}

func Client(grep_command string, machine_num string) {
	var wg sync.WaitGroup

	mu.Lock()
	for key, value := range nodes {
		if fmt.Sprint(value) != machine_num {

			wg.Add(1)

			go func(innerKey string) {
				SendGrepRequest(innerKey, grep_command)
				wg.Done()
			}(key)
		}
	}
	mu.Unlock()

	wg.Wait()
	// fmt.Println("Reached end of client")
}

func HandleTCPData(conn net.Conn) {
	r := bufio.NewReader(conn)
	chunkSize := 10000 * 1024 // 1 mB
	buffer := make([]byte, chunkSize)
	receivedData := ""
	totalBytesRead := 0
	defer conn.Close()

	for {
		bytesRead, readErr := r.Read(buffer)
		totalBytesRead += bytesRead
		if bytesRead == 0 || readErr == io.EOF {
			break
		}

		if readErr != nil {
			fmt.Println("Error in handling tcp data:", readErr)
			return
		}

		receivedData += string(buffer[:bytesRead])
	}

	remoteAddr := conn.RemoteAddr().String()
	host, _, _ := net.SplitHostPort(remoteAddr)
	ipaddr := host

	PrettyPrint(receivedData, nodes[ipaddr])
}

func SendGrepRequest(target_server_addr string, grep_command string) {

	conn, dial_err := net.DialTimeout("tcp", target_server_addr+SERVER_TCP_PORT, 1000*time.Millisecond)
	if dial_err != nil {

		mu.Lock()
		// id := nodes[target_server_addr]
		// fmt.Printf("Node %d is down\n", id)
		delete(nodes, target_server_addr)
		mu.Unlock()
		fmt.Printf("%s\n", dial_err.Error())
		return
	}
	defer conn.Close()

	message := []byte(grep_command)
	_, write_err := conn.Write(message)
	if write_err != nil {
		fmt.Println("CLIENT:78 = Error sending:", write_err)
		return
	}

	// print(fmt.Sprintf("TCP packet sent to %s%s", target_server_addr, SERVER_TCP_PORT))
	HandleTCPData(conn)
}

func ExecuteGrep(grepPattern string, machine_num int) {
	exec_query := fmt.Sprintf("grep -rH %s ../logs/machine.%d.log", grepPattern, machine_num)
	// exec_query := fmt.Sprintf("grep -rH %s ../../logs/sample.log", grepPattern)
	cmd := exec.Command("bash", "-c", exec_query)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	var data string

	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			data = ""
		} else {
			fmt.Println("Command failed:", err)
		}
	} else {
		data = stdout.String()
	}

	PrettyPrint(data, machine_num)
}

func PrettyPrint(receivedData string, machineNum int) {
	numLines := strings.Count(receivedData, "\n")

	mu.Lock()
	fmt.Printf("Machine %d sent %d lines:\n%s\n", machineNum, numLines, receivedData)
	// fmt.Printf("Machine %d sent %d lines:\n", machineNum, numLines)
	mu.Unlock()
}
