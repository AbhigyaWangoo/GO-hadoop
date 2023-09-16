package testing

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	client "gitlab.engr.illinois.edu/asehgal4/cs425mps/client/distributedGrepClient"
)

type MachineData struct {
	ip            string
	MachineNumber int
}

func TestSendingData(t *testing.T) {

	print(os.Getwd())

	MachineFile := "../../../logs/machine.txt"

	content, read_err := os.ReadFile(MachineFile)
	if read_err != nil {
		fmt.Println("SERVER:99 = Error:", read_err)
		return
	}

	// Convert the file content to an integer
	MachineNumber := string(content[0])

	var nodes = map[string]MachineData{
		// "asehgal4@fa23-cs425-4901.cs.illinois.edu": {"172.22.156.162", 1},
		// "asehgal4@fa23-cs425-4902.cs.illinois.edu": {"172.22.158.162", 2},
		// "asehgal4@fa23-cs425-4903.cs.illinois.edu": {"172.22.94.162", 3},
		// "asehgal4@fa23-cs425-4904.cs.illinois.edu": {"172.22.156.163", 4},
		// "asehgal4@fa23-cs425-4905.cs.illinois.edu": {"172.22.158.163", 5},
		"asehgal4@fa23-cs425-4906.cs.illinois.edu": {"172.22.94.163", 6},
		"asehgal4@fa23-cs425-4907.cs.illinois.edu": {"172.22.156.164", 7},
		// "asehgal4@fa23-cs425-4908.cs.illinois.edu": {"172.22.158.164", 8},
		// "asehgal4@fa23-cs425-4909.cs.illinois.edu": {"172.22.94.164", 9},
		// "asehgal4@fa23-cs425-4910.cs.illinois.edu": {"172.22.156.165", 10},
	}

	// Store current node
	CurrNode := "172.22.94.163"

	for node, MachineInfo := range nodes {
		if MachineInfo.ip == CurrNode {
			continue
		}
		StartRemoteServer(node)
	}

	time.Sleep(time.Millisecond * 100)

	// Generate random log file
	GenerateLogFiles(MachineNumber)

	// Set output of distributed grep command to be a local file
	OrigStdout := os.Stdout

	DistributedLogFile, err := os.Create("results.log")
	if err != nil {
		for node, MachineInfo := range nodes {
			if MachineInfo.ip == CurrNode {
				continue
			}
			StopRemoteServer(node)
		}
		t.Fatalf("Unable to create log file: %v", err)
	}
	defer DistributedLogFile.Close()

	os.Stdout = DistributedLogFile

	// Send random sample log file to all servers
	LocalMachineSampleLog := fmt.Sprintf("../../../logs/machine.%s.log", MachineNumber)
	for node, NodeInformation := range nodes {
		if NodeInformation.ip == CurrNode {
			continue
		}
		LogPath := fmt.Sprintf("/home/asehgal4/MPs/mp1/logs/machine.%d.log", NodeInformation.MachineNumber)

		cmd := exec.Command("scp", LocalMachineSampleLog, node+":"+LogPath)

		output, err := cmd.CombinedOutput()
		if err != nil {
			for node, MachineInfo := range nodes {
				if MachineInfo.ip == CurrNode {
					continue
				}
				StopRemoteServer(node)
			}
			t.Errorf("scp command failed with error: %v, output: %s", err, output)
		}
	}

	// Set arguments and run main function
	os.Args = []string{"go run client.go", "TRACE"}
	client.InitializeClient()

	// // Count number of lines returned by grep command
	// DistributedLogFile.Seek(0, 0)
	// DistributedLineCount := GetFileLineNumber(DistributedLogFile)

	// Execute the same grep command locally, storing the output in a buffer
	LocalGrepCommand := exec.Command("grep", "-rH", "TRACE", LocalMachineSampleLog)
	var LocalGrepOutput bytes.Buffer
	LocalGrepCommand.Stdout = &LocalGrepOutput
	err = LocalGrepCommand.Run()

	// Check if execution failed
	if err != nil {
		for node, MachineInfo := range nodes {
			if MachineInfo.ip == CurrNode {
				continue
			}
			StopRemoteServer(node)
		}
		log.Fatalf("Command failed with error: %v", err)
	}

	// // Count number of lines returned by local log
	// LocalGrepLineCount := GetFileLineNumber(&LocalGrepOutput)
	os.Stdout = OrigStdout

	DistributedLogFile.Seek(0, 0)
	pattern := `^(\.\./\.\./\.\./logs/machine\.)(\d+)(\.log):` // Adjusted the pattern

	DistributedResultOutput, err := FetchLinesStartingWithPattern(DistributedLogFile, pattern)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	LocalResultOutput, err := FetchLinesStartingWithPattern(&LocalGrepOutput, pattern)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	for _, CorrectMachineOutput := range LocalResultOutput {
		if !CompareOutputs(CorrectMachineOutput, DistributedResultOutput) {
			t.Fatalf("Incorrect output")
		}
	}

	// Since same log file in all nodes, line count from the distributed command
	// should be equivalent to 10 times the local line count excluding the extraneous lines printed
	// detailing how many lines came from each server

	for node, MachineInfo := range nodes {
		if MachineInfo.ip == CurrNode {
			continue
		}
		StopRemoteServer(node)
	}
}

func CompareOutputs(LocalOutput []string, DistributedOutput map[string][]string) bool {
	for _, output := range DistributedOutput {
		if len(LocalOutput) != len(output) {
			return false
		}
		for i := 0; i < len(LocalOutput); i++ {
			if LocalOutput[i] != output[i] {
				return false
			}
		}
	}
	return true
}

// Helper function to get lines of a file or buffer
// func GetFileLines(LogFile io.Reader) []string {
// 	FileOutput := make([]string, 0)
// 	FileScanner := bufio.NewScanner(LogFile)
// 	for FileScanner.Scan() {
// 		line := FileScanner.Text()
// 		FileOutput = append(FileOutput, line)
// 	}
// 	return FileOutput
// }

func FetchLinesStartingWithPattern(FileStream io.Reader, pattern string) (map[string][]string, error) {
	DistributedOutput := make(map[string][]string)
	re := regexp.MustCompile(pattern)
	scanner := bufio.NewScanner(FileStream)
	for scanner.Scan() {
		line := scanner.Text()
		MatchedLine := re.FindStringSubmatch(line)
		if MatchedLine != nil {
			ProcessedLine := re.ReplaceAllLiteralString(line, "")
			MachineNumber := MatchedLine[2]
			DistributedOutput[MachineNumber] = append(DistributedOutput[MachineNumber], ProcessedLine)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return DistributedOutput, nil
}

// Helper function to generate a random log file
func GenerateLogFiles(MachineNumber string) {
	const entries = 800

	rand.Seed(time.Now().UnixNano())
	FileToCreate := fmt.Sprintf("../../../logs/machine.%s.log", MachineNumber)
	file, err := os.Create(FileToCreate)
	if err != nil {
		fmt.Println("Unable to create log file:", err)
	}

	defer file.Close()
	LogTypes := []string{"INFO", "WARNING", "TRACE", "ERROR"}
	MessageTypes := []string{"Starting Application", "Application is too crazy", "Finding process in application", "Application deleted all files"}
	for i := 0; i < entries; i++ {
		TimeStamp := time.Now().UTC()
		LogType := LogTypes[rand.Intn(4)]
		MessageType := MessageTypes[rand.Intn(4)]

		message := fmt.Sprintf("%s %s %s\n", TimeStamp, LogType, MessageType)

		_, err := file.Write([]byte(message))
		if err != nil {
			fmt.Println("Unable to write to sample log:", err)
		}
	}
}

func StartRemoteServer(address string) {
	// cmd := exec.Command("scp", "../../logs/sample.log", "asehgal4@"+node+":/home/asehgal4/MPs/mp1/logs/")

	// Prepare the command
	cmd := exec.Command("ssh", address, "cd /home/asehgal4/MPs/mp1/src/server && ./server")

	// Start the command
	err := cmd.Start()
	if err != nil {
		log.Fatalf("Failed to start the command: %v", err)
	}
}

func StopRemoteServer(address string) {
	cmd := exec.Command("ssh", address, "pkill server")

	// Start the command
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to run the command: %v", err)
	}
}
