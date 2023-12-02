package sqlcommands

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	maplejuiceclient "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/client"
	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
)

func ProcessSQLCommand(command string) {
	commandNumber, parsedData, err := sqlCommandParsing(command)
	if err != nil {
		log.Println("Error processing SQL command: ", err)
	}

	if commandNumber == maplejuiceutils.COMMAND_1 {
		maplejuiceclient.InitiateMaplePhase("sql_command_1_map_exec", 6, "command_1_map_out", parsedData["dataset"], []string{parsedData["condition"]})
		maplejuiceclient.InitiateJuicePhase("sql_command_1_reduce_exec", 6, "command_1_map_out", "command_1_reduce_out", false, maplejuiceutils.HASH)
		// maplejuiceclient.InitiateJuicePhase()
	} else if commandNumber == maplejuiceutils.COMMAND_2 {
		maplejuiceclient.InitiateMaplePhase("sql_command_2_exec", 6, "command_2_out", parsedData["D1"], []string{parsedData["condition"]})
	}
}

func sqlCommandParsing(command string) (maplejuiceutils.SQLCommandType, map[string]string, error) {
	// Define regular expressions for the two commands
	regex1 := regexp.MustCompile(`SELECT ALL FROM (\w+) WHERE (.+)`)
	regex2 := regexp.MustCompile(`SELECT ALL FROM (\w+), (\w+) WHERE (.+?)\s*=\s*(.+)`)

	// Check for a match with the first command
	if matches := regex1.FindStringSubmatch(command); len(matches) == 3 {
		return maplejuiceutils.COMMAND_1, map[string]string{
			"Dataset":   matches[1],
			"Condition": matches[2],
		}, nil
	}

	// Check for a match with the second command
	if matches := regex2.FindStringSubmatch(command); len(matches) == 5 {
		return maplejuiceutils.COMMAND_2, map[string]string{
			"D1":             matches[1],
			"D2":             matches[2],
			"LeftCondition":  strings.TrimSpace(matches[3]),
			"RightCondition": strings.TrimSpace(matches[4]),
		}, nil
	}

	// No match found
	return maplejuiceutils.INVALID_COMMAND, nil, fmt.Errorf("No match found for the input command")
}
