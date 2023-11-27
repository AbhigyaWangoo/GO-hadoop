package maplejuice

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	// "strconv"

	maplejuiceUtils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfs_client "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
	sdfsutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs/sdfsUtils"
)

func InitiateJuicePhase(LocalExecFile string, NJuices uint32, SdfsPrefix string, SdfsDst string, DeleteInput bool, Partition maplejuiceUtils.PartitioningType) {
	// Initiates the Juice phase via client command

	// 1. GET all sdfs files' names associated with SdfsPrefix (1 file per unique key), call it SdfsPrefixKeys
	SdfsPrefixKeys := sdfs_client.InitiateLsWithPrefix(SdfsPrefix)
	fmt.Println(SdfsPrefixKeys)

	// 2. get an array of NJuices IPs from gossip memlist, call it JuiceDsts
	JuiceDsts := gossiputils.RandomKIpAddrs(int(NJuices), true)
	fmt.Println(JuiceDsts)

	// 3. Call PartitionKeys(SdfsPrefixKeys, JuiceDsts) that returns a map of IPAddr:[sdfsKeyFile]
	PartitionedKeys := PartitionKeys(SdfsPrefixKeys, JuiceDsts, Partition)
	fmt.Println(PartitionedKeys)

	// Add localexec file to sdfs. TODO this can be send directly to avoid time wasted.
	sdfs_client.CLIPut(LocalExecFile, LocalExecFile)
	fmt.Println("Put exec file into sdfs")

	// 4. For each IpAddr in above map:
	var i uint32 = 0
	for IpAddr, sdfsKeyFiles := range PartitionedKeys {

		// 5. SendJuiceTask(IpAddr, [sdfsKeyFiles])
		err := SendJuiceTask(IpAddr, sdfsKeyFiles, i, SdfsPrefix, LocalExecFile, NJuices)
		if err != nil {
			fmt.Printf("Error with sending juice task to ip addr %s, %v\n", IpAddr, err)
		}

		i++
	}
}

func PartitionKeys(SdfsPrefixKeys []string, JuiceDsts []string, Partition maplejuiceUtils.PartitioningType) map[string][]string {
	rv := make(map[string][]string)
	// nKeys := len(SdfsPrefixKeys)

	if Partition == maplejuiceUtils.RANGE {
		sort.Strings(SdfsPrefixKeys)
		// nKeysPerDst := len(SdfsPrefixKeys) / len(JuiceDsts)

		// min, err := strconv.Atoi(SdfsPrefixKeys[0])
		// if err != nil {
		// 	panic(err)
		// }

		// max, err = strconv.Atoi(SdfsPrefixKeys[len(SdfsPrefixKeys)-1])
		// if err != nil {
		// 	panic(err)
		// }
	}

	for _, key := range SdfsPrefixKeys {
		var ipaddr string

		if Partition == maplejuiceUtils.HASH {
			hash := maplejuiceUtils.CalculateSHA256(key)
			hashint, err := hex.DecodeString(hash)
			if err != nil {
				fmt.Printf("Error in key partitioning, cant get hash of %s: %v\n", hash, err)
			}
			intHash := int(hashint[0])
			
			// Perform modular arithmetic
			idx := intHash % len(JuiceDsts)
			ipaddr = JuiceDsts[idx]
		} else if Partition == maplejuiceUtils.RANGE {

		}

		val, exists := rv[ipaddr]
		fmt.Println(ipaddr)
		var arr []string
		if !exists {
			rv[ipaddr] = append(arr, key)
		} else {
			rv[ipaddr] = append(val, key)
		}
	}

	return rv
}

func SendJuiceTask(ipDest string, sdfsKeyFiles []string, nodeIdx uint32, localExecFile string, sdfsPrefix string, nJuices uint32) error {
	if ipDest == "" {
		return errors.New("Ip destination was empty for sending juice task")
	}

	conn, err := sdfsutils.OpenTCPConnection(ipDest, maplejuiceUtils.MAPLE_JUICE_PORT)
	if err != nil {
		return err
	}
	defer conn.Close()

	Task := maplejuiceUtils.MapleJuiceTask{
		Type:            maplejuiceUtils.JUICE,
		NodeDesignation: nodeIdx,
		SdfsPrefix:      sdfsPrefix,
		SdfsExecFile:    localExecFile,
		NumberOfMJTasks: nJuices,
	}

	arr := Task.Marshal()
	_, err = conn.Write(arr)
	fmt.Println("Sent juice task to end node ", ipDest)
	if err != nil {
		return err
	}

	n, err := conn.Write([]byte("\n"))
	if err != nil || n != 1 {
		return err
	}

	return nil
}
