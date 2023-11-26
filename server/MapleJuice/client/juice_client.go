package maplejuice

import (
	"fmt"
	"math/big"
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
	nJuices := int64(len(JuiceDsts))
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
			modulus := big.NewInt(nJuices)

			// Perform modular arithmetic
			idx := new(big.Int).Mod(hash, modulus)
			ipaddr = JuiceDsts[idx.Int64()]

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
	
	// fmt.Println(rv)
	return rv
}

func SendJuiceTask(ipDest string, sdfsKeyFiles []string, nodeIdx uint32, sdfsPrefix string, localExecFile string, nJuices uint32) error {
	conn, err := sdfsutils.OpenTCPConnection(ipDest, maplejuiceUtils.MAPLE_JUICE_PORT)
	if err != nil {
		return err
	}

	// Add localexec file to sdfs. TODO this can be send directly to avoid time wasted.
	sdfs_client.CLIPut(localExecFile, localExecFile)

	Task := maplejuiceUtils.MapleJuiceTask{
		Type: maplejuiceUtils.JUICE,
		NodeDesignation: nodeIdx,
		SdfsPrefix: sdfsPrefix,
		SdfsExecFile: localExecFile,
		NumberOfMJTasks: nJuices,
	}

	arr := Task.Marshal()
	_, err = conn.Write(arr)
	if err != nil {
		return err
	}

	return nil
}
