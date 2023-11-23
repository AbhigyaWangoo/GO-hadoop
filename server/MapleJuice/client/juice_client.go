package maplejuice

import (
	"fmt"

	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	gossiputils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/gossip/gossipUtils"
	sdfs_client "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
)

func InitiateJuicePhase(LocalExecFile string, NJuices uint32, SdfsPrefix string, SdfsDst string, DeleteInput bool, Partition maplejuiceutils.PartitioningType) {
	// Initiates the Juice phase via client command

	// 1. GET all sdfs files' names associated with SdfsPrefix (1 file per unique key), call it SdfsPrefixKeys
	SdfsPrefixKeys := sdfs_client.InitiateLsWithPrefix(SdfsPrefix)

	// 2. get an array of NJuices IPs from gossip memlist, call it JuiceDsts
	JuiceDsts := gossiputils.RandomKIpAddrs(int(NJuices))

	// 3. Call PartitionKeys(SdfsPrefixKeys, JuiceDsts) that returns a map of IPAddr:[sdfsKeyFile]
	PartitionedKeys := PartitionKeys(SdfsPrefixKeys, JuiceDsts)

	// 4. For each IpAddr in above map:
	for IpAddr, sdfsKeyFiles := range PartitionedKeys {

		// 5. SendJuiceTask(IpAddr, [sdfsKeyFiles])
		err := SendJuiceTask(IpAddr, sdfsKeyFiles)
		if err != nil {
			fmt.Printf("Error with sending juice task to ip addr %s, %v\n", IpAddr, err)
		}
	}
}

func PartitionKeys(SdfsPrefixKeys []string, JuiceDsts []string) map[string][]string {
	rv := make(map[string][]string)

	return rv
}

func SendJuiceTask(ipDest string, sdfsKeyFiles []string) error {
	return nil
}
