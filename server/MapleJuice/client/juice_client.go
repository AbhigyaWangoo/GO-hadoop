package maplejuice

import (
	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
)

func InitiateJuicePhase(LocalExecFile string, NJuices uint32, SdfsPrefix string, SdfsDst string, DeleteInput bool, Partition maplejuiceutils.PartitioningType) {
	// Initiates the Juice phase via client command

	// 1. GET all sdfs files' names associated with SdfsPrefix (1 file per unique key), call it SdfsPrefixKeys
	// 2. get an array of NJuices IPs from gossip memlist, call it JuiceDsts
	// 3. Call PartitionKeys(SdfsPrefixKeys, JuiceDsts) that returns a map of IPAddr:[sdfsKeyFile]
	// 4. For each IpAddr in above map:
	// 		5. SendJuiceTask(IpAddr, [sdfsKeyFiles])
}
