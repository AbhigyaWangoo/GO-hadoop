package maplejuiceutils

type MapleJuiceType int
type PartitioningType int

const HASH PartitioningType = 0
const RANGE PartitioningType = 1
const MAPLE MapleJuiceType = 0
const JUICE MapleJuiceType = 1


type MapleJuiceTask struct {
	Type MapleJuiceType
	NodeDesignation uint32 // The 'index' of the node recieving the maplejuice task
	SdfsPrefix string
	SdfsExecFile string // The name of the executable that exists in sdfs
	// We also need to somehow track 
}