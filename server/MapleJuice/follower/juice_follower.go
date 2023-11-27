package maplejuice

import (
	"fmt"
	"net"

	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
	sdfs "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/sdfs"
)

func HandleJuiceRequest(Task *maplejuiceutils.MapleJuiceTask, conn *net.Conn) {
	fmt.Println("Entering handle juice request for ", Task.SdfsPrefix)
	SdfsFilename := Task.SdfsPrefix // SdfsFilename is the one to pull from SDFS, and run the juice task on.
	
	// CLI GET file locally
	sdfs.CLIGet(SdfsFilename, SdfsFilename)

	// Run exec file on input file
	

}
