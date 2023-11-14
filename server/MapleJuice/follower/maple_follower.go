package maplejuice

import (
	"net"

	maplejuiceutils "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/MapleJuice/mapleJuiceUtils"
)

func HandleMapleRequest(Task* maplejuiceutils.MapleJuiceTask, MapleConn *net.Conn) {
	// a function to handle a single maple task request
	
	// 1. Take Maple task, Retrieve exec file from sdfs, and [dataset lines] from connection
	// 2. Run executable on each line of the [dataset lines]
	// 3. From the resultant [K, V], store each unique K, V in Task.NodeDesignation_Task.SdfsPrefix_K locally, MAKE SURE TO OPEN FILE IF DNE, OR IN APPEND MODE
	// 4. send ack to sdfs master for locally created files. 
}