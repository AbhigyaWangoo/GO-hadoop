package sdfsutils

type Command int

const (
	READ         Command = 0
	WRITE         Command = 1
	DELETE      Command = 2
)

const KB int = 1024
const SDFS_PORT string = "3541"
const SDFS_BUFFER_SIZE int = 4 * KB
const FILESYSTEM_ROOT string = "sdfs/sdfsFileSystemRoot/"

var MASTER_IP string = "172.22.158.162"
