package sdfs

import (
	cmap "github.com/orcaman/concurrent-map/v2"
)

var BlockLocations cmap.ConcurrentMap[string, [][]string] // filename : [[ip addr, ip addr, ], ], index 2d arr by block index


// Master functions