package main

import (
	distributedGrepServer "gitlab.engr.illinois.edu/asehgal4/cs425mps/server/distributedGrepServer"
)

func main() {
	go distributedGrepServer.InitializeServer()
}
