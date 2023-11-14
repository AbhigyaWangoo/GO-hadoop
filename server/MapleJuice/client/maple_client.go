package maplejuice

func InitiateMaplePhase(LocalExecFile string, NMaples uint32, SdfsPrefix string, SdfsSrcDataset string) {

	// Initiates the Maple phase via client command

	// 1. GET SdfsSrcDataset -> dataset.txt
	// 2. get an array of NMaples IPs from gossip memlist, call it MapleDsts
	// 3. For each line in dataset.txt,
	// 		4. i = hash(line) % NMaples. Also, need to save in append mode this line into a seperate file for later sending
	// 		5. SendMapleTask(MapleDsts[i], CreatedTask) // Need to think about this carefully
}