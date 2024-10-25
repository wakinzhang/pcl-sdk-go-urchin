package main

import (
	. "pcl-sdk-go-urchin/storage/adapter"
	. "pcl-sdk-go-urchin/storage/common"
)

var urchinServiceAddr = "urchin_service_addr"
var sourcePath = "source_path"

func main() {
	log := new(Log)
	log.Init()

	Upload(urchinServiceAddr, sourcePath)
}
