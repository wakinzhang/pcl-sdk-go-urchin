package main

import (
	. "pcl-sdk-go-urchin/storage/adaptee"
	. "pcl-sdk-go-urchin/storage/common"
)

var endpoint = ""
var accessKey = ""
var secretKey = ""

func main() {
	log := new(Log)
	log.Init()

	obsAdapteeWithSignedUrl := new(ObsAdapteeWithSignedUrl)
	obsAdapteeWithSignedUrl.Init()
	obsAdapteeWithSignedUrl.Upload("")
}
