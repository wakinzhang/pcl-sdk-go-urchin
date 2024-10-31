package main

import (
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adapter"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
)

var urchinServiceAddr = "https://127.0.0.1:39256"
var sourcePath = "/Users/zhangjiayuan/Downloads/test/"
var objUuid = "bc63d925-98ff-4f0c-8d72-495534e981bd"

func main() {
	log := new(Log)
	log.Init()

	//Upload(urchinServiceAddr, sourcePath)
	Download(urchinServiceAddr, objUuid)
}
