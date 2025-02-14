package main

import (
	. "pcl-sdk-go-urchin/storage/adapter"
	. "pcl-sdk-go-urchin/storage/common"
)

var urchinServiceAddr = "https://127.0.0.1:39256"
var sourcePath = "/Users/zhangjiayuan/Downloads/source/"
var targetPath = "/Users/zhangjiayuan/Downloads/target/"
var cachePath = "/Users/zhangjiayuan/Downloads/cache/"
var objUuid = "6348513e-fbed-447d-8d4f-13b25173200e"
var sourceNodeId int32 = 3
var targetNodeId int32 = 1

func main() {
	log := new(Log)
	log.Init()

	//Upload(urchinServiceAddr, sourcePath)
	//Download(urchinServiceAddr, objUuid, targetPath)
	Migrate(urchinServiceAddr, objUuid, &sourceNodeId, targetNodeId, cachePath)
}
