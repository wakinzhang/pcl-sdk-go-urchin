package main

import (
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adapter"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
)

var urchinServiceAddr = "https://127.0.0.1:39256"
var sourcePath = "/Users/zhangjiayuan/Downloads/source/"
var targetPath = "/Users/zhangjiayuan/Downloads/target/"
var cachePath = "/Users/zhangjiayuan/Downloads/cache/"
var objUuid = "01805ff7-8e1f-46ec-8d6f-f4f03bfd13e7"
var sourceNodeId int32 = 1
var targetNodeId int32 = 2

func main() {
	log := new(Log)
	log.Init()

	//Upload(urchinServiceAddr, sourcePath)
	//Download(urchinServiceAddr, objUuid, targetPath)
	Migrate(urchinServiceAddr, objUuid, &sourceNodeId, targetNodeId, cachePath)
}
