package main

import (
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adapter"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
)

var urchinServiceAddr = "https://127.0.0.1:39256"
var sourcePath = "/Users/zhangjiayuan/Downloads/test.zip"

func main() {
	log := new(Log)
	log.Init()

	Upload(urchinServiceAddr, sourcePath)
}
