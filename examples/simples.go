package main

import (
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adapter"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	//. "pcl-sdk-go-urchin/storage/operation"
)

var urchinServiceAddr = "https://127.0.0.1:39256"
var sourcePath = "/Users/zhangjiayuan/Downloads/source"
var uploadFileSourcePath = "/Users/zhangjiayuan/Downloads/upload_file"
var targetPath = "/Users/zhangjiayuan/Downloads/target/"
var cachePath = "/Users/zhangjiayuan/Downloads/cache/"
var objUuid = "20ca0fe5-d671-4111-9419-18374839dec2"
var objPath = "tmp/"
var sourceNodeName = "test-ipfs"
var targetNodeName = "chengdu-obs"
var taskId int32 = 54

func main() {
	log := new(Log)
	log.Init()

	/*上传数据对象*/
	//Upload(urchinServiceAddr, sourcePath)

	/*下载数据对象*/
	//Download(urchinServiceAddr, objUuid, targetPath)

	/*迁移数据对象，可不清理断点续传信息，可重试迁移，needPure默认置false*/
	/*
		Migrate(
			urchinServiceAddr,
			objUuid,
			&sourceNodeName,
			targetNodeName,
			cachePath,
			false)
	*/

	/*数据对象新增文件*/
	UploadFile(
		urchinServiceAddr,
		objUuid,
		objPath,
		uploadFileSourcePath,
		false)

	/*任务重试，可不清理断点续传信息，needPure默认置false*/
	//RetryTask(urchinServiceAddr, taskId, false)
}
