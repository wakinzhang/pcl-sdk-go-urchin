package main

import (
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage_proxy/adapter"
	//. "github.com/wakinzhang/pcl-sdk-go-urchin/operation"
)

var (
	logPath           = "./logs"
	logFile           = "urchin-sdk-access.log"
	urchinServiceAddr = "https://127.0.0.1:39256"
)

func main() {
	InitLog(logPath, logFile, 6)

	/*上传数据对象*/
	/**/
	var objectName = "wakinzhang-test-obj-20250519-3"
	//var sourcePath = "/Users/zhangjiayuan/Downloads/source/"
	var sourcePath = "/Users/zhangjiayuan/Downloads/test.zip"
	//var sourcePath = "/Users/zhangjiayuan/Downloads/source/diversicus.mp4"

	_ = UploadByProxy(
		DefaultUrchinClientUserId,
		DefaultUrchinClientToken,
		urchinServiceAddr,
		sourcePath,
		objectName)

	/*下载数据对象*/
	/*
		var objUuid = "3c4661a1-c3d5-41fd-990b-f70badbf0fa2"
		var targetPath = "/Users/zhangjiayuan/Downloads/target/"

		_ = DownloadByProxy(
			DefaultUrchinClientUserId,
			DefaultUrchinClientToken,
			urchinServiceAddr,
			objUuid,
			targetPath)
	*/

	/*迁移数据对象，可不清理断点续传信息，可重试迁移，needPure默认置false*/
	/*
		var objUuid = ""
		var sourceNodeName = "test-ipfs"
		var targetNodeName = "chengdu-obs"
		var cachePath = "/Users/zhangjiayuan/Downloads/cache/"

		_ = MigrateByProxy(
			DefaultUrchinClientUserId,
			DefaultUrchinClientToken,
			urchinServiceAddr,
			objUuid,
			&sourceNodeName,
			targetNodeName,
			cachePath,
			false)
	*/

	/*数据对象新增文件、文件夹*/
	/*
		var objUuid = "60d605ea-b474-492f-a269-e98a6fdec188"
		var objPath = "empty/"
		var uploadFileSourcePath = "/Users/zhangjiayuan/Downloads/upload_file"

		_ = UploadFileByProxy(
			DefaultUrchinClientUserId,
			DefaultUrchinClientToken,
			urchinServiceAddr,
			objUuid,
			objPath,
			uploadFileSourcePath,
			false)
	*/

	/*下载数据对象指定文件、文件夹*/
	/*
		var objUuid = "60d605ea-b474-492f-a269-e98a6fdec188"
		var source = "small/"
		var targetPath = "/Users/zhangjiayuan/Downloads/download_file/"

		_ = DownloadFileByProxy(
			DefaultUrchinClientUserId,
			DefaultUrchinClientToken,
			urchinServiceAddr,
			objUuid,
			source,
			targetPath)
	*/

	/*任务重试，可不清理断点续传信息，needPure默认置false*/
	/*
		var taskId int32 = 54

		_ = RetryTask(urchinServiceAddr, taskId, false)
	*/

}
