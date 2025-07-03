package main

import (
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	//. "github.com/wakinzhang/pcl-sdk-go-urchin/operation"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage_proxy/adapter"
)

var (
	accessLog         = "./logs/urchin-sdk-access.log"
	errorLog          = "./logs/urchin-sdk-error.log"
	urchinServiceAddr = "http://127.0.0.1:39256"
)

func main() {
	InitLog(accessLog, errorLog, 6)

	/*上传数据对象*/
	/**/
	var objectName = "wakinzhang-test-obj-20250703-1"
	var sourcePath = "/Users/zhangjiayuan/Downloads/source"
	//var sourcePath = "/Users/zhangjiayuan/Downloads/test.zip"
	//var sourcePath = "/Users/zhangjiayuan/Downloads/empty"
	//var sourcePath = "/Users/zhangjiayuan/Downloads/source/diversicus.mp4"
	var nodeName = ""

	_, _ = UploadByProxy(
		DefaultUrchinClientUserId,
		DefaultUrchinClientToken,
		urchinServiceAddr,
		sourcePath,
		objectName,
		nodeName)

	/*下载数据对象*/
	/*
		var objUuid = "e12074ba-e907-41b0-a6b7-9105159b9ff5"
		var targetPath = "/Users/zhangjiayuan/Downloads/target"
		var nodeName = ""

		_, _ = DownloadByProxy(
			DefaultUrchinClientUserId,
			DefaultUrchinClientToken,
			urchinServiceAddr,
			objUuid,
			targetPath,
			nodeName)
	*/

	/*导入数据对象，可不清理断点续传信息，可重试导入，needPure默认置false*/
	/*
		var objUuid = "e12074ba-e907-41b0-a6b7-9105159b9ff5"
		var sourceNodeName = "test-chengdu-obs"
		var targetNodeName = "test-jcs"
		var cachePath = "/Users/zhangjiayuan/Downloads/cache"

		_, _ = LoadByProxy(
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
		var objUuid = "bb618a96-a1d2-4759-9df0-9c4268321314"
		var objPath = "/"
		var uploadFileSourcePath = "/Users/zhangjiayuan/Downloads/upload_file"

		_, _ = UploadFileByProxy(
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
		var source = "/small/"
		var targetPath = "/Users/zhangjiayuan/Downloads/download_file/"

		_, _ = DownloadFileByProxy(
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

		_ = RetryTask(
			DefaultUrchinClientUserId,
			DefaultUrchinClientToken,
			urchinServiceAddr,
			taskId,
			false)
	*/
}
