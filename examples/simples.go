package main

import (
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage_proxy/adapter"
	//. "github.com/wakinzhang/pcl-sdk-go-urchin/operation"
)

var urchinServiceAddr = "https://127.0.0.1:39256"

func main() {
	InitLog(6)

	/*上传数据对象*/
	/*
		var objectName = "wakinzhang-test-obj"
		var sourcePath = "/Users/zhangjiayuan/Downloads/source"

		_ = UploadByProxy(urchinServiceAddr, sourcePath, objectName)
	*/

	/*下载数据对象*/
	/**/
	var objUuid = ""
	var targetPath = "/Users/zhangjiayuan/Downloads/target/"

	_ = DownloadByProxy(urchinServiceAddr, objUuid, targetPath)

	/*迁移数据对象，可不清理断点续传信息，可重试迁移，needPure默认置false*/
	/*
		var objUuid = ""
		var sourceNodeName = "test-ipfs"
		var targetNodeName = "chengdu-obs"
		var cachePath = "/Users/zhangjiayuan/Downloads/cache/"

		_ = MigrateByProxy(
			urchinServiceAddr,
			objUuid,
			&sourceNodeName,
			targetNodeName,
			cachePath,
			false)
	*/

	/*数据对象新增文件*/
	/*
		var objUuid = ""
		var objPath = "tmp/"
		var uploadFileSourcePath = "/Users/zhangjiayuan/Downloads/upload_file"

		_ = UploadFileByProxy(
			urchinServiceAddr,
			objUuid,
			objPath,
			uploadFileSourcePath,
			false)
	*/

	/*任务重试，可不清理断点续传信息，needPure默认置false*/
	/*
		var taskId int32 = 54

		_ = RetryTask(urchinServiceAddr, taskId, false)
	*/

}
