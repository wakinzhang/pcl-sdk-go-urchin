package adapter

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
)

func DownloadFileByProxy(
	urchinServiceAddr,
	objUuid,
	source,
	targetPath string) (err error) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	Logger.WithContext(ctx).Debug(
		"DownloadFileByProxy start.",
		" objUuid: ", objUuid,
		" source: ", source,
		" targetPath: ", targetPath)

	UClient.Init(
		ctx,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	downloadFileReq := new(DownloadFileReq)
	downloadFileReq.UserId = DefaultUrchinClientUserId
	downloadFileReq.ObjUuid = objUuid
	downloadFileReq.Source = source
	downloadFileReq.TargetLocalPath = targetPath

	err, downloadFileResp := UClient.DownloadFile(ctx, downloadFileReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.DownloadFile failed.",
			" err: ", err)
		return err
	}

	fmt.Printf("DownloadFile TaskId: %d\n", downloadFileResp.TaskId)

	err = ProcessDownloadFileByProxy(
		ctx,
		targetPath,
		downloadFileResp.BucketName,
		downloadFileResp.TaskId,
		downloadFileResp.NodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessDownloadFileByProxy failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"DownloadFileByProxy finish.")
	return err
}

func ProcessDownloadFileByProxy(
	ctx context.Context,
	targetPath,
	bucketName string,
	taskId,
	nodeType int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessDownloadFileByProxy start.",
		" targetPath: ", targetPath,
		" bucketName: ", bucketName,
		" taskId: ", taskId,
		" nodeType: ", nodeType)

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.TaskId = taskId
		if nil != err {
			finishTaskReq.Result = TaskFResultEFailed
		} else {
			finishTaskReq.Result = TaskFResultESuccess
		}
		err, _ = UClient.FinishTask(ctx, finishTaskReq)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"UrchinClient.FinishTask failed.",
				" err: ", err)
		}
	}()

	err, storage := NewStorageProxy(ctx, nodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"NewStorageProxy failed.",
			" err: ", err)
		return err
	}
	err = storage.Download(
		ctx,
		targetPath,
		taskId,
		bucketName)

	if nil != err {
		Logger.WithContext(ctx).Error(
			"storage.Download failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"ProcessDownloadFileByProxy finish.")
	return nil
}
