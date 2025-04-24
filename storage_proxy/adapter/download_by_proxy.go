package adapter

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
)

func DownloadByProxy(
	urchinServiceAddr,
	objUuid,
	targetPath string) (err error) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	Logger.WithContext(ctx).Debug(
		"DownloadByProxy start.",
		" targetPath: ", targetPath,
		" objUuid: ", objUuid)

	UClient.Init(
		ctx,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	downloadObjectReq := new(DownloadObjectReq)
	downloadObjectReq.UserId = DefaultUrchinClientUserId
	downloadObjectReq.ObjUuid = objUuid
	downloadObjectReq.TargetLocalPath = targetPath

	err, downloadObjectResp := UClient.DownloadObject(
		ctx, downloadObjectReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.DownloadObject  failed.",
			" err: ", err)
		return err
	}

	fmt.Printf("Download TaskId: %d\n", downloadObjectResp.TaskId)

	err = ProcessDownloadByProxy(
		ctx,
		targetPath,
		downloadObjectResp.BucketName,
		downloadObjectResp.TaskId,
		downloadObjectResp.NodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessDownloadByProxy failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"DownloadByProxy finish.")
	return err
}

func ProcessDownloadByProxy(
	ctx context.Context,
	targetPath,
	bucketName string,
	taskId,
	nodeType int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessDownloadByProxy start.",
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
		"ProcessDownloadByProxy finish.")
	return nil
}
