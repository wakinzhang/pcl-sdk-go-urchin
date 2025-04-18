package adapter

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
)

func DownloadFile(
	urchinServiceAddr,
	objUuid,
	source,
	targetPath string) (err error) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	Logger.WithContext(ctx).Debug(
		"DownloadFile start.",
		" objUuid: ", objUuid,
		" source: ", source,
		" targetPath: ", targetPath)

	UClient.Init(
		ctx,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	downloadFileReq := new(DownloadFileReq)
	downloadFileReq.UserId = "vg3yHwUa"
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

	err = ProcessDownloadFile(
		ctx,
		targetPath,
		downloadFileResp.BucketName,
		downloadFileResp.TaskId,
		downloadFileResp.NodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessDownloadFile failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"DownloadFile finish.")
	return err
}

func ProcessDownloadFile(
	ctx context.Context,
	targetPath, bucketName string,
	taskId, nodeType int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessDownloadFile start.",
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

	err, storage := NewStorage(ctx, nodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"NewStorage failed.",
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
		"ProcessDownloadFile finish.")
	return nil
}
