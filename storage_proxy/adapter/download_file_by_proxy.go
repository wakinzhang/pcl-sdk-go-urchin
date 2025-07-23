package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
)

func DownloadFileByProxy(
	userId string,
	token string,
	urchinServiceAddr,
	objUuid,
	source,
	targetPath string) (
	err error,
	downloadFileResp *DownloadFileResp) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	if '/' != targetPath[len(targetPath)-1] {
		targetPath = targetPath + "/"
	}

	InfoLogger.WithContext(ctx).Debug(
		"DownloadFileByProxy start.",
		zap.String("userId", userId),
		zap.String("token", "***"),
		zap.String("objUuid", objUuid),
		zap.String("source", source),
		zap.String("targetPath", targetPath))

	downloadFileResp = new(DownloadFileResp)

	UClient.Init(
		ctx,
		userId,
		token,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	downloadFileReq := new(DownloadFileReq)
	downloadFileReq.UserId = userId
	downloadFileReq.ObjUuid = objUuid
	downloadFileReq.Source = source
	downloadFileReq.TargetLocalPath = targetPath

	err, downloadFileResp = UClient.DownloadFile(ctx, downloadFileReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.DownloadFile failed.",
			zap.Error(err))
		return err, downloadFileResp
	}

	downloadFileRespBuf, _ := json.Marshal(downloadFileResp)
	fmt.Printf("DownloadFile Response: %s\n",
		string(downloadFileRespBuf))

	err = ProcessDownloadFileByProxy(
		ctx,
		userId,
		targetPath,
		downloadFileResp.BucketName,
		downloadFileResp.TaskId,
		downloadFileResp.NodeType)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ProcessDownloadFileByProxy failed.",
			zap.Error(err))
		return err, downloadFileResp
	}
	InfoLogger.WithContext(ctx).Debug(
		"DownloadFileByProxy finish.")
	return err, downloadFileResp
}

func ProcessDownloadFileByProxy(
	ctx context.Context,
	userId,
	targetPath,
	bucketName string,
	taskId,
	nodeType int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ProcessDownloadFileByProxy start.",
		zap.String("userId", userId),
		zap.String("targetPath", targetPath),
		zap.String("bucketName", bucketName),
		zap.Int32("taskId", taskId),
		zap.Int32("nodeType", nodeType))

	if '/' != targetPath[len(targetPath)-1] {
		targetPath = targetPath + "/"
	}

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.UserId = userId
		finishTaskReq.TaskId = taskId
		if nil != err {
			finishTaskReq.Result = TaskFResultEFailed
		} else {
			finishTaskReq.Result = TaskFResultESuccess
		}
		_err, _ := UClient.FinishTask(ctx, finishTaskReq)
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"UrchinClient.FinishTask failed.",
				zap.Error(_err))
		}
	}()

	err, storage := NewStorageProxy(ctx, nodeType)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"NewStorageProxy failed.",
			zap.Error(err))
		return err
	}
	err = storage.Download(
		ctx,
		userId,
		targetPath,
		taskId,
		bucketName)

	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"storage.Download failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ProcessDownloadFileByProxy finish.")
	return nil
}
