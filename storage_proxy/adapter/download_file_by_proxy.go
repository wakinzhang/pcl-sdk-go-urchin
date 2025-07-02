package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
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

	Logger.WithContext(ctx).Debug(
		"DownloadFileByProxy start.",
		" userId: ", userId,
		" token: ", "***",
		" objUuid: ", objUuid,
		" source: ", source,
		" targetPath: ", targetPath)

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
		Logger.WithContext(ctx).Error(
			"UrchinClient.DownloadFile failed.",
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"ProcessDownloadFileByProxy failed.",
			" err: ", err)
		return err, downloadFileResp
	}
	Logger.WithContext(ctx).Debug(
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

	Logger.WithContext(ctx).Debug(
		"ProcessDownloadFileByProxy start.",
		" userId: ", userId,
		" targetPath: ", targetPath,
		" bucketName: ", bucketName,
		" taskId: ", taskId,
		" nodeType: ", nodeType)

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
			Logger.WithContext(ctx).Error(
				"UrchinClient.FinishTask failed.",
				" err: ", _err)
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
		userId,
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
