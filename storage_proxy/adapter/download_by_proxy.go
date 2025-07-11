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

func DownloadByProxy(
	userId,
	token,
	urchinServiceAddr,
	objUuid,
	targetPath,
	nodeName string) (
	err error,
	downloadObjectResp *DownloadObjectResp) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	if '/' != targetPath[len(targetPath)-1] {
		targetPath = targetPath + "/"
	}

	Logger.WithContext(ctx).Debug(
		"DownloadByProxy start.",
		" userId: ", userId,
		" token: ", "***",
		" objUuid: ", objUuid,
		" targetPath: ", targetPath,
		" nodeName: ", nodeName)

	downloadObjectResp = new(DownloadObjectResp)

	UClient.Init(
		ctx,
		userId,
		token,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	downloadObjectReq := new(DownloadObjectReq)
	downloadObjectReq.UserId = userId
	downloadObjectReq.ObjUuid = objUuid
	downloadObjectReq.TargetLocalPath = targetPath
	if 0 != len(nodeName) {
		downloadObjectReq.NodeName = &nodeName
	}

	err, downloadObjectResp = UClient.DownloadObject(
		ctx, downloadObjectReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.DownloadObject failed.",
			" err: ", err)
		return err, downloadObjectResp
	}

	downloadObjectRespBuf, _ := json.Marshal(downloadObjectResp)
	fmt.Printf("Download Response: %s\n",
		string(downloadObjectRespBuf))

	err = ProcessDownloadByProxy(
		ctx,
		userId,
		targetPath,
		downloadObjectResp.BucketName,
		downloadObjectResp.TaskId,
		downloadObjectResp.NodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessDownloadByProxy failed.",
			" err: ", err)
		return err, downloadObjectResp
	}
	Logger.WithContext(ctx).Debug(
		"DownloadByProxy finish.")
	return err, downloadObjectResp
}

func ProcessDownloadByProxy(
	ctx context.Context,
	userId string,
	targetPath,
	bucketName string,
	taskId,
	nodeType int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessDownloadByProxy start.",
		" userId: ", userId,
		" targetPath: ", targetPath,
		" bucketName: ", bucketName,
		" taskId: ", taskId,
		" nodeType: ", nodeType)

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
		"ProcessDownloadByProxy finish.")
	return nil
}
