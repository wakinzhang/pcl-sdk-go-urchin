package adapter

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "pcl-sdk-go-urchin/client"
	. "pcl-sdk-go-urchin/common"
	. "pcl-sdk-go-urchin/module"
)

func UploadFileByProxy(
	userId string,
	token string,
	urchinServiceAddr,
	objUuid,
	objPath,
	sourcePath string,
	needPure bool) (err error) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	Logger.WithContext(ctx).Debug(
		"UploadFileByProxy start.",
		" userId: ", userId,
		" token: ", "***",
		" objUuid: ", objUuid,
		" objPath: ", objPath,
		" sourcePath: ", sourcePath,
		" needPure: ", needPure)

	UClient.Init(
		ctx,
		userId,
		token,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	uploadFileReq := new(UploadFileReq)
	uploadFileReq.UserId = userId
	uploadFileReq.ObjUuid = objUuid
	uploadFileReq.Source = objPath
	uploadFileReq.SourceLocalPath = sourcePath

	err, uploadFileResp := UClient.UploadFile(ctx, uploadFileReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.UploadFile failed.",
			" err: ", err)
		return err
	}

	fmt.Printf("UploadFile TaskId: %d\n", uploadFileResp.TaskId)

	err = ProcessUploadFileByProxy(
		ctx,
		sourcePath,
		uploadFileResp.TaskId,
		uploadFileResp.NodeType,
		needPure)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessUploadFileByProxy failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"UploadFileByProxy finish.")
	return err
}

func ProcessUploadFileByProxy(
	ctx context.Context,
	sourcePath string,
	taskId,
	nodeType int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessUploadFileByProxy start.",
		" sourcePath: ", sourcePath,
		" taskId: ", taskId,
		" nodeType: ", nodeType,
		" needPure: ", needPure)

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
	err = storage.Upload(
		ctx,
		sourcePath,
		taskId,
		needPure)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"storage.Upload failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"ProcessUploadFileByProxy finish.")
	return nil
}
