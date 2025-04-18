package adapter

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
)

func UploadFile(
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
		"UploadFile start.",
		" objUuid: ", objUuid,
		" objPath: ", objPath,
		" sourcePath: ", sourcePath,
		" needPure: ", needPure)

	UClient.Init(
		ctx,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	uploadFileReq := new(UploadFileReq)
	uploadFileReq.UserId = DefaultUrchinClientUserId
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

	err = ProcessUploadFile(
		ctx,
		sourcePath,
		uploadFileResp.TaskId,
		uploadFileResp.NodeType,
		needPure)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessUploadFile failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"UploadFile finish.")
	return err
}

func ProcessUploadFile(
	ctx context.Context,
	sourcePath string,
	taskId, nodeType int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessUploadFile start.",
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

	err, storage := NewStorage(ctx, nodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"NewStorage failed.",
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
		"ProcessUploadFile finish.")
	return nil
}
