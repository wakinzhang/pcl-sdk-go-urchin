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
	"os"
)

func UploadFileByProxy(
	userId string,
	token string,
	urchinServiceAddr,
	objUuid,
	objPath,
	sourcePath string,
	needPure bool) (
	err error,
	uploadFileResp *UploadFileResp) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	if '/' != objPath[len(objPath)-1] {
		objPath = objPath + "/"
	}

	if '/' != objPath[0] {
		objPath = "/" + objPath
	}

	InfoLogger.WithContext(ctx).Debug(
		"UploadFileByProxy start.",
		zap.String("userId", userId),
		zap.String("token", "***"),
		zap.String("objUuid", objUuid),
		zap.String("objPath", objPath),
		zap.String("sourcePath", sourcePath),
		zap.Bool("needPure", needPure))

	uploadFileResp = new(UploadFileResp)

	_, err = os.Stat(sourcePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err, uploadFileResp
	}

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

	err, uploadFileResp = UClient.UploadFile(ctx, uploadFileReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.UploadFile failed.",
			zap.Error(err))
		return err, uploadFileResp
	}

	uploadFileRespBuf, _ := json.Marshal(uploadFileResp)
	fmt.Printf("UploadFile Response: %s\n",
		string(uploadFileRespBuf))

	err = ProcessUploadFileByProxy(
		ctx,
		userId,
		sourcePath,
		uploadFileResp.TaskId,
		uploadFileResp.NodeType,
		needPure)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ProcessUploadFileByProxy failed.",
			zap.Error(err))
		return err, uploadFileResp
	}
	InfoLogger.WithContext(ctx).Debug(
		"UploadFileByProxy finish.")
	return err, uploadFileResp
}

func ProcessUploadFileByProxy(
	ctx context.Context,
	userId string,
	sourcePath string,
	taskId,
	nodeType int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ProcessUploadFileByProxy start.",
		zap.String("userId", userId),
		zap.String("sourcePath", sourcePath),
		zap.Int32("taskId", taskId),
		zap.Int32("nodeType", nodeType),
		zap.Bool("needPure", needPure))

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
	err = storage.Upload(
		ctx,
		userId,
		sourcePath,
		taskId,
		needPure)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"storage.Upload failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ProcessUploadFileByProxy finish.")
	return nil
}
