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

func UploadByProxy(
	userId,
	token,
	urchinServiceAddr,
	sourcePath,
	objectName,
	nodeName string) (
	err error, uploadObjectResp *UploadObjectResp) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	InfoLogger.WithContext(ctx).Debug(
		"UploadByProxy start.",
		zap.String("userId", userId),
		zap.String("token", "***"),
		zap.String("sourcePath", sourcePath),
		zap.String("objectName", objectName),
		zap.String("nodeName", nodeName))

	uploadObjectResp = new(UploadObjectResp)

	_, err = os.Stat(sourcePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err, uploadObjectResp
	}

	UClient.Init(
		ctx,
		userId,
		token,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	uploadObjectReq := new(UploadObjectReq)
	uploadObjectReq.UserId = userId
	uploadObjectReq.Name = objectName
	uploadObjectReq.SourceLocalPath = sourcePath
	if 0 != len(nodeName) {
		uploadObjectReq.NodeName = &nodeName
	}

	err, uploadObjectResp = UClient.UploadObject(ctx, uploadObjectReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.UploadObject failed.",
			zap.Error(err))
		return err, uploadObjectResp
	}

	uploadObjectRespBuf, _ := json.Marshal(uploadObjectResp)
	fmt.Printf("Upload Response: %s\n",
		string(uploadObjectRespBuf))

	err = ProcessUploadByProxy(
		ctx,
		userId,
		sourcePath,
		uploadObjectResp.TaskId,
		uploadObjectResp.NodeType,
		true)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ProcessUploadByProxy failed.",
			zap.Error(err))
		return err, uploadObjectResp
	}
	InfoLogger.WithContext(ctx).Debug(
		"UploadByProxy success.")
	return err, uploadObjectResp
}

func ProcessUploadByProxy(
	ctx context.Context,
	userId string,
	sourcePath string,
	taskId,
	nodeType int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ProcessUploadByProxy start.",
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
		"ProcessUploadByProxy success.")
	return nil
}
