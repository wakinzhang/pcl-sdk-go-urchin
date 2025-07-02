package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
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

	Logger.WithContext(ctx).Debug(
		"UploadByProxy start.",
		" userId: ", userId,
		" token: ", "***",
		" sourcePath: ", sourcePath,
		" objectName: ", objectName,
		" nodeName: ", nodeName)

	uploadObjectResp = new(UploadObjectResp)

	_, err = os.Stat(sourcePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"UrchinClient.UploadObject failed.",
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"ProcessUploadByProxy failed.",
			" err: ", err)
		return err, uploadObjectResp
	}
	Logger.WithContext(ctx).Debug(
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

	Logger.WithContext(ctx).Debug(
		"ProcessUploadByProxy start.",
		" userId: ", userId,
		" sourcePath: ", sourcePath,
		" taskId: ", taskId,
		" nodeType: ", nodeType,
		" needPure: ", needPure)

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
	err = storage.Upload(
		ctx,
		userId,
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
		"ProcessUploadByProxy success.")
	return nil
}
