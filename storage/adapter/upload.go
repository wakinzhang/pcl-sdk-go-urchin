package adapter

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	"os"
	"path/filepath"
)

func Upload(
	urchinServiceAddr,
	sourcePath string) (err error) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	Logger.WithContext(ctx).Debug(
		"Upload start. sourcePath: ", sourcePath)

	UClient.Init(
		ctx,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	uploadObjectReq := new(UploadObjectReq)
	uploadObjectReq.UserId = DefaultUrchinClientUserId
	uploadObjectReq.Name = "wakinzhang-test-obj"

	stat, err := os.Stat(sourcePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}
	if stat.IsDir() {
		uploadObjectReq.Type = DataObjectTypeEFolder
	} else {
		uploadObjectReq.Type = DataObjectTypeEFile
	}
	uploadObjectReq.Source = filepath.Base(sourcePath)
	uploadObjectReq.SourceLocalPath = sourcePath

	err, uploadObjectResp := UClient.UploadObject(ctx, uploadObjectReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.UploadObject  failed.",
			" err: ", err)
		return err
	}

	fmt.Printf("Upload TaskId: %d\n", uploadObjectResp.TaskId)

	err = ProcessUpload(
		ctx,
		sourcePath,
		uploadObjectResp.TaskId,
		uploadObjectResp.NodeType,
		true)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessUpload failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"Upload success.")
	return err
}

func ProcessUpload(
	ctx context.Context,
	sourcePath string,
	taskId, nodeType int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessUpload start.",
		" sourcePath: ", sourcePath, " taskId: ", taskId,
		" nodeType: ", nodeType, " needPure: ", needPure)

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
		"ProcessUpload success.")
	return nil
}
