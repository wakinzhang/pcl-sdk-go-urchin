package operation

import (
	"context"
	"encoding/json"
	"errors"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage_proxy/adapter"
	"go.uber.org/zap"
)

func RetryTask(
	userId,
	token,
	urchinServiceAddr string,
	taskId int32,
	needPure bool) (err error) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	InfoLogger.WithContext(ctx).Debug(
		"RetryTask start.",
		zap.String("userId", userId),
		zap.String("token", "***"),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure))

	UClient.Init(
		ctx,
		userId,
		token,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	getTaskReq := new(GetTaskReq)
	getTaskReq.UserId = userId
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			zap.Error(err))
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		ErrorLogger.WithContext(ctx).Error(
			"task not exist.",
			zap.Int32("taskId", taskId))
		return errors.New("task not exist")
	}
	task := getTaskResp.Data.List[0].Task

	retryTaskReq := new(RetryTaskReq)
	retryTaskReq.UserId = userId
	retryTaskReq.TaskId = taskId

	err, _ = UClient.RetryTask(ctx, retryTaskReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.RetryTask failed.",
			zap.Error(err))
		return err
	}

	if _, exists := TaskTypeOnlyServiceRetry[task.Type]; exists {
		InfoLogger.WithContext(ctx).Debug(
			"RetryTask finish, Only service retry.",
			zap.Int32("taskId", task.Id),
			zap.Int32("taskType", task.Type))
		return err
	}

	switch task.Type {
	case TaskTypeUpload:
		err = processRetryUploadTask(
			ctx,
			userId,
			task,
			taskId,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"processRetryUploadTask failed.",
				zap.Error(err))
			return
		}
	case TaskTypeUploadFile:
		err = processRetryUploadFileTask(
			ctx,
			userId,
			task,
			taskId,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"processRetryUploadFileTask failed.",
				zap.Error(err))
			return
		}
	case TaskTypeDownload:
		err = processRetryDownloadTask(
			ctx,
			userId,
			task,
			taskId)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"processRetryDownloadTask failed.",
				zap.Error(err))
			return
		}
	case TaskTypeDownloadFile:
		err = processRetryDownloadFileTask(
			ctx,
			userId,
			task,
			taskId)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"processRetryDownloadFileTask failed.",
				zap.Error(err))
			return
		}
	case TaskTypeLoad:
		err = processRetryLoadTask(
			ctx,
			userId,
			task,
			taskId,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"processRetryLoadTask failed.",
				zap.Error(err))
			return
		}
	default:
		ErrorLogger.WithContext(ctx).Error(
			"task type invalid.",
			zap.Int32("taskId", task.Id),
			zap.Int32("taskType", task.Type))
	}

	InfoLogger.WithContext(ctx).Debug(
		"RetryTask finish.")
	return err
}

func processRetryUploadTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"processRetryUploadTask start.",
		zap.String("userId", userId),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure),
		zap.String("taskParams", task.Params))

	uploadObjectTaskParams := new(UploadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), uploadObjectTaskParams)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UploadObjectTaskParams Unmarshal failed.",
			zap.Error(err))
		return err
	}

	err = ProcessUploadByProxy(
		ctx,
		userId,
		uploadObjectTaskParams.Request.SourceLocalPath,
		taskId,
		uploadObjectTaskParams.NodeType,
		needPure)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ProcessUploadByProxy failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"processRetryUploadTask finish.")
	return err
}

func processRetryUploadFileTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"processRetryUploadFileTask start.",
		zap.String("userId", userId),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure),
		zap.String("taskParams", task.Params))

	uploadFileTaskParams := new(UploadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), uploadFileTaskParams)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UploadFileTaskParams Unmarshal failed.",
			zap.Error(err))
		return err
	}

	err = ProcessUploadFileByProxy(
		ctx,
		userId,
		uploadFileTaskParams.Request.SourceLocalPath,
		taskId,
		uploadFileTaskParams.NodeType,
		needPure)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ProcessUploadFileByProxy failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"processRetryUploadFileTask finish.")
	return err
}

func processRetryDownloadTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"processRetryDownloadTask start.",
		zap.String("userId", userId),
		zap.Int32("taskId", taskId),
		zap.String("taskParams", task.Params))

	downloadObjectTaskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadObjectTaskParams)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"DownloadObjectTaskParams Unmarshal failed.",
			zap.Error(err))
		return err
	}

	err = ProcessDownloadByProxy(
		ctx,
		userId,
		downloadObjectTaskParams.Request.TargetLocalPath,
		downloadObjectTaskParams.BucketName,
		taskId,
		downloadObjectTaskParams.NodeType)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ProcessDownloadByProxy failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"processRetryDownloadTask finish.")
	return err
}

func processRetryDownloadFileTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"processRetryDownloadFileTask start.",
		zap.String("userId", userId),
		zap.Int32("taskId", taskId),
		zap.String("taskParams", task.Params))

	downloadFileTaskParams := new(DownloadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadFileTaskParams)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"DownloadFileTaskParams Unmarshal failed.",
			zap.Error(err))
		return err
	}

	err = ProcessDownloadByProxy(
		ctx,
		userId,
		downloadFileTaskParams.Request.TargetLocalPath,
		downloadFileTaskParams.BucketName,
		taskId,
		downloadFileTaskParams.NodeType)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ProcessDownloadByProxy failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"processRetryDownloadFileTask finish.")
	return err
}

func processRetryLoadTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"processRetryLoadTask start.",
		zap.String("userId", userId),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure),
		zap.String("taskParams", task.Params))

	loadObjectTaskParams := new(LoadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), loadObjectTaskParams)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"LoadObjectTaskParams Unmarshal failed.",
			zap.Error(err))
		return err
	}

	err = ProcessLoadByProxy(
		ctx,
		userId,
		loadObjectTaskParams.Request.CacheLocalPath,
		loadObjectTaskParams.Request.ObjUuid,
		loadObjectTaskParams.SourceBucketName,
		taskId,
		loadObjectTaskParams.SourceNodeType,
		loadObjectTaskParams.TargetNodeType,
		needPure)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ProcessLoadByProxy failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"processRetryLoadTask finish.")
	return err
}
