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

	Logger.WithContext(ctx).Debug(
		"RetryTask start.",
		" userId: ", userId,
		" token: ", "***",
		" taskId: ", taskId,
		" needPure: ", needPure)

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
		Logger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			" err: ", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		Logger.WithContext(ctx).Error(
			"task not exist. taskId: ", taskId)
		return errors.New("task not exist")
	}
	task := getTaskResp.Data.List[0].Task

	retryTaskReq := new(RetryTaskReq)
	retryTaskReq.UserId = userId
	retryTaskReq.TaskId = taskId

	err, _ = UClient.RetryTask(ctx, retryTaskReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.RetryTask failed.",
			" err: ", err)
		return err
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
		err, _ = UClient.FinishTask(ctx, finishTaskReq)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"UrchinClient.FinishTask failed.",
				" err: ", err)
		}
	}()

	switch task.Type {
	case TaskTypeUpload:
		err = processRetryUploadTask(
			ctx,
			userId,
			task,
			taskId,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"processRetryUploadTask failed.",
				" err: ", err)
			return err
		}
	case TaskTypeUploadFile:
		err = processRetryUploadFileTask(
			ctx,
			userId,
			task,
			taskId,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"processRetryUploadFileTask failed.",
				" err: ", err)
			return err
		}
	case TaskTypeDownload:
		err = processRetryDownloadTask(
			ctx,
			userId,
			task,
			taskId)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"processRetryDownloadTask failed.",
				" err: ", err)
			return err
		}
	case TaskTypeDownloadFile:
		err = processRetryDownloadFileTask(
			ctx,
			userId,
			task,
			taskId)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"processRetryDownloadFileTask failed.",
				" err: ", err)
			return err
		}
	case TaskTypeMigrate:
		err = processRetryLoadTask(
			ctx,
			userId,
			task,
			taskId,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"processRetryLoadTask failed.",
				" err: ", err)
			return err
		}
	default:
		Logger.WithContext(ctx).Error(
			"task type invalid.",
			" taskId: ", task.Id, " taskType: ", task.Type)
		return errors.New("task type invalid")
	}

	Logger.WithContext(ctx).Debug(
		"RetryTask finish.")
	return err
}

func processRetryUploadTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryUploadTask start.",
		" userId: ", userId,
		" taskId: ", taskId,
		" needPure: ", needPure,
		" taskParams: ", task.Params)

	uploadObjectTaskParams := new(UploadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), uploadObjectTaskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UploadObjectTaskParams Unmarshal failed.",
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"ProcessUploadByProxy failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryUploadTask finish.")
	return err
}

func processRetryUploadFileTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryUploadFileTask start.",
		" userId: ", userId,
		" taskId: ", taskId,
		" needPure: ", needPure,
		" taskParams: ", task.Params)

	uploadFileTaskParams := new(UploadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), uploadFileTaskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UploadFileTaskParams Unmarshal failed.",
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"ProcessUploadFileByProxy failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryUploadFileTask finish.")
	return err
}

func processRetryDownloadTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryDownloadTask start.",
		" userId: ", userId,
		" taskId: ", taskId,
		" taskParams: ", task.Params)

	downloadObjectTaskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadObjectTaskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"DownloadObjectTaskParams Unmarshal failed.",
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"ProcessDownloadByProxy failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryDownloadTask finish.")
	return err
}

func processRetryDownloadFileTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryDownloadFileTask start.",
		" userId: ", userId,
		" taskId: ", taskId,
		" taskParams: ", task.Params)

	downloadFileTaskParams := new(DownloadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadFileTaskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"DownloadFileTaskParams Unmarshal failed.",
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"ProcessDownloadByProxy failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryDownloadFileTask finish.")
	return err
}

func processRetryLoadTask(
	ctx context.Context,
	userId string,
	task *TaskData,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryLoadTask start.",
		" userId: ", userId,
		" taskId: ", taskId,
		" needPure: ", needPure,
		" taskParams: ", task.Params)

	loadObjectTaskParams := new(LoadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), loadObjectTaskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"LoadObjectTaskParams Unmarshal failed.",
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"ProcessLoadByProxy failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryLoadTask finish.")
	return err
}
