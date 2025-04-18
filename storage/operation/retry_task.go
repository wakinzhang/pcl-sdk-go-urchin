package operation

import (
	"context"
	"encoding/json"
	"errors"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adapter"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
)

func RetryTask(
	ctx context.Context,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"RetryTask start.",
		" taskId: ", taskId,
		" needPure: ", needPure)

	getTaskReq := new(GetTaskReq)
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
			task,
			taskId)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"processRetryDownloadFileTask failed.",
				" err: ", err)
			return err
		}
	case TaskTypeMigrate:
		err = processRetryMigrateTask(
			ctx,
			task,
			taskId,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"processRetryMigrateTask failed.",
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
	task *Task,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryUploadTask start.",
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

	err = ProcessUpload(
		ctx,
		uploadObjectTaskParams.Request.SourceLocalPath,
		taskId,
		uploadObjectTaskParams.NodeType,
		needPure)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessUpload failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryUploadTask finish.")
	return err
}

func processRetryUploadFileTask(
	ctx context.Context,
	task *Task,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryUploadFileTask start.",
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

	err = ProcessUploadFile(
		ctx,
		uploadFileTaskParams.Request.SourceLocalPath,
		taskId,
		uploadFileTaskParams.NodeType,
		needPure)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessUploadFile failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryUploadFileTask finish.")
	return err
}

func processRetryDownloadTask(
	ctx context.Context,
	task *Task,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryDownloadTask start.",
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

	err = ProcessDownload(
		ctx,
		downloadObjectTaskParams.Request.TargetLocalPath,
		downloadObjectTaskParams.BucketName,
		taskId,
		downloadObjectTaskParams.NodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessDownload failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryDownloadTask finish.")
	return err
}

func processRetryDownloadFileTask(
	ctx context.Context,
	task *Task,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryDownloadFileTask start.",
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

	err = ProcessDownload(
		ctx,
		downloadFileTaskParams.Request.TargetLocalPath,
		downloadFileTaskParams.BucketName,
		taskId,
		downloadFileTaskParams.NodeType)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessDownload failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryDownloadFileTask finish.")
	return err
}

func processRetryMigrateTask(
	ctx context.Context,
	task *Task,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"processRetryMigrateTask start.",
		" taskId: ", taskId,
		" needPure: ", needPure,
		" taskParams: ", task.Params)

	migrateObjectTaskParams := new(MigrateObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), migrateObjectTaskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"MigrateObjectTaskParams Unmarshal failed.",
			" err: ", err)
		return err
	}

	err = ProcessMigrate(
		ctx,
		migrateObjectTaskParams.Request.CacheLocalPath,
		migrateObjectTaskParams.Request.ObjUuid,
		migrateObjectTaskParams.SourceBucketName,
		taskId,
		migrateObjectTaskParams.SourceNodeType,
		migrateObjectTaskParams.TargetNodeType,
		needPure)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessMigrate failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"processRetryMigrateTask finish.")
	return err
}
