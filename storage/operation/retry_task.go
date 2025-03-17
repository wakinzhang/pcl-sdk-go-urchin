package operation

import (
	"encoding/json"
	"errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adapter"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
)

func RetryTask(
	urchinServiceAddr string,
	taskId int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "RetryTask start."+
		" urchinServiceAddr: %s, taskId: %d, needPure: %t",
		urchinServiceAddr, taskId, needPure)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := urchinService.GetTask(getTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. err: %v", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		obs.DoLog(obs.LEVEL_ERROR, "task not exist. taskId: %d", taskId)
		return errors.New("task not exist")
	}
	task := getTaskResp.Data.List[0].Task

	retryTaskReq := new(RetryTaskReq)
	retryTaskReq.TaskId = taskId

	err, _ = urchinService.RetryTask(retryTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "RetryTask failed. err: %v", err)
		return err
	}

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.TaskId = taskId
		if err != nil {
			finishTaskReq.Result = TaskFResultEFailed
		} else {
			finishTaskReq.Result = TaskFResultESuccess
		}
		err, _ = urchinService.FinishTask(finishTaskReq)
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"UrchinService.FinishTask failed. error: %v", err)
		}
	}()

	switch task.Type {
	case TaskTypeUpload:
		err = processRetryUploadTask(
			task,
			urchinServiceAddr,
			taskId,
			needPure)
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"processRetryUploadTask failed. error: %v", err)
			return err
		}
	case TaskTypeUploadFile:
		err = processRetryUploadFileTask(
			task,
			urchinServiceAddr,
			taskId,
			needPure)
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"processRetryUploadFileTask failed. error: %v", err)
			return err
		}
	case TaskTypeDownload:
		err = processRetryDownloadTask(
			task,
			urchinServiceAddr,
			taskId)
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"processRetryDownloadTask failed. error: %v", err)
			return err
		}
	case TaskTypeDownloadFile:
		err = processRetryDownloadFileTask(
			task,
			urchinServiceAddr,
			taskId)
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"processRetryDownloadFileTask failed. error: %v", err)
			return err
		}
	case TaskTypeMigrate:
		err = processRetryMigrateTask(
			task,
			urchinServiceAddr,
			taskId,
			needPure)
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"processRetryMigrateTask failed. error: %v", err)
			return err
		}
	default:
		obs.DoLog(obs.LEVEL_ERROR,
			"task type invalid."+
				" taskId: %d, taskType: %d", task.Id, task.Type)
		return errors.New("task type invalid")
	}
	obs.DoLog(obs.LEVEL_DEBUG, "RetryTask success.")
	return err
}

func processRetryUploadTask(
	task *Task,
	urchinServiceAddr string,
	taskId int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "processRetryUploadTask start."+
		" urchinServiceAddr: %s, taskId: %d, taskParams: %s, needPure: %t",
		urchinServiceAddr, taskId, task.Params, needPure)
	uploadObjectTaskParams := new(UploadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), uploadObjectTaskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"UploadObjectTaskParams Unmarshal failed."+
				" params: %s, error: %v", task.Params, err)
		return err
	}

	err = ProcessUpload(
		urchinServiceAddr,
		uploadObjectTaskParams.Request.SourceLocalPath,
		taskId,
		uploadObjectTaskParams.NodeType,
		needPure)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessUpload failed. error: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "processRetryUploadTask success.")
	return err
}

func processRetryUploadFileTask(
	task *Task,
	urchinServiceAddr string,
	taskId int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "processRetryUploadFileTask start."+
		" urchinServiceAddr: %s, taskId: %d, taskParams: %s, needPure: %t",
		urchinServiceAddr, taskId, task.Params, needPure)
	uploadFileTaskParams := new(UploadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), uploadFileTaskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"UploadFileTaskParams Unmarshal failed."+
				" params: %s, error: %v", task.Params, err)
		return err
	}

	err = ProcessUploadFile(
		urchinServiceAddr,
		uploadFileTaskParams.Request.SourceLocalPath,
		taskId,
		uploadFileTaskParams.NodeType,
		needPure)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessUploadFile failed. error: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "processRetryUploadFileTask success.")
	return err
}

func processRetryDownloadTask(
	task *Task,
	urchinServiceAddr string,
	taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "processRetryDownloadTask start."+
		" urchinServiceAddr: %s, taskId: %d, taskParams: %s",
		urchinServiceAddr, taskId, task.Params)
	downloadObjectTaskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadObjectTaskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"DownloadObjectTaskParams Unmarshal failed."+
				" params: %s, error: %v", task.Params, err)
		return err
	}

	err = ProcessDownload(
		urchinServiceAddr,
		downloadObjectTaskParams.Request.TargetLocalPath,
		downloadObjectTaskParams.BucketName,
		taskId,
		downloadObjectTaskParams.NodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessDownload failed. error: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "processRetryDownloadTask success.")
	return err
}

func processRetryDownloadFileTask(
	task *Task,
	urchinServiceAddr string,
	taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "processRetryDownloadFileTask start."+
		" urchinServiceAddr: %s, taskId: %d, taskParams: %s",
		urchinServiceAddr, taskId, task.Params)
	downloadFileTaskParams := new(DownloadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadFileTaskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"DownloadFileTaskParams Unmarshal failed."+
				" params: %s, error: %v", task.Params, err)
		return err
	}

	err = ProcessDownload(
		urchinServiceAddr,
		downloadFileTaskParams.Request.TargetLocalPath,
		downloadFileTaskParams.BucketName,
		taskId,
		downloadFileTaskParams.NodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessDownload failed. error: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "processRetryDownloadFileTask success.")
	return err
}

func processRetryMigrateTask(
	task *Task,
	urchinServiceAddr string,
	taskId int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "processRetryMigrateTask start."+
		" urchinServiceAddr: %s, taskId: %d, taskParams: %s, needPure: %t",
		urchinServiceAddr, taskId, task.Params, needPure)

	migrateObjectTaskParams := new(MigrateObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), migrateObjectTaskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"MigrateObjectTaskParams Unmarshal failed."+
				" params: %s, error: %v", task.Params, err)
		return err
	}

	err = ProcessMigrate(
		urchinServiceAddr,
		migrateObjectTaskParams.Request.CacheLocalPath,
		migrateObjectTaskParams.Request.ObjUuid,
		migrateObjectTaskParams.SourceBucketName,
		taskId,
		migrateObjectTaskParams.SourceNodeType,
		migrateObjectTaskParams.TargetNodeType,
		needPure)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessMigrate failed. error: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "processRetryMigrateTask success.")
	return err
}
