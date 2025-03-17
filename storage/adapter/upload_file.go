package adapter

import (
	"fmt"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
)

func UploadFile(
	urchinServiceAddr, objUuid, objPath, sourcePath string,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "UploadFile start."+
		" urchinServiceAddr: %s objUuid: %s, objPath: %s, sourcePath: %s, needPure: %t",
		urchinServiceAddr, objUuid, objPath, sourcePath, needPure)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	uploadFileReq := new(UploadFileReq)
	uploadFileReq.UserId = "vg3yHwUa"
	uploadFileReq.ObjUuid = objUuid
	uploadFileReq.Source = objPath
	uploadFileReq.SourceLocalPath = sourcePath

	err, uploadFileResp := urchinService.UploadFile(uploadFileReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"UrchinService.UploadFile failed. error: %v", err)
		return err
	}

	fmt.Printf("UploadFile TaskId: %d\n", uploadFileResp.TaskId)

	err = ProcessUploadFile(
		urchinServiceAddr,
		sourcePath,
		uploadFileResp.TaskId,
		uploadFileResp.NodeType,
		needPure)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessUploadFile failed. error: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "UploadFile success.")
	return err
}

func ProcessUploadFile(
	urchinServiceAddr, sourcePath string,
	taskId, nodeType int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessUploadFile start."+
		" urchinServiceAddr: %s sourcePath: %s, taskId: %d, nodeType: %d, needPure: %t",
		urchinServiceAddr, sourcePath, taskId, nodeType, needPure)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)
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

	err, storage := NewStorage(nodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "NewStorage failed. error: %v", err)
		return err
	}
	err = storage.Upload(
		urchinServiceAddr,
		sourcePath,
		taskId,
		needPure)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "storage.Upload failed. error: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessUploadFile success.")
	return nil
}
