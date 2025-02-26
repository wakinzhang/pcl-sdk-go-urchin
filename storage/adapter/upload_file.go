package adapter

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
)

func UploadFile(urchinServiceAddr, objUuid, objPath, sourcePath string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "UploadFile start."+
		" urchinServiceAddr: %s objUuid: %s, objPath: %s, sourcePath: %s",
		urchinServiceAddr, objUuid, objPath, sourcePath)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	uploadFileReq := new(UploadFileReq)
	uploadFileReq.UserId = "wakinzhang"
	uploadFileReq.ObjUuid = objUuid
	uploadFileReq.Source = objPath

	err, uploadFileResp := urchinService.UploadFile(uploadFileReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"UrchinService.UploadFile failed. error: %v", err)
		return err
	}

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.TaskId = uploadFileResp.TaskId
		if err != nil {
			finishTaskReq.Result = TaskFResultEFailed
		} else {
			finishTaskReq.Result = TaskFResultESuccess
		}
		_err, _ := urchinService.FinishTask(finishTaskReq)
		if nil != _err {
			obs.DoLog(obs.LEVEL_ERROR,
				"UrchinService.FinishTask failed. error: %v", _err)
		}
	}()

	err, storage := NewStorage(uploadFileResp.NodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "NewStorage failed. error: %v", err)
		return err
	}
	err = storage.Upload(urchinServiceAddr, sourcePath, uploadFileResp.TaskId)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "storage.Upload failed. error: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "UploadFile success.")
	return nil
}
