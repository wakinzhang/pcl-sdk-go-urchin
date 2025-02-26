package adapter

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
)

func DownloadFile(urchinServiceAddr, objUuid, source, targetPath string) (err error) {
	obs.DoLog(obs.LEVEL_DEBUG,
		"DownloadFile start. urchinServiceAddr: %s, objUuid: %s,"+
			" source: %s, targetPath: %s",
		urchinServiceAddr, objUuid, source, targetPath)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	downloadFileReq := new(DownloadFileReq)
	downloadFileReq.UserId = "wakinzhang"
	downloadFileReq.ObjUuid = objUuid
	downloadFileReq.Source = source

	err, downloadFileResp := urchinService.DownloadFile(downloadFileReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"UrchinService.DownloadFile failed. error: %v", err)
		return err
	}

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.TaskId = downloadFileResp.TaskId
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

	err, storage := NewStorage(downloadFileResp.NodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "NewStorage failed. error: %v", err)
		return err
	}
	err = storage.Download(
		urchinServiceAddr,
		targetPath,
		downloadFileResp.TaskId,
		downloadFileResp.BucketName)

	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "storage.Download failed. error: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "DownloadFile success.")
	return nil
}
