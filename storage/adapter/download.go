package adapter

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
)

func Download(urchinServiceAddr, objUuid string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Download start. urchinServiceAddr: %s objUuid: %s",
		urchinServiceAddr, objUuid)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	/*
		getObjectReq := new(GetObjectReq)
		getObjectReq.UserId = "wakinzhang"
		getObjectReq.ObjUuid = &objUuid
		getObjectReq.PageIndex = 0
		getObjectReq.PageSize = 10

		err, getObjectResp := urchinService.GetObject(
			ConfigDefaultUrchinServiceGetObjectInterface,
			getObjectReq)
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR, "UrchinService.GetObject failed."+
				" interface: %s, error: %v",
				ConfigDefaultUrchinServiceGetObjectInterface, err)
			return err
		}

		if 0 == len(getObjectResp.Data.List) {
			err = errors.New("no valid data object")
			obs.DoLog(obs.LEVEL_ERROR, "UrchinService.GetObject failed."+
				" ObjUuid: %s, error: %v", objUuid, err)
			return err
		}
	*/

	downloadObjectReq := new(DownloadObjectReq)
	downloadObjectReq.UserId = "wakinzhang"
	downloadObjectReq.ObjUuid = objUuid

	err, downloadObjectResp := urchinService.DownloadObject(
		ConfigDefaultUrchinServiceDownloadObjectInterface,
		downloadObjectReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "UrchinService.DownloadObject failed."+
			" interface: %s, error: %v",
			ConfigDefaultUrchinServiceDownloadObjectInterface, err)
		return err
	}

	err, storage := NewStorage(downloadObjectResp.NodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "NewStorage failed. error: %v", err)
		return err
	}
	err = storage.Download(
		urchinServiceAddr,
		downloadObjectResp.TaskId)

	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "storage.Download failed. error: %v", err)
		return err
	}

	finishTaskReq := new(FinishTaskReq)
	finishTaskReq.TaskId = downloadObjectResp.TaskId
	finishTaskReq.Result = TaskFResultESuccess
	err, _ = urchinService.FinishTask(
		ConfigDefaultUrchinServiceFinishTaskInterface,
		finishTaskReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "UrchinService.FinishTask failed."+
			" interface: %s, error: %v",
			ConfigDefaultUrchinServiceFinishTaskInterface, err)
		return err
	}
	return nil
}
