package adapter

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
)

func Download(urchinServiceAddr, objUuid, targetPath string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Download start. urchinServiceAddr: %s, targetPath: %s, objUuid: %s",
		urchinServiceAddr, targetPath, objUuid)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

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
		targetPath,
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
