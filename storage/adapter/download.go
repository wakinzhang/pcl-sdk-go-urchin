package adapter

import (
	"fmt"
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
	downloadObjectReq.UserId = "vg3yHwUa"
	downloadObjectReq.ObjUuid = objUuid
	downloadObjectReq.TargetLocalPath = targetPath

	err, downloadObjectResp := urchinService.DownloadObject(downloadObjectReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"UrchinService.DownloadObject failed. error: %v", err)
		return err
	}

	fmt.Printf("Download TaskId: %d\n", downloadObjectResp.TaskId)

	err = ProcessDownload(
		urchinServiceAddr,
		targetPath,
		downloadObjectResp.BucketName,
		downloadObjectResp.TaskId,
		downloadObjectResp.NodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessDownload failed. error: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Download success.")
	return err
}

func ProcessDownload(
	urchinServiceAddr, targetPath, bucketName string,
	taskId, nodeType int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessDownload start."+
		" urchinServiceAddr: %s targetPath: %s, bucketName: %s, taskId: %d, nodeType: %d",
		urchinServiceAddr, targetPath, bucketName, taskId, nodeType)

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
	err = storage.Download(
		urchinServiceAddr,
		targetPath,
		taskId,
		bucketName)

	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "storage.Download failed. error: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessDownload success.")
	return nil
}
