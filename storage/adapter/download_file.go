package adapter

import (
	"fmt"
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
	downloadFileReq.UserId = "vg3yHwUa"
	downloadFileReq.ObjUuid = objUuid
	downloadFileReq.Source = source
	downloadFileReq.TargetLocalPath = targetPath

	err, downloadFileResp := urchinService.DownloadFile(downloadFileReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"UrchinService.DownloadFile failed. error: %v", err)
		return err
	}

	fmt.Printf("DownloadFile TaskId: %d\n", downloadFileResp.TaskId)

	err = ProcessDownloadFile(
		urchinServiceAddr,
		targetPath,
		downloadFileResp.BucketName,
		downloadFileResp.TaskId,
		downloadFileResp.NodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessDownloadFile failed. error: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "DownloadFile success.")
	return err
}

func ProcessDownloadFile(
	urchinServiceAddr, targetPath, bucketName string,
	taskId, nodeType int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessDownloadFile start."+
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

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessDownloadFile success.")
	return nil
}
