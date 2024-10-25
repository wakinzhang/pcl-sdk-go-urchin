package adapter

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"os"
	. "pcl-sdk-go-urchin/storage/common"
	. "pcl-sdk-go-urchin/storage/proto/api/v1"
	. "pcl-sdk-go-urchin/storage/service"
)

func Upload(urchinServiceAddr, sourcePath string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "Upload start."+
		" urchinServiceAddr: %s sourcePath: %s", urchinServiceAddr, sourcePath)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	uploadObjectReq := new(UploadObjectReq)
	stat, err := os.Stat(sourcePath)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. sourcePath: %s, err: %v", sourcePath, err)
		return err
	}
	if stat.IsDir() {
		uploadObjectReq.Type = DataObjectTypeEFolder
	} else {
		uploadObjectReq.Type = DataObjectTypeEFile
	}

	err, uploadObjectResp := urchinService.UploadObject(
		ConfigDefaultUrchinServiceUploadObjectInterface,
		uploadObjectReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "UrchinService.UploadObject failed."+
			" interface: %s, error: %v",
			ConfigDefaultUrchinServiceUploadObjectInterface, err)
		return err
	}

	err, storage := NewStorage(uploadObjectResp.NodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "NewStorage failed. error: %v", err)
		return err
	}
	err = storage.Upload(urchinServiceAddr, sourcePath, uploadObjectResp.TaskId)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "storage.Upload failed. error: %v", err)
		return err
	}

	finishTaskReq := new(FinishTaskReq)
	finishTaskReq.TaskId = uploadObjectResp.TaskId
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
