package adapter

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
	"os"
	"path/filepath"
)

func Upload(urchinServiceAddr, sourcePath string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "Upload start."+
		" urchinServiceAddr: %s sourcePath: %s", urchinServiceAddr, sourcePath)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	uploadObjectReq := new(UploadObjectReq)
	uploadObjectReq.UserId = "wakinzhang"
	uploadObjectReq.Name = "wakinzhang-test-obj"

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
	uploadObjectReq.Source = filepath.Base(sourcePath)

	err, uploadObjectResp := urchinService.UploadObject(uploadObjectReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"UrchinService.UploadObject failed. error: %v", err)
		return err
	}

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.TaskId = uploadObjectResp.TaskId
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

	obs.DoLog(obs.LEVEL_DEBUG, "Upload success.")
	return nil
}
