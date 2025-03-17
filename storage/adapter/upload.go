package adapter

import (
	"fmt"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
	"os"
	"path/filepath"
)

func Upload(urchinServiceAddr, sourcePath string) (err error) {
	obs.DoLog(obs.LEVEL_DEBUG, "Upload start."+
		" urchinServiceAddr: %s sourcePath: %s",
		urchinServiceAddr, sourcePath)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	uploadObjectReq := new(UploadObjectReq)
	uploadObjectReq.UserId = "vg3yHwUa"
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
	uploadObjectReq.SourceLocalPath = sourcePath

	err, uploadObjectResp := urchinService.UploadObject(uploadObjectReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"UrchinService.UploadObject failed. error: %v", err)
		return err
	}

	fmt.Printf("Upload TaskId: %d\n", uploadObjectResp.TaskId)

	err = ProcessUpload(
		urchinServiceAddr,
		sourcePath,
		uploadObjectResp.TaskId,
		uploadObjectResp.NodeType,
		true)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessUpload failed. error: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Upload success.")
	return err
}

func ProcessUpload(
	urchinServiceAddr, sourcePath string,
	taskId, nodeType int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessUpload start."+
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

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessUpload success.")
	return nil
}
