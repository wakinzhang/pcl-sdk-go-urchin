package adapter

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
)

func Migrate(
	urchinServiceAddr, objUuid string,
	sourceNodeId *int32, targetNodeId int32, cachePath string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Migrate start. urchinServiceAddr: %s, objUuid: %s,"+
			" sourceNodeId: %d, targetNodeId: %d",
		urchinServiceAddr, objUuid, sourceNodeId, targetNodeId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	migrateObjectReq := new(MigrateObjectReq)
	migrateObjectReq.UserId = "wakinzhang"
	migrateObjectReq.ObjUuid = objUuid
	if nil != sourceNodeId {
		migrateObjectReq.SourceNodeId = sourceNodeId
	}
	migrateObjectReq.TargetNodeId = targetNodeId

	err, migrateObjectResp := urchinService.MigrateObject(
		ConfigDefaultUrchinServiceMigrateObjectInterface,
		migrateObjectReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "UrchinService.MigrateObject failed."+
			" interface: %s, error: %v",
			ConfigDefaultUrchinServiceMigrateObjectInterface, err)
		return err
	}

	err, sourceStorage := NewStorage(migrateObjectResp.SourceNodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "NewStorage failed. error: %v", err)
		return err
	}
	err = sourceStorage.Download(
		urchinServiceAddr,
		cachePath,
		migrateObjectResp.TaskId,
		migrateObjectResp.SourceBucketName)

	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "sourceStorage.Download failed. error: %v", err)
		return err
	}

	err, targetStorage := NewStorage(migrateObjectResp.TargetNodeType)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "NewStorage failed. error: %v", err)
		return err
	}
	err = targetStorage.Upload(
		urchinServiceAddr,
		cachePath+"/"+migrateObjectResp.Location,
		migrateObjectResp.TaskId)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "migrateObjectResp.Upload failed. error: %v", err)
		return err
	}

	finishTaskReq := new(FinishTaskReq)
	finishTaskReq.TaskId = migrateObjectResp.TaskId
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
