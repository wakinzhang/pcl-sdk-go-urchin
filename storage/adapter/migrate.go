package adapter

import (
	"fmt"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
	"os"
)

func Migrate(
	urchinServiceAddr, objUuid string,
	sourceNodeName *string,
	targetNodeName string,
	cachePath string,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Migrate start. urchinServiceAddr: %s, objUuid: %s,"+
			" sourceNodeName: %s, targetNodeName: %s, needPure: %t",
		urchinServiceAddr, objUuid, sourceNodeName, targetNodeName, needPure)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	migrateObjectReq := new(MigrateObjectReq)
	migrateObjectReq.UserId = "vg3yHwUa"
	migrateObjectReq.ObjUuid = objUuid
	if nil != sourceNodeName {
		migrateObjectReq.SourceNodeName = sourceNodeName
	}
	migrateObjectReq.TargetNodeName = targetNodeName
	migrateObjectReq.CacheLocalPath = cachePath

	err, migrateObjectResp := urchinService.MigrateObject(migrateObjectReq)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"UrchinService.MigrateObject failed. error: %v", err)
		return err
	}

	fmt.Printf("Migrate TaskId: %d\n", migrateObjectResp.TaskId)

	err = ProcessMigrate(
		urchinServiceAddr,
		cachePath,
		objUuid,
		migrateObjectResp.SourceBucketName,
		migrateObjectResp.TaskId,
		migrateObjectResp.SourceNodeType,
		migrateObjectResp.TargetNodeType,
		needPure)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"ProcessMigrate failed. error: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Download success.")
	return err
}

func ProcessMigrate(
	urchinServiceAddr, cachePath, objUuid, sourceBucketName string,
	taskId, sourceNodeType, targetNodeType int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ProcessMigrate start."+
		" urchinServiceAddr: %s cachePath: %s, objUuid: %s, sourceBucketName: %s,"+
		" taskId: %d, sourceNodeType: %d, targetNodeType: %d, needPure: %t",
		urchinServiceAddr, cachePath, objUuid, sourceBucketName, taskId,
		sourceNodeType, targetNodeType, needPure)

	migrateDownloadFinishFile := cachePath + objUuid + ".migrate_download_finish"
	migrateUploadFinishFile := cachePath + objUuid + ".migrate_upload_finish"
	migrateCachePath := cachePath + "/" + objUuid

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
			return
		}
		_err := os.RemoveAll(migrateCachePath)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"os.Remove failed. migrateCachePath: %s, error: %v",
				migrateCachePath, _err)
		}
		_err = os.Remove(migrateDownloadFinishFile)
		if nil != _err {
			obs.DoLog(obs.LEVEL_ERROR,
				"os.Remove failed. migrateDownloadFinishFile: %s, err: %v",
				migrateDownloadFinishFile, _err)
		}
		_err = os.Remove(migrateUploadFinishFile)
		if nil != _err {
			obs.DoLog(obs.LEVEL_ERROR,
				"os.Remove failed. migrateUploadFinishFile: %s, err: %v",
				migrateUploadFinishFile, _err)
		}
	}()

	if needPure {
		err = os.RemoveAll(migrateCachePath)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"os.RemoveAll failed. migrateCachePath: %s, error: %v",
				migrateCachePath, err)
			return err
		}
		err = os.Remove(migrateDownloadFinishFile)
		if nil != err {
			if !os.IsNotExist(err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. migrateDownloadFinishFile: %s, err: %v",
					migrateDownloadFinishFile, err)
				return err
			}
		}
		err = os.Remove(migrateUploadFinishFile)
		if nil != err {
			if !os.IsNotExist(err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. migrateUploadFinishFile: %s, err: %v",
					migrateUploadFinishFile, err)
				return err
			}
		}
	}

	_, err = os.Stat(migrateDownloadFinishFile)
	if nil != err {
		if os.IsNotExist(err) {
			err, sourceStorage := NewStorage(sourceNodeType)
			if nil != err {
				obs.DoLog(obs.LEVEL_ERROR,
					"source NewStorage failed. error: %v", err)
				return err
			}
			err = sourceStorage.Download(
				urchinServiceAddr,
				cachePath,
				taskId,
				sourceBucketName)

			if nil != err {
				obs.DoLog(obs.LEVEL_ERROR,
					"sourceStorage.Download failed. error: %v", err)
				return err
			}
			_, err = os.Create(migrateDownloadFinishFile)
			if nil != err {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Create failed.  migrateDownloadFinishFile: %s error: %v",
					migrateDownloadFinishFile, err)
				return err
			}
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"os.Stat failed. migrateDownloadFinishFile: %s, err: %v",
				migrateDownloadFinishFile, err)
			return err
		}
	}

	_, err = os.Stat(migrateUploadFinishFile)
	if nil != err {
		if os.IsNotExist(err) {
			err, targetStorage := NewStorage(targetNodeType)
			if nil != err {
				obs.DoLog(obs.LEVEL_ERROR,
					"target NewStorage failed. error: %v", err)
				return err
			}
			entries, err := os.ReadDir(migrateCachePath)
			if nil != err {
				obs.DoLog(obs.LEVEL_ERROR, "os.ReadDir failed."+
					" dir: %s, error: %v", migrateCachePath, err)
				return err
			}
			if 0 == len(entries) {
				obs.DoLog(obs.LEVEL_ERROR, "object cache empty."+
					" dir: %s", migrateCachePath)
				return err
			}
			err = targetStorage.Upload(
				urchinServiceAddr,
				migrateCachePath+"/"+entries[0].Name(),
				taskId,
				needPure)
			if nil != err {
				obs.DoLog(obs.LEVEL_ERROR,
					"targetStorage.Upload failed. error: %v", err)
				return err
			}
			_, err = os.Create(migrateUploadFinishFile)
			if nil != err {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Create failed.  migrateUploadFinishFile: %s error: %v",
					migrateUploadFinishFile, err)
				return err
			}
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"os.Stat failed. migrateUploadFinishFile: %s, err: %v",
				migrateUploadFinishFile, err)
			return err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "ProcessMigrate success.")
	return nil
}
