package adapter

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"os"
	. "pcl-sdk-go-urchin/client"
	. "pcl-sdk-go-urchin/common"
	. "pcl-sdk-go-urchin/module"
)

func MigrateByProxy(
	userId string,
	token string,
	urchinServiceAddr string,
	objUuid string,
	sourceNodeName *string,
	targetNodeName string,
	cachePath string,
	needPure bool) (err error) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	Logger.WithContext(ctx).Debug(
		"MigrateByProxy start.",
		" userId: ", userId,
		" token: ", "***",
		" objUuid: ", objUuid,
		" sourceNodeName: ", *sourceNodeName,
		" targetNodeName: ", targetNodeName,
		" cachePath: ", cachePath,
		" needPure: ", needPure)

	UClient.Init(
		ctx,
		userId,
		token,
		urchinServiceAddr,
		DefaultUClientReqTimeout,
		DefaultUClientMaxConnection)

	migrateObjectReq := new(MigrateObjectReq)
	migrateObjectReq.UserId = userId
	migrateObjectReq.ObjUuid = objUuid
	if nil != sourceNodeName {
		migrateObjectReq.SourceNodeName = sourceNodeName
	}
	migrateObjectReq.TargetNodeName = targetNodeName
	migrateObjectReq.CacheLocalPath = cachePath

	err, migrateObjectResp := UClient.MigrateObject(ctx, migrateObjectReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.MigrateObject  failed.",
			" err: ", err)
		return err
	}

	fmt.Printf("Migrate TaskId: %d\n", migrateObjectResp.TaskId)

	err = ProcessMigrateByProxy(
		ctx,
		cachePath,
		objUuid,
		migrateObjectResp.SourceBucketName,
		migrateObjectResp.TaskId,
		migrateObjectResp.SourceNodeType,
		migrateObjectResp.TargetNodeType,
		needPure)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessMigrateByProxy failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"MigrateByProxy finish.")
	return err
}

func ProcessMigrateByProxy(
	ctx context.Context,
	cachePath,
	objUuid,
	sourceBucketName string,
	taskId,
	sourceNodeType,
	targetNodeType int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessMigrateByProxy start.",
		" cachePath: ", cachePath,
		" objUuid: ", objUuid,
		" sourceBucketName: ", sourceBucketName,
		" taskId: ", taskId,
		" sourceNodeType: ", sourceNodeType,
		" targetNodeType: ", targetNodeType,
		" needPure: ", needPure)

	migrateDownloadFinishFile :=
		cachePath + objUuid + ".migrate_download_finish"

	migrateUploadFinishFile :=
		cachePath + objUuid + ".migrate_upload_finish"

	migrateCachePath := cachePath + "/" + objUuid

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.TaskId = taskId
		if nil != err {
			finishTaskReq.Result = TaskFResultEFailed
		} else {
			finishTaskReq.Result = TaskFResultESuccess
		}
		err, _ = UClient.FinishTask(ctx, finishTaskReq)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"UrchinClient.FinishTask failed.",
				" err: ", err)
			return
		}
		_err := os.RemoveAll(migrateCachePath)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" migrateCachePath: ", migrateCachePath,
				" err: ", _err)
		}
		_err = os.Remove(migrateDownloadFinishFile)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" migrateDownloadFinishFile: ", migrateDownloadFinishFile,
				" err: ", _err)
		}
		_err = os.Remove(migrateUploadFinishFile)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" migrateUploadFinishFile: ", migrateUploadFinishFile,
				" err: ", _err)
		}
	}()

	if needPure {
		err = os.RemoveAll(migrateCachePath)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" migrateCachePath: ", migrateCachePath,
				" err: ", err)
			return err
		}
		err = os.Remove(migrateDownloadFinishFile)
		if nil != err {
			if !os.IsNotExist(err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" migrateDownloadFinishFile: ", migrateDownloadFinishFile,
					" err: ", err)
				return err
			}
		}
		err = os.Remove(migrateUploadFinishFile)
		if nil != err {
			if !os.IsNotExist(err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" migrateUploadFinishFile: ", migrateUploadFinishFile,
					" err: ", err)
				return err
			}
		}
	}

	_, err = os.Stat(migrateDownloadFinishFile)
	if nil != err {
		if os.IsNotExist(err) {
			err, sourceStorage := NewStorageProxy(ctx, sourceNodeType)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"source NewStorageProxy failed.",
					" err: ", err)
				return err
			}
			err = sourceStorage.Download(
				ctx,
				cachePath,
				taskId,
				sourceBucketName)

			if nil != err {
				Logger.WithContext(ctx).Error(
					"sourceStorage.Download failed.",
					" err: ", err)
				return err
			}
			_, err = os.Create(migrateDownloadFinishFile)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"os.Create failed.",
					" migrateDownloadFinishFile: ", migrateDownloadFinishFile,
					" err: ", err)
				return err
			}
		} else {
			Logger.WithContext(ctx).Error(
				"os.Stat failed.",
				" migrateDownloadFinishFile: ", migrateDownloadFinishFile,
				" err: ", err)
			return err
		}
	}

	_, err = os.Stat(migrateUploadFinishFile)
	if nil != err {
		if os.IsNotExist(err) {
			err, targetStorage := NewStorageProxy(ctx, targetNodeType)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"target NewStorageProxy failed.",
					" err: ", err)
				return err
			}
			entries, err := os.ReadDir(migrateCachePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"os.ReadDir failed.",
					" migrateCachePath: ", migrateCachePath,
					" err: ", err)
				return err
			}
			if 0 == len(entries) {
				Logger.WithContext(ctx).Error(
					"object cache empty.",
					" migrateCachePath: ", migrateCachePath)
				return err
			}
			err = targetStorage.Upload(
				ctx,
				migrateCachePath+"/"+entries[0].Name(),
				taskId,
				needPure)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"targetStorage.Upload failed.",
					" err: ", err)
				return err
			}
			_, err = os.Create(migrateUploadFinishFile)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"os.Create failed.",
					" migrateUploadFinishFile: ", migrateUploadFinishFile,
					" err: ", err)
				return err
			}
		} else {
			Logger.WithContext(ctx).Error(
				"os.Stat failed.",
				" migrateUploadFinishFile: ", migrateUploadFinishFile,
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"ProcessMigrateByProxy finish.")
	return nil
}
