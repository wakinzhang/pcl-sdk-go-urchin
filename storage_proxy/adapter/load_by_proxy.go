package adapter

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"os"
)

func LoadByProxy(
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

	if '/' != cachePath[len(cachePath)-1] {
		cachePath = cachePath + "/"
	}

	Logger.WithContext(ctx).Debug(
		"LoadByProxy start.",
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

	loadObjectReq := new(LoadObjectReq)
	loadObjectReq.UserId = userId
	loadObjectReq.ObjUuid = objUuid
	if nil != sourceNodeName {
		loadObjectReq.SourceNodeName = sourceNodeName
	}
	loadObjectReq.TargetNodeName = targetNodeName
	loadObjectReq.CacheLocalPath = cachePath

	err, loadObjectResp := UClient.LoadObject(ctx, loadObjectReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.LoadObject failed.",
			" err: ", err)
		return err
	}

	fmt.Printf("Load TaskId: %d\n", loadObjectResp.TaskId)

	err = ProcessLoadByProxy(
		ctx,
		userId,
		cachePath,
		objUuid,
		loadObjectResp.SourceBucketName,
		loadObjectResp.TaskId,
		loadObjectResp.SourceNodeType,
		loadObjectResp.TargetNodeType,
		needPure)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ProcessLoadByProxy failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"LoadByProxy finish.")
	return err
}

func ProcessLoadByProxy(
	ctx context.Context,
	userId,
	cachePath,
	objUuid,
	sourceBucketName string,
	taskId,
	sourceNodeType,
	targetNodeType int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"ProcessLoadByProxy start.",
		" userId: ", userId,
		" cachePath: ", cachePath,
		" objUuid: ", objUuid,
		" sourceBucketName: ", sourceBucketName,
		" taskId: ", taskId,
		" sourceNodeType: ", sourceNodeType,
		" targetNodeType: ", targetNodeType,
		" needPure: ", needPure)

	loadDownloadFinishFile :=
		cachePath + objUuid + ".load_download_finish"

	loadUploadFinishFile :=
		cachePath + objUuid + ".load_upload_finish"

	loadCachePath := cachePath + objUuid

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.UserId = userId
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
		_err := os.RemoveAll(loadCachePath)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" loadCachePath: ", loadCachePath,
				" err: ", _err)
		}
		_err = os.Remove(loadDownloadFinishFile)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" loadDownloadFinishFile: ", loadDownloadFinishFile,
				" err: ", _err)
		}
		_err = os.Remove(loadUploadFinishFile)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" loadUploadFinishFile: ", loadUploadFinishFile,
				" err: ", _err)
		}
	}()

	if needPure {
		err = os.RemoveAll(loadCachePath)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" loadCachePath: ", loadCachePath,
				" err: ", err)
			return err
		}
		err = os.Remove(loadDownloadFinishFile)
		if nil != err {
			if !os.IsNotExist(err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" loadDownloadFinishFile: ", loadDownloadFinishFile,
					" err: ", err)
				return err
			}
		}
		err = os.Remove(loadUploadFinishFile)
		if nil != err {
			if !os.IsNotExist(err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" loadUploadFinishFile: ", loadUploadFinishFile,
					" err: ", err)
				return err
			}
		}
	}

	_, err = os.Stat(loadDownloadFinishFile)
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
				userId,
				cachePath,
				taskId,
				sourceBucketName)

			if nil != err {
				Logger.WithContext(ctx).Error(
					"sourceStorage.Download failed.",
					" err: ", err)
				return err
			}
			_, err = os.Create(loadDownloadFinishFile)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"os.Create failed.",
					" loadDownloadFinishFile: ", loadDownloadFinishFile,
					" err: ", err)
				return err
			}
		} else {
			Logger.WithContext(ctx).Error(
				"os.Stat failed.",
				" loadDownloadFinishFile: ", loadDownloadFinishFile,
				" err: ", err)
			return err
		}
	}

	_, err = os.Stat(loadUploadFinishFile)
	if nil != err {
		if os.IsNotExist(err) {
			err, targetStorage := NewStorageProxy(ctx, targetNodeType)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"target NewStorageProxy failed.",
					" err: ", err)
				return err
			}
			entries, err := os.ReadDir(loadCachePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"os.ReadDir failed.",
					" loadCachePath: ", loadCachePath,
					" err: ", err)
				return err
			}
			if 0 == len(entries) {
				Logger.WithContext(ctx).Error(
					"object cache empty.",
					" loadCachePath: ", loadCachePath)
				return err
			}
			err = targetStorage.Upload(
				ctx,
				userId,
				loadCachePath,
				taskId,
				needPure)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"targetStorage.Upload failed.",
					" err: ", err)
				return err
			}
			_, err = os.Create(loadUploadFinishFile)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"os.Create failed.",
					" loadUploadFinishFile: ", loadUploadFinishFile,
					" err: ", err)
				return err
			}
		} else {
			Logger.WithContext(ctx).Error(
				"os.Stat failed.",
				" loadUploadFinishFile: ", loadUploadFinishFile,
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"ProcessLoadByProxy finish.")
	return nil
}
