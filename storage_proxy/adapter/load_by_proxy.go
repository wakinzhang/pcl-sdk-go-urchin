package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
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
	needPure bool) (
	err error,
	loadObjectResp *LoadObjectResp) {

	requestId := uuid.NewV4().String()
	var ctx context.Context
	ctx = context.Background()
	ctx = context.WithValue(ctx, "X-Request-Id", requestId)

	if '/' != cachePath[len(cachePath)-1] {
		cachePath = cachePath + "/"
	}

	InfoLogger.WithContext(ctx).Debug(
		"LoadByProxy start.",
		zap.String("userId", userId),
		zap.String("token", "***"),
		zap.String("objUuid", objUuid),
		zap.String("sourceNodeName", *sourceNodeName),
		zap.String("targetNodeName", targetNodeName),
		zap.String("cachePath", cachePath),
		zap.Bool("needPure", needPure))

	loadObjectResp = new(LoadObjectResp)

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
	loadObjectReq.SourceNodeName = sourceNodeName
	loadObjectReq.TargetNodeName = targetNodeName
	loadObjectReq.CacheLocalPath = cachePath

	err, loadObjectResp = UClient.LoadObject(ctx, loadObjectReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.LoadObject failed.",
			zap.Error(err))
		return err, loadObjectResp
	}

	loadObjectRespBuf, _ := json.Marshal(loadObjectResp)
	fmt.Printf("Load Response: %s\n",
		string(loadObjectRespBuf))

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
		ErrorLogger.WithContext(ctx).Error(
			"ProcessLoadByProxy failed.",
			zap.Error(err))
		return err, loadObjectResp
	}
	InfoLogger.WithContext(ctx).Debug(
		"LoadByProxy finish.")
	return err, loadObjectResp
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

	InfoLogger.WithContext(ctx).Debug(
		"ProcessLoadByProxy start.",
		zap.String("userId", userId),
		zap.String("cachePath", cachePath),
		zap.String("objUuid", objUuid),
		zap.String("sourceBucketName", sourceBucketName),
		zap.Int32("taskId", taskId),
		zap.Int32("sourceNodeType", sourceNodeType),
		zap.Int32("targetNodeType", targetNodeType),
		zap.Bool("needPure", needPure))

	loadDownloadFinishFile := cachePath +
		objUuid +
		fmt.Sprintf("_%d.load_download_finish", taskId)

	loadUploadFinishFile := cachePath +
		objUuid +
		fmt.Sprintf("_%d.load_upload_finish", taskId)

	loadCachePath := cachePath + objUuid + fmt.Sprintf("_%d", taskId)

	defer func() {
		finishTaskReq := new(FinishTaskReq)
		finishTaskReq.UserId = userId
		finishTaskReq.TaskId = taskId
		if nil != err {
			finishTaskReq.Result = TaskFResultEFailed
		} else {
			finishTaskReq.Result = TaskFResultESuccess
		}
		_err, _ := UClient.FinishTask(ctx, finishTaskReq)
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"UrchinClient.FinishTask failed.",
				zap.Error(_err))
			return
		}
		if TaskFResultESuccess == finishTaskReq.Result {
			_err = os.RemoveAll(loadCachePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("loadCachePath", loadCachePath),
					zap.Error(_err))
			}
			_err = os.Remove(loadDownloadFinishFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("loadDownloadFinishFile",
						loadDownloadFinishFile),
					zap.Error(_err))
			}
			_err = os.Remove(loadUploadFinishFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("loadUploadFinishFile",
						loadUploadFinishFile),
					zap.Error(_err))
			}
		}
	}()

	if needPure {
		err = os.RemoveAll(loadCachePath)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"os.Remove failed.",
				zap.String("loadCachePath", loadCachePath),
				zap.Error(err))
			return err
		}
		err = os.Remove(loadDownloadFinishFile)
		if nil != err {
			if !os.IsNotExist(err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("loadDownloadFinishFile",
						loadDownloadFinishFile),
					zap.Error(err))
				return err
			}
		}
		err = os.Remove(loadUploadFinishFile)
		if nil != err {
			if !os.IsNotExist(err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("loadUploadFinishFile",
						loadUploadFinishFile),
					zap.Error(err))
				return err
			}
		}
	}

	_, err = os.Stat(loadDownloadFinishFile)
	if nil != err {
		if os.IsNotExist(err) {
			err, sourceStorage := NewStorageProxy(ctx, sourceNodeType)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"source NewStorageProxy failed.",
					zap.Error(err))
				return err
			}
			err = sourceStorage.Download(
				ctx,
				userId,
				cachePath,
				taskId,
				sourceBucketName)

			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"sourceStorage.Download failed.",
					zap.Error(err))
				return err
			}
			_, err = os.Create(loadDownloadFinishFile)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"os.Create failed.",
					zap.String("loadDownloadFinishFile",
						loadDownloadFinishFile),
					zap.Error(err))
				return err
			}
		} else {
			ErrorLogger.WithContext(ctx).Error(
				"os.Stat failed.",
				zap.String("loadDownloadFinishFile",
					loadDownloadFinishFile),
				zap.Error(err))
			return err
		}
	}

	_, err = os.Stat(loadUploadFinishFile)
	if nil != err {
		if os.IsNotExist(err) {
			err, targetStorage := NewStorageProxy(ctx, targetNodeType)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"target NewStorageProxy failed.",
					zap.Error(err))
				return err
			}
			err = targetStorage.Upload(
				ctx,
				userId,
				loadCachePath,
				taskId,
				needPure)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"targetStorage.Upload failed.",
					zap.Error(err))
				return err
			}
			_, err = os.Create(loadUploadFinishFile)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"os.Create failed.",
					zap.String("loadUploadFinishFile",
						loadUploadFinishFile),
					zap.Error(err))
				return err
			}
		} else {
			ErrorLogger.WithContext(ctx).Error(
				"os.Stat failed.",
				zap.String("loadUploadFinishFile",
					loadUploadFinishFile),
				zap.Error(err))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"ProcessLoadByProxy finish.")
	return nil
}
