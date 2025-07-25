package adaptee

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/urchinfs/go-urchin2-sdk/ipfs_api"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"os"
	"path/filepath"
	"strings"
)

type IPFSProxy struct {
}

func (o *IPFSProxy) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("UploadMultiTaskNum", config.UploadMultiTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	ErrorLogger.WithContext(ctx).Error(
		"IPFSProxy not support concurrency config.")

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:SetConcurrency finish.")
	return nil
}

func (o *IPFSProxy) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:SetRate start.")

	ErrorLogger.WithContext(ctx).Error(
		"IPFSProxy not support rate config.")

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:SetRate finish.")
	return nil
}

func (o *IPFSProxy) Upload(
	ctx context.Context,
	userId string,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:Upload start.",
		zap.String("userId", userId),
		zap.String("sourcePath", sourcePath),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure))

	getTaskReq := new(GetTaskReq)
	getTaskReq.UserId = userId
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			zap.Error(err))
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		ErrorLogger.WithContext(ctx).Error(
			"task not exist.",
			zap.Int32("taskId", taskId))
		return errors.New("task not exist")
	}
	if TaskTypeUpload == getTaskResp.Data.List[0].Task.Type ||
		TaskTypeLoad == getTaskResp.Data.List[0].Task.Type {
		err = o.uploadObject(
			ctx,
			userId,
			sourcePath,
			getTaskResp.Data.List[0].Task)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"IPFSProxy:uploadObject failed.",
				zap.Error(err))
			return err
		}
	} else if TaskTypeUploadFile == getTaskResp.Data.List[0].Task.Type {
		err = o.uploadFile(
			ctx,
			sourcePath,
			getTaskResp.Data.List[0].Task)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"IPFSProxy:uploadFile failed.",
				zap.Error(err))
			return err
		}
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"task type invalid.",
			zap.Int32("taskId", taskId))
		return errors.New("task type invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:Upload finish.")
	return err
}

func (o *IPFSProxy) uploadObject(
	ctx context.Context,
	userId string,
	sourcePath string,
	task *TaskData) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:uploadObject start.",
		zap.String("userId", userId),
		zap.String("sourcePath", sourcePath))

	var objUuid, nodeName string
	if TaskTypeUpload == task.Type {
		uploadObjectTaskParams := new(UploadObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), uploadObjectTaskParams)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"UploadObjectTaskParams Unmarshal failed.",
				zap.String("params", task.Params),
				zap.Error(err))
			return err
		}
		objUuid = uploadObjectTaskParams.Uuid
		nodeName = uploadObjectTaskParams.NodeName
	} else if TaskTypeLoad == task.Type {
		loadObjectTaskParams := new(LoadObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), loadObjectTaskParams)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"LoadObjectTaskParams Unmarshal failed.",
				zap.String("params", task.Params),
				zap.Error(err))
			return err
		}
		objUuid = loadObjectTaskParams.Request.ObjUuid
		nodeName = loadObjectTaskParams.Request.TargetNodeName
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"task type invalid.",
			zap.Int32("taskId", task.Id),
			zap.Int32("taskType", task.Type))
		return errors.New("task type invalid")
	}

	getIpfsTokenReq := new(GetIpfsTokenReq)
	getIpfsTokenReq.NodeName = nodeName

	err, getIpfsTokenResp := UClient.GetIpfsToken(ctx, getIpfsTokenReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"GetIpfsToken failed.",
			zap.Error(err))
		return err
	}
	ipfsClient := ipfs_api.NewClient(
		getIpfsTokenResp.Url,
		getIpfsTokenResp.Token)

	stat, err := os.Stat(sourcePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}

	ch := make(chan XIpfsUpload)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				ErrorLogger.WithContext(ctx).Error(
					"Recover info.",
					zap.Any("message", e),
					zap.Stack("stacktrace"))
			}
		}()
		if stat.IsDir() {
			InfoLogger.WithContext(ctx).Debug(
				"IPFSProxy:AddDir start.",
				zap.String("sourcePath", sourcePath))
			cid, _err := ipfsClient.AddDir(context.Background(), sourcePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"IPFSProxy:AddDir failed.",
					zap.String("sourcePath", sourcePath),
					zap.Error(_err))
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			InfoLogger.WithContext(ctx).Debug(
				"IPFSProxy:AddDir finish.",
				zap.String("sourcePath", sourcePath))
			ch <- XIpfsUpload{
				CId:    cid,
				Result: ChanResultSuccess}
		} else {
			InfoLogger.WithContext(ctx).Debug(
				"IPFSProxy:Add start.",
				zap.String("sourcePath", sourcePath))
			cid, _err := ipfsClient.Add(context.Background(), sourcePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"IPFSProxy:Add failed.",
					zap.String("sourcePath", sourcePath),
					zap.Error(_err))
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			InfoLogger.WithContext(ctx).Debug(
				"IPFSProxy:Add finish.",
				zap.String("sourcePath", sourcePath))
			ch <- XIpfsUpload{
				CId:    cid,
				Result: ChanResultSuccess}
		}
	}()
	var cid string
	for {
		uploadResult, ok := <-ch
		if !ok {
			break
		}
		if ChanResultFailed == uploadResult.Result {
			ErrorLogger.WithContext(ctx).Error(
				"IPFSProxy:uploadObject failed.",
				zap.String("sourcePath", sourcePath))
			return errors.New("IPFSProxy:Upload failed")
		}
		cid = uploadResult.CId
		close(ch)
	}
	var location string
	if stat.IsDir() {
		location = cid + "/" + filepath.Base(sourcePath) + "/"
	} else {
		location = cid + "/" + filepath.Base(sourcePath)
	}
	putObjectDeploymentReq := new(PutObjectDeploymentReq)
	putObjectDeploymentReq.UserId = userId
	putObjectDeploymentReq.ObjUuid = objUuid
	putObjectDeploymentReq.NodeName = nodeName
	putObjectDeploymentReq.Location = &location

	err, _ = UClient.PutObjectDeployment(ctx, putObjectDeploymentReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"PutObjectDeployment failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:uploadObject finish.")
	return nil
}

func (o *IPFSProxy) uploadFile(
	ctx context.Context,
	sourcePath string,
	task *TaskData) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:uploadFile start.",
		zap.String("sourcePath", sourcePath))

	taskParams := new(UploadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), taskParams)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UploadFileTaskParams Unmarshal failed.",
			zap.String("params", task.Params),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:uploadFile finish.")
	return nil
}

func (o *IPFSProxy) Download(
	ctx context.Context,
	userId string,
	targetPath string,
	taskId int32,
	bucketName string) (err error) {

	if '/' != targetPath[len(targetPath)-1] {
		targetPath = targetPath + "/"
	}

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:Download start.",
		zap.String("userId", userId),
		zap.String("targetPath", targetPath),
		zap.Int32("taskId", taskId),
		zap.String("bucketName", bucketName))

	err = os.MkdirAll(targetPath, os.ModePerm)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.MkdirAll failed.",
			zap.Error(err))
		return err
	}

	getTaskReq := new(GetTaskReq)
	getTaskReq.UserId = userId
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			zap.Error(err))
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		ErrorLogger.WithContext(ctx).Error(
			"task invalid.",
			zap.Int32("taskId", taskId))
		return errors.New("task invalid")
	}
	var nodeName, hash, prePath, postPath string
	if TaskTypeDownload == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(DownloadObjectTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"DownloadObjectTaskParams Unmarshal failed.",
				zap.String("params", getTaskResp.Data.List[0].Task.Params),
				zap.Error(err))
			return err
		}
		nodeName = taskParams.NodeName
		hash = (strings.Split(taskParams.Location, "/"))[0]
		prePath = targetPath + hash
		postPath = targetPath + taskParams.Request.ObjUuid
	} else if TaskTypeDownloadFile == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(DownloadFileTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"DownloadFileTaskParams Unmarshal failed.",
				zap.String("params", getTaskResp.Data.List[0].Task.Params),
				zap.Error(err))
			return err
		}
		nodeName = taskParams.NodeName
		hash = (strings.Split(taskParams.Location, "/"))[0] + "/" +
			taskParams.Request.Source
		prePath = targetPath + hash
		postPath = targetPath + taskParams.Request.ObjUuid
	} else if TaskTypeLoad == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(LoadObjectTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"LoadObjectTaskParams Unmarshal failed.",
				zap.String("params", getTaskResp.Data.List[0].Task.Params),
				zap.Error(err))
			return err
		}
		nodeName = taskParams.SourceNodeName
		hash = (strings.Split(taskParams.SourceLocation, "/"))[0]
		prePath = targetPath + hash
		postPath = targetPath + taskParams.Request.ObjUuid
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"task type invalid.",
			zap.Int32("taskId", taskId))
		return errors.New("task type invalid")
	}

	getIpfsTokenReq := new(GetIpfsTokenReq)
	getIpfsTokenReq.NodeName = nodeName

	err, getIpfsTokenResp := UClient.GetIpfsToken(ctx, getIpfsTokenReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.GetIpfsToken failed.",
			zap.Error(err))
		return err
	}
	ipfsClient := ipfs_api.NewClient(
		getIpfsTokenResp.Url,
		getIpfsTokenResp.Token)
	ch := make(chan XIpfsDownload)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				ErrorLogger.WithContext(ctx).Error(
					"Recover info.",
					zap.Any("message", e),
					zap.Stack("stacktrace"))
			}
		}()
		InfoLogger.WithContext(ctx).Debug(
			"IPFSProxy:Get start.",
			zap.String("hash", hash),
			zap.String("targetPath", targetPath))
		_err := ipfsClient.Get(hash, targetPath)
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"IPFSProxy:Get failed.",
				zap.String("hash", hash),
				zap.String("targetPath", targetPath),
				zap.Error(_err))
			ch <- XIpfsDownload{
				Result: ChanResultFailed}
		}
		InfoLogger.WithContext(ctx).Debug(
			"IPFSProxy:Get finish.",
			zap.String("hash", hash),
			zap.String("targetPath", targetPath))
		ch <- XIpfsDownload{
			Result: ChanResultSuccess}
	}()

	for {
		downloadResult, ok := <-ch
		if !ok {
			break
		}
		if ChanResultFailed == downloadResult.Result {
			ErrorLogger.WithContext(ctx).Error(
				"IPFSProxy:Download failed.",
				zap.String("targetPath", targetPath),
				zap.Int32("taskId", taskId))
			return errors.New("IPFSProxy:Download failed")
		}
		close(ch)
	}
	err = os.Rename(prePath, postPath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Rename failed.",
			zap.String("prePath", prePath),
			zap.String("postPath", postPath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"IPFSProxy:Download finish.")
	return nil
}
