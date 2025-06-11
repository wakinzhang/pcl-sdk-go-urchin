package adaptee

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/urchinfs/go-urchin2-sdk/ipfs_api"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"os"
	"path/filepath"
	"strings"
)

type IPFSProxy struct {
}

func (o *IPFSProxy) Upload(
	ctx context.Context,
	userId string,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"IPFSProxy:Upload start.",
		" userId: ", userId,
		" sourcePath: ", sourcePath,
		" taskId: ", taskId,
		" needPure: ", needPure)

	getTaskReq := new(GetTaskReq)
	getTaskReq.UserId = userId
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			" err: ", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		Logger.WithContext(ctx).Error(
			"task not exist.",
			" taskId: ", taskId)
		return errors.New("task not exist")
	}
	if TaskTypeUpload == getTaskResp.Data.List[0].Task.Type ||
		TaskTypeMigrate == getTaskResp.Data.List[0].Task.Type {
		err = o.uploadObject(
			ctx,
			userId,
			sourcePath,
			getTaskResp.Data.List[0].Task)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"IPFSProxy:uploadObject failed.",
				" err: ", err)
			return err
		}
	} else if TaskTypeUploadFile == getTaskResp.Data.List[0].Task.Type {
		err = o.uploadFile(
			ctx,
			sourcePath,
			getTaskResp.Data.List[0].Task)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"IPFSProxy:uploadFile failed.",
				" err: ", err)
			return err
		}
	} else {
		Logger.WithContext(ctx).Error(
			"task type invalid.",
			" taskId: ", taskId)
		return errors.New("task type invalid")
	}

	Logger.WithContext(ctx).Debug(
		"IPFSProxy:Upload finish.")
	return err
}

func (o *IPFSProxy) uploadObject(
	ctx context.Context,
	userId string,
	sourcePath string,
	task *TaskData) (err error) {

	Logger.WithContext(ctx).Debug(
		"IPFSProxy:uploadObject start.",
		" userId: ", userId,
		" sourcePath: ", sourcePath)

	var objUuid, nodeName string
	if TaskTypeUpload == task.Type {
		uploadObjectTaskParams := new(UploadObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), uploadObjectTaskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"UploadObjectTaskParams Unmarshal failed.",
				" params: ", task.Params,
				" err: ", err)
			return err
		}
		objUuid = uploadObjectTaskParams.Uuid
		nodeName = uploadObjectTaskParams.NodeName
	} else if TaskTypeMigrate == task.Type {
		loadObjectTaskParams := new(LoadObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), loadObjectTaskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"LoadObjectTaskParams Unmarshal failed.",
				" params: ", task.Params,
				" err: ", err)
			return err
		}
		objUuid = loadObjectTaskParams.Request.ObjUuid
		nodeName = loadObjectTaskParams.Request.TargetNodeName
	} else {
		Logger.WithContext(ctx).Error(
			"task type invalid.",
			" taskId: ", task.Id,
			" taskType: ", task.Type)
		return errors.New("task type invalid")
	}

	getIpfsTokenReq := new(GetIpfsTokenReq)
	getIpfsTokenReq.NodeName = nodeName

	err, getIpfsTokenResp := UClient.GetIpfsToken(ctx, getIpfsTokenReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"GetIpfsToken failed.",
			" err: ", err)
		return err
	}
	ipfsClient := ipfs_api.NewClient(
		getIpfsTokenResp.Url,
		getIpfsTokenResp.Token)

	stat, err := os.Stat(sourcePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}

	ch := make(chan XIpfsUpload)
	go func() {
		if stat.IsDir() {
			Logger.WithContext(ctx).Debug(
				"IPFSProxy:AddDir start.",
				" sourcePath: ", sourcePath)
			cid, _err := ipfsClient.AddDir(context.Background(), sourcePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"IPFSProxy:AddDir failed.",
					" sourcePath: ", sourcePath,
					" err: ", _err)
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			Logger.WithContext(ctx).Debug(
				"IPFSProxy:AddDir finish.",
				" sourcePath: ", sourcePath)
			ch <- XIpfsUpload{
				CId:    cid,
				Result: ChanResultSuccess}
		} else {
			Logger.WithContext(ctx).Debug(
				"IPFSProxy:Add start.",
				" sourcePath: ", sourcePath)
			cid, _err := ipfsClient.Add(context.Background(), sourcePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"IPFSProxy:Add failed.",
					" sourcePath: ", sourcePath,
					" err: ", _err)
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			Logger.WithContext(ctx).Debug(
				"IPFSProxy:Add finish.",
				" sourcePath: ", sourcePath)
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
			Logger.WithContext(ctx).Error(
				"IPFSProxy:uploadObject failed.",
				" sourcePath: ", sourcePath)
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
		Logger.WithContext(ctx).Error(
			"PutObjectDeployment failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"IPFSProxy:uploadObject finish.")
	return nil
}

func (o *IPFSProxy) uploadFile(
	ctx context.Context,
	sourcePath string,
	task *TaskData) (err error) {

	Logger.WithContext(ctx).Debug(
		"IPFSProxy:uploadFile start.",
		" sourcePath: ", sourcePath)

	taskParams := new(UploadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), taskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UploadFileTaskParams Unmarshal failed.",
			" param: ", task.Params,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"IPFSProxy:uploadFile finish.")
	return nil
}

func (o *IPFSProxy) Download(
	ctx context.Context,
	userId string,
	targetPath string,
	taskId int32,
	bucketName string) (err error) {

	Logger.WithContext(ctx).Debug(
		"IPFSProxy:Download start.",
		" userId: ", userId,
		" targetPath: ", targetPath,
		" taskId: ", taskId,
		" bucketName: ", bucketName)

	err = os.MkdirAll(targetPath, os.ModePerm)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.MkdirAll failed.",
			" err: ", err)
		return err
	}

	getTaskReq := new(GetTaskReq)
	getTaskReq.UserId = userId
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			" err: ", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		Logger.WithContext(ctx).Error(
			"task invalid. taskId: ", taskId)
		return errors.New("task invalid")
	}
	var nodeName, hash, prePath, postPath string
	if TaskTypeDownload == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(DownloadObjectTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"DownloadObjectTaskParams Unmarshal failed.",
				" params: ", getTaskResp.Data.List[0].Task.Params,
				" err: ", err)
			return err
		}
		nodeName = taskParams.NodeName
		hash = (strings.Split(taskParams.Location, "/"))[0]
		prePath = targetPath + "/" + hash
		postPath = targetPath + "/" + taskParams.Request.ObjUuid
	} else if TaskTypeDownloadFile == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(DownloadFileTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"DownloadFileTaskParams Unmarshal failed.",
				" params: ", getTaskResp.Data.List[0].Task.Params,
				" err: ", err)
			return err
		}
		nodeName = taskParams.NodeName
		hash = (strings.Split(taskParams.Location, "/"))[0] + "/" +
			taskParams.Request.Source
		prePath = targetPath + "/" + hash
		postPath = targetPath + "/" + taskParams.Request.ObjUuid
	} else if TaskTypeMigrate == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(LoadObjectTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"LoadObjectTaskParams Unmarshal failed.",
				" params: ", getTaskResp.Data.List[0].Task.Params,
				" err: ", err)
			return err
		}
		nodeName = taskParams.SourceNodeName
		hash = (strings.Split(taskParams.SourceLocation, "/"))[0]
		prePath = targetPath + "/" + hash
		postPath = targetPath + "/" + taskParams.Request.ObjUuid
	} else {
		Logger.WithContext(ctx).Error(
			"task type invalid. taskId: ", taskId)
		return errors.New("task type invalid")
	}

	getIpfsTokenReq := new(GetIpfsTokenReq)
	getIpfsTokenReq.NodeName = nodeName

	err, getIpfsTokenResp := UClient.GetIpfsToken(ctx, getIpfsTokenReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.GetIpfsToken failed.",
			" err: ", err)
		return err
	}
	ipfsClient := ipfs_api.NewClient(
		getIpfsTokenResp.Url,
		getIpfsTokenResp.Token)
	ch := make(chan XIpfsDownload)
	go func() {
		Logger.WithContext(ctx).Debug(
			"IPFSProxy:Get start.",
			" hash: ", hash,
			" targetPath: ", targetPath)
		_err := ipfsClient.Get(hash, targetPath)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"IPFSProxy:Get failed.",
				" hash: ", hash,
				" targetPath: ", targetPath,
				" err: ", _err)
			ch <- XIpfsDownload{
				Result: ChanResultFailed}
		}
		Logger.WithContext(ctx).Debug(
			"IPFSProxy:Get finish.",
			" hash: ", hash,
			" targetPath: ", targetPath)
		ch <- XIpfsDownload{
			Result: ChanResultSuccess}
	}()

	for {
		downloadResult, ok := <-ch
		if !ok {
			break
		}
		if ChanResultFailed == downloadResult.Result {
			Logger.WithContext(ctx).Error(
				"IPFSProxy:Download failed.",
				" targetPath: ", targetPath,
				" taskId: ", taskId)
			return errors.New("IPFSProxy:Download failed")
		}
		close(ch)
	}
	err = os.Rename(prePath, postPath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Rename failed.",
			" prePath: ", prePath,
			" postPath: ", postPath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"IPFSProxy:Download finish.")
	return nil
}
