package adaptee

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/urchinfs/go-urchin2-sdk/ipfs_api"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	"os"
	"path/filepath"
	"strings"
)

type IPFS struct {
}

func (o *IPFS) Upload(
	ctx context.Context,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"IPFS:Upload start.",
		" sourcePath: ", sourcePath,
		" taskId: ", taskId,
		" needPure: ", needPure)

	getTaskReq := new(GetTaskReq)
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
			"task not exist. taskId: ", taskId)
		return errors.New("task not exist")
	}
	if TaskTypeUpload == getTaskResp.Data.List[0].Task.Type ||
		TaskTypeMigrate == getTaskResp.Data.List[0].Task.Type {
		err = o.uploadObject(
			ctx,
			sourcePath,
			getTaskResp.Data.List[0].Task)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"IPFS:uploadObject failed.",
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
				"IPFS:uploadFile failed.",
				" err: ", err)
			return err
		}
	} else {
		Logger.WithContext(ctx).Error(
			"task type invalid. taskId: ", taskId)
		return errors.New("task type invalid")
	}

	Logger.WithContext(ctx).Debug(
		"IPFS:Upload finish.")
	return err
}

func (o *IPFS) uploadObject(
	ctx context.Context,
	sourcePath string,
	task *Task) (err error) {

	Logger.WithContext(ctx).Debug(
		"IPFS:uploadObject start.",
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
		migrateObjectTaskParams := new(MigrateObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), migrateObjectTaskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"MigrateObjectTaskParams Unmarshal failed.",
				" params: ", task.Params,
				" err: ", err)
			return err
		}
		objUuid = migrateObjectTaskParams.Request.ObjUuid
		nodeName = migrateObjectTaskParams.Request.TargetNodeName
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
	ipfsClient := ipfs_api.NewClient(getIpfsTokenResp.Url, getIpfsTokenResp.Token)

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
				"IPFS:AddDir start.",
				" sourcePath: ", sourcePath)
			cid, _err := ipfsClient.AddDir(context.Background(), sourcePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"IPFS:AddDir failed.",
					" sourcePath: ", sourcePath,
					" err: ", _err)
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			Logger.WithContext(ctx).Debug(
				"IPFS:AddDir finish.",
				" sourcePath: ", sourcePath)
			ch <- XIpfsUpload{
				CId:    cid,
				Result: ChanResultSuccess}
		} else {
			Logger.WithContext(ctx).Debug(
				"IPFS:Add start.",
				" sourcePath: ", sourcePath)
			cid, _err := ipfsClient.Add(context.Background(), sourcePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"IPFS:Add failed.",
					" sourcePath: ", sourcePath,
					" err: ", _err)
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			Logger.WithContext(ctx).Debug(
				"IPFS:Add finish.",
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
				"IPFS:uploadObject failed.",
				" sourcePath: ", sourcePath)
			return errors.New("IPFS:Upload failed")
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
		"IPFS:uploadObject finish.")
	return nil
}

func (o *IPFS) uploadFile(
	ctx context.Context,
	sourcePath string,
	task *Task) (err error) {

	Logger.WithContext(ctx).Debug(
		"IPFS:uploadFile start.",
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
		"IPFS:uploadFile finish.")
	return nil
}

func (o *IPFS) Download(
	ctx context.Context,
	targetPath string,
	taskId int32,
	bucketName string) (err error) {

	Logger.WithContext(ctx).Debug(
		"IPFS:Download start.",
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
		err = json.Unmarshal([]byte(getTaskResp.Data.List[0].Task.Params), taskParams)
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
		err = json.Unmarshal([]byte(getTaskResp.Data.List[0].Task.Params), taskParams)
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
		taskParams := new(MigrateObjectTaskParams)
		err = json.Unmarshal([]byte(getTaskResp.Data.List[0].Task.Params), taskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"MigrateObjectTaskParams Unmarshal failed.",
				" params: ", getTaskResp.Data.List[0].Task.Params,
				" err: ", err)
			return err
		}
		nodeName = taskParams.SourceNodeName
		hash = (strings.Split(taskParams.Location, "/"))[0]
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
	ipfsClient := ipfs_api.NewClient(getIpfsTokenResp.Url, getIpfsTokenResp.Token)
	ch := make(chan XIpfsDownload)
	go func() {
		Logger.WithContext(ctx).Debug(
			"IPFS:Get start.",
			" hash: ", hash,
			" targetPath: ", targetPath)
		_err := ipfsClient.Get(hash, targetPath)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"IPFS:Get failed.",
				" hash: ", hash,
				" targetPath: ", targetPath,
				" err: ", _err)
			ch <- XIpfsDownload{
				Result: ChanResultFailed}
		}
		Logger.WithContext(ctx).Debug(
			"IPFS:Get finish.",
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
				"IPFS:Download failed.",
				" targetPath: ", targetPath,
				" taskId: ", taskId)
			return errors.New("IPFS:Download failed")
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
		"IPFS:Download finish.")
	return nil
}
