package adaptee

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/urchinfs/go-urchin2-sdk/ipfs_api"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
	"os"
	"path/filepath"
	"strings"
)

type IPFS struct {
}

func (o *IPFS) Upload(
	urchinServiceAddr, sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"IPFS:Upload start."+
			" urchinServiceAddr: %s, sourcePath: %s, taskId: %d, needPure: %t",
		urchinServiceAddr, sourcePath, taskId, needPure)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := urchinService.GetTask(getTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. err: %v", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		obs.DoLog(obs.LEVEL_ERROR, "task not exist. taskId: %d", taskId)
		return errors.New("task not exist")
	}
	if TaskTypeUpload == getTaskResp.Data.List[0].Task.Type ||
		TaskTypeMigrate == getTaskResp.Data.List[0].Task.Type {
		err = o.uploadObject(
			urchinServiceAddr,
			sourcePath,
			getTaskResp.Data.List[0].Task)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "IPFS:uploadObject failed. err: %v", err)
			return err
		}
	} else if TaskTypeUploadFile == getTaskResp.Data.List[0].Task.Type {
		err = o.uploadFile(
			urchinServiceAddr,
			sourcePath,
			getTaskResp.Data.List[0].Task)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "IPFS:uploadFile failed. err: %v", err)
			return err
		}
	} else {
		obs.DoLog(obs.LEVEL_ERROR, "task type invalid. taskId: %d", taskId)
		return errors.New("task type invalid")
	}

	obs.DoLog(obs.LEVEL_DEBUG, "IPFS:Upload finish.")
	return err
}

func (o *IPFS) uploadObject(
	urchinServiceAddr, sourcePath string, task *Task) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"IPFS:uploadObject start. urchinServiceAddr: %s, sourcePath: %s",
		urchinServiceAddr, sourcePath)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	var objUuid, nodeName string
	if TaskTypeUpload == task.Type {
		uploadObjectTaskParams := new(UploadObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), uploadObjectTaskParams)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"UploadObjectTaskParams Unmarshal failed. params: %s, error: %v",
				task.Params, err)
			return err
		}
		objUuid = uploadObjectTaskParams.Uuid
		nodeName = uploadObjectTaskParams.NodeName
	} else if TaskTypeMigrate == task.Type {
		migrateObjectTaskParams := new(MigrateObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), migrateObjectTaskParams)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"MigrateObjectTaskParams Unmarshal failed. params: %s, error: %v",
				task.Params, err)
			return err
		}
		objUuid = migrateObjectTaskParams.Request.ObjUuid
		nodeName = migrateObjectTaskParams.Request.TargetNodeName
	} else {
		obs.DoLog(obs.LEVEL_ERROR,
			"task type invalid. taskId: %d, taskType: %d", task.Id, task.Type)
		return errors.New("task type invalid")
	}

	getIpfsTokenReq := new(GetIpfsTokenReq)
	getIpfsTokenReq.NodeName = nodeName

	err, getIpfsTokenResp := urchinService.GetIpfsToken(getIpfsTokenReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetIpfsToken failed. err: %v", err)
		return err
	}
	ipfsClient := ipfs_api.NewClient(getIpfsTokenResp.Url, getIpfsTokenResp.Token)

	stat, err := os.Stat(sourcePath)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. sourcePath: %s, err: %v",
			sourcePath, err)
		return err
	}

	ch := make(chan XIpfsUpload)
	go func() {
		if stat.IsDir() {
			obs.DoLog(obs.LEVEL_DEBUG,
				"IPFS:AddDir start. sourcePath: %s", sourcePath)
			cid, _err := ipfsClient.AddDir(context.Background(), sourcePath)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"IPFS.AddDir failed. sourcePath: %s, err: %v",
					sourcePath, _err)
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			obs.DoLog(obs.LEVEL_DEBUG,
				"IPFS:AddDir finish. sourcePath: %s", sourcePath)
			ch <- XIpfsUpload{
				CId:    cid,
				Result: ChanResultSuccess}
		} else {
			obs.DoLog(obs.LEVEL_DEBUG,
				"IPFS:Add start. sourcePath: %s", sourcePath)
			cid, _err := ipfsClient.Add(context.Background(), sourcePath)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR, "IPFS.Add failed."+
					" urchinServiceAddr: %s, sourcePath: %s, err: %v",
					urchinServiceAddr, sourcePath, _err)
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			obs.DoLog(obs.LEVEL_DEBUG,
				"IPFS:Add finish. sourcePath: %s", sourcePath)
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
			obs.DoLog(obs.LEVEL_ERROR, "IPFS:uploadObject failed."+
				" urchinServiceAddr: %s, sourcePath: %s", urchinServiceAddr, sourcePath)
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

	err, _ = urchinService.PutObjectDeployment(putObjectDeploymentReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "PutObjectDeployment failed. err: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "IPFS:uploadObject finish.")
	return nil
}

func (o *IPFS) uploadFile(
	urchinServiceAddr, sourcePath string, task *Task) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"IPFS:uploadFile start. urchinServiceAddr: %s, sourcePath: %s",
		urchinServiceAddr, sourcePath)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	taskParams := new(UploadFileTaskParams)
	err = json.Unmarshal([]byte(task.Params), taskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"UploadFileTaskParams Unmarshal failed. params: %s, error: %v",
			task.Params, err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "IPFS:uploadFile finish.")
	return nil
}

func (o *IPFS) Download(
	urchinServiceAddr, targetPath string,
	taskId int32,
	bucketName string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "IPFS:Download start."+
		" urchinServiceAddr: %s, targetPath: %s, taskId: %d, bucketName: %s",
		urchinServiceAddr, targetPath, taskId, bucketName)

	err = os.MkdirAll(targetPath, os.ModePerm)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Failed to make dir. error: %v", err)
		return err
	}

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := urchinService.GetTask(getTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. err: %v", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		obs.DoLog(obs.LEVEL_ERROR, "task invalid. taskId: %d", taskId)
		return errors.New("task invalid")
	}
	var nodeName, hash, prePath, postPath string
	if TaskTypeDownload == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(DownloadObjectTaskParams)
		err = json.Unmarshal([]byte(getTaskResp.Data.List[0].Task.Params), taskParams)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"DownloadObjectTaskParams Unmarshal failed."+
					" params: %s, error: %v",
				getTaskResp.Data.List[0].Task.Params, err)
			return err
		}
		nodeName = taskParams.NodeName
		hash = (strings.Split(taskParams.Location, "/"))[0]
		prePath = targetPath + "/" + hash
		postPath = targetPath + "/" + taskParams.Request.ObjUuid
	} else if TaskTypeDownloadFile == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(DownloadFileTaskParams)
		err = json.Unmarshal([]byte(getTaskResp.Data.List[0].Task.Params), taskParams)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"DownloadFileTaskParams Unmarshal failed."+
					" params: %s, error: %v",
				getTaskResp.Data.List[0].Task.Params, err)
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
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"MigrateObjectTaskParams Unmarshal failed."+
					" params: %s, error: %v",
				getTaskResp.Data.List[0].Task.Params, err)
			return err
		}
		nodeName = taskParams.SourceNodeName
		hash = (strings.Split(taskParams.Location, "/"))[0]
		prePath = targetPath + "/" + hash
		postPath = targetPath + "/" + taskParams.Request.ObjUuid
	} else {
		obs.DoLog(obs.LEVEL_ERROR, "task type invalid. taskId: %d", taskId)
		return errors.New("task type invalid")
	}

	getIpfsTokenReq := new(GetIpfsTokenReq)
	getIpfsTokenReq.NodeName = nodeName

	err, getIpfsTokenResp := urchinService.GetIpfsToken(getIpfsTokenReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetIpfsToken failed. err: %v", err)
		return err
	}
	ipfsClient := ipfs_api.NewClient(getIpfsTokenResp.Url, getIpfsTokenResp.Token)
	ch := make(chan XIpfsDownload)
	go func() {
		obs.DoLog(obs.LEVEL_DEBUG,
			"IPFS:Get start. hash: %s, targetPath: %s", hash, targetPath)
		_err := ipfsClient.Get(hash, targetPath)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"IPFS.Get failed. hash: %s, targetPath: %s, err: %v",
				hash, targetPath, _err)
			ch <- XIpfsDownload{
				Result: ChanResultFailed}
		}
		obs.DoLog(obs.LEVEL_DEBUG,
			"IPFS:Get finish. hash: %s, targetPath: %s", hash, targetPath)
		ch <- XIpfsDownload{
			Result: ChanResultSuccess}
	}()

	for {
		downloadResult, ok := <-ch
		if !ok {
			break
		}
		if ChanResultFailed == downloadResult.Result {
			obs.DoLog(obs.LEVEL_ERROR, "IPFS:Download failed."+
				" urchinServiceAddr: %s, targetPath: %s, taskId: %d",
				urchinServiceAddr, targetPath, taskId)
			return errors.New("IPFS:Download failed")
		}
		close(ch)
	}
	err = os.Rename(prePath, postPath)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Rename failed. prePath: %s, postPath: %s, error: %v",
			prePath, postPath, err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "IPFS:Download finish.")
	return nil
}
