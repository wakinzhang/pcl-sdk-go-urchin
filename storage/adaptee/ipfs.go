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
)

type IPFS struct {
}

func (o *IPFS) Upload(
	urchinServiceAddr, sourcePath string, taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"IPFS:Upload start. urchinServiceAddr: %s, sourcePath: %s, taskId: %d",
		urchinServiceAddr, sourcePath, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := urchinService.GetTask(
		ConfigDefaultUrchinServiceGetTaskInterface,
		getTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. err: %v", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		obs.DoLog(obs.LEVEL_ERROR, "task invalid. taskId: %d", taskId)
		return errors.New("task invalid")
	}
	taskParams := new(UploadObjectTaskParams)
	err = json.Unmarshal([]byte(getTaskResp.Data.List[0].Task.Params), taskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"UploadObjectTaskParams Unmarshal failed. params: %s, error:",
			getTaskResp.Data.List[0].Task.Params, err)
		return err
	}

	getIpfsTokenReq := new(GetIpfsTokenReq)
	getIpfsTokenReq.NodeName = taskParams.NodeName

	err, getIpfsTokenResp := urchinService.GetIpfsToken(
		ConfigDefaultUrchinServiceGetIpfsTokenInterface,
		getIpfsTokenReq)
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
			cid, _err := ipfsClient.AddDir(context.Background(), sourcePath)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"IPFS.AddDir failed. sourcePath: %s, taskId: %d, err: %v",
					sourcePath, taskId, _err)
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			ch <- XIpfsUpload{
				CId:    cid,
				Result: ChanResultSuccess}
		} else {
			cid, _err := ipfsClient.Add(context.Background(), sourcePath)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"IPFS.Add failed. urchinServiceAddr: %s, sourcePath: %s,"+
						" taskId: %d, err: %v",
					urchinServiceAddr, sourcePath, taskId, _err)
				ch <- XIpfsUpload{
					Result: ChanResultFailed}
			}
			ch <- XIpfsUpload{
				CId:    cid,
				Result: ChanResultSuccess}
		}
	}()
	var location string
	for {
		uploadResult, ok := <-ch
		if !ok {
			break
		}
		if ChanResultFailed == uploadResult.Result {
			obs.DoLog(obs.LEVEL_ERROR, "IPFS:Upload failed."+
				" urchinServiceAddr: %s, sourcePath: %s, taskId: %d",
				urchinServiceAddr, sourcePath, taskId)
			return errors.New("IPFS:Upload failed")
		}
		location = uploadResult.CId
		close(ch)
	}
	putObjectDeploymentReq := new(PutObjectDeploymentReq)
	putObjectDeploymentReq.ObjUuid = taskParams.Uuid
	putObjectDeploymentReq.NodeName = taskParams.NodeName
	putObjectDeploymentReq.Location = &location

	err, _ = urchinService.PutObjectDeployment(
		ConfigDefaultUrchinServicePutObjectDeploymentInterface,
		putObjectDeploymentReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "PutObjectDeployment failed. err: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "IPFS:Upload finish.")
	return nil
}

func (o *IPFS) Download(
	urchinServiceAddr, targetPath string,
	taskId int32,
	bucketName string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "IPFS:Download start."+
		" urchinServiceAddr: %s, targetPath: %s, taskId: %d, bucketName: %s",
		urchinServiceAddr, targetPath, taskId, bucketName)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := urchinService.GetTask(
		ConfigDefaultUrchinServiceGetTaskInterface,
		getTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. err: %v", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		obs.DoLog(obs.LEVEL_ERROR, "task invalid. taskId: %d", taskId)
		return errors.New("task invalid")
	}
	taskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(getTaskResp.Data.List[0].Task.Params), taskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "DownloadObjectTaskParams Unmarshal failed."+
			" params: %s, error:", getTaskResp.Data.List[0].Task.Params, err)
		return err
	}

	getIpfsTokenReq := new(GetIpfsTokenReq)
	getIpfsTokenReq.NodeName = taskParams.NodeName

	err, getIpfsTokenResp := urchinService.GetIpfsToken(
		ConfigDefaultUrchinServiceGetIpfsTokenInterface,
		getIpfsTokenReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetIpfsToken failed. err: %v", err)
		return err
	}
	ipfsClient := ipfs_api.NewClient(getIpfsTokenResp.Url, getIpfsTokenResp.Token)
	ch := make(chan XIpfsDownload)
	go func() {
		_err := ipfsClient.Get(taskParams.Location, targetPath)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"IPFS.Get failed. CId: %s, targetPath: %s, err: %v",
				taskParams.Location, targetPath, _err)
			ch <- XIpfsDownload{
				Result: ChanResultFailed}
		}
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
			return errors.New("IPFS:Upload failed")
		}
		close(ch)
	}

	obs.DoLog(obs.LEVEL_DEBUG, "IPFS:Download finish.")
	return nil
}
