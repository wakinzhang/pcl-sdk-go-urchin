package adaptee

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

type JCS struct {
	jcsClient *JCSClient
}

func (o *JCS) Init(
	ctx context.Context,
	reqTimeout,
	maxConnection int) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:Init start.",
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

	o.jcsClient = new(JCSClient)
	o.jcsClient.Init(ctx, reqTimeout, maxConnection)

	Logger.WithContext(ctx).Debug(
		"JCS:Init finish.")
	return nil
}

func (o *JCS) NewFolderWithSignedUrl(
	ctx context.Context,
	objectPath string,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:NewFolderWithSignedUrl start.",
		" objectPath: ", objectPath,
		" taskId: ", taskId)

	createJCSPreSignedObjectUploadReq :=
		new(CreateJCSPreSignedObjectUploadReq)
	createJCSPreSignedObjectUploadReq.TaskId = taskId
	createJCSPreSignedObjectUploadReq.Source = objectPath

	err, createJCSPreSignedObjectUploadResp :=
		UClient.CreateJCSPreSignedObjectUpload(
			ctx,
			createJCSPreSignedObjectUploadReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectUpload"+
				" failed.",
			" err: ", err)
		return err
	}

	err = o.jcsClient.UploadFileWithSignedUrl(
		ctx,
		createJCSPreSignedObjectUploadResp.SignedUrl,
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.UploadFileWithSignedUrl failed.",
			" signedUrl: ", createJCSPreSignedObjectUploadResp.SignedUrl,
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:NewFolderWithSignedUrl finish.")

	return err
}

func (o *JCS) UploadFileWithSignedUrl(
	ctx context.Context,
	sourceFile,
	objectPath string,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:UploadFileWithSignedUrl start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" taskId: ", taskId)

	createJCSPreSignedObjectUploadReq :=
		new(CreateJCSPreSignedObjectUploadReq)
	createJCSPreSignedObjectUploadReq.TaskId = taskId
	createJCSPreSignedObjectUploadReq.Source = objectPath

	err, createJCSPreSignedObjectUploadResp :=
		UClient.CreateJCSPreSignedObjectUpload(
			ctx,
			createJCSPreSignedObjectUploadReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectUpload"+
				" failed.",
			" err: ", err)
		return err
	}

	fd, err := os.Open(sourceFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Open failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" sourceFile: ", sourceFile,
				" err: ", errMsg)
		}
	}()

	err = o.jcsClient.UploadFileWithSignedUrl(
		ctx,
		createJCSPreSignedObjectUploadResp.SignedUrl,
		fd)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.UploadFileWithSignedUrl failed.",
			" sourceFile: ", sourceFile,
			" signedUrl: ", createJCSPreSignedObjectUploadResp.SignedUrl,
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:UploadFileWithSignedUrl finish.")

	return err
}

func (o *JCS) NewMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectPath string,
	taskId int32) (output *JCSNewMultiPartUploadResponse, err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:NewMultipartUploadWithSignedUrl start.",
		" objectPath: ", objectPath,
		" taskId: ", taskId)

	createJCSPreSignedObjectNewMultipartUploadReq :=
		new(CreateJCSPreSignedObjectNewMultipartUploadReq)
	createJCSPreSignedObjectNewMultipartUploadReq.TaskId = taskId
	createJCSPreSignedObjectNewMultipartUploadReq.Source = objectPath

	err, createJCSPreSignedObjectNewMultipartUploadResp :=
		UClient.CreateJCSPreSignedObjectNewMultipartUpload(
			ctx,
			createJCSPreSignedObjectNewMultipartUploadReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectNewMultipartUpload"+
				" failed.",
			" err: ", err)
		return output, err
	}

	// 初始化分段上传任务
	err, output = o.jcsClient.NewMultiPartUploadWithSignedUrl(
		ctx,
		createJCSPreSignedObjectNewMultipartUploadResp.SignedUrl)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.NewMultiPartUploadWithSignedUrl failed.",
			" signedUrl: ",
			createJCSPreSignedObjectNewMultipartUploadResp.SignedUrl,
			" err: ", err)
		return output, err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:NewMultipartUploadWithSignedUrl finish.")

	return output, err
}

func (o *JCS) CompleteMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectId int32,
	taskId int32,
	indexes []int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:CompleteMultipartUploadWithSignedUrl start.",
		" objectId: ", objectId,
		" taskId: ", taskId)

	// 合并段
	createJCSPreSignedObjectCompleteMultipartUploadReq :=
		new(CreateJCSPreSignedObjectCompleteMultipartUploadReq)
	createJCSPreSignedObjectCompleteMultipartUploadReq.ObjectId = objectId
	createJCSPreSignedObjectCompleteMultipartUploadReq.TaskId = taskId
	createJCSPreSignedObjectCompleteMultipartUploadReq.Indexes = indexes

	err, createCompleteMultipartUploadSignedUrlResp :=
		UClient.CreateJCSPreSignedObjectCompleteMultipartUpload(
			ctx,
			createJCSPreSignedObjectCompleteMultipartUploadReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient:CreateCompleteMultipartUploadSignedUrl"+
				" failed.",
			" err: ", err)
		return err
	}

	err, _ = o.jcsClient.CompleteMultiPartUploadWithSignedUrl(
		ctx,
		createCompleteMultipartUploadSignedUrlResp.SignedUrl)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.CompleteMultipartUploadWithSignedUrl failed.",
			" signedUrl: ",
			createCompleteMultipartUploadSignedUrlResp.SignedUrl,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCS.CompleteMultipartUploadWithSignedUrl finish.")
	return nil
}

func (o *JCS) ListObjectsWithSignedUrl(
	ctx context.Context,
	taskId int32,
	continuationToken string) (
	listObjectsData *JCSListData,
	err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:ListObjectsWithSignedUrl start.",
		" taskId: ", taskId,
		" continuationToken: ", continuationToken)

	createJCSPreSignedObjectListReq := new(CreateJCSPreSignedObjectListReq)
	createJCSPreSignedObjectListReq.TaskId = taskId
	if "" != continuationToken {
		createJCSPreSignedObjectListReq.ContinuationToken = &continuationToken
	}

	err, createListObjectsSignedUrlResp :=
		UClient.CreateJCSPreSignedObjectList(
			ctx,
			createJCSPreSignedObjectListReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectList failed.",
			" err: ", err)
		return listObjectsData, err
	}

	err, listObjectsResponse :=
		o.jcsClient.ListWithSignedUrl(
			ctx,
			createListObjectsSignedUrlResp.SignedUrl)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.ListWithSignedUrl failed.",
			" signedUrl: ", createListObjectsSignedUrlResp.SignedUrl,
			" err: ", err)
		return listObjectsData, err
	}
	listObjectsData = new(JCSListData)
	listObjectsData = listObjectsResponse.Data

	Logger.WithContext(ctx).Debug(
		"JCS:ListObjectsWithSignedUrl finish.")
	return listObjectsData, nil
}

func (o *JCS) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	Logger.WithContext(ctx).Debug(
		"JCS:loadCheckpointFile start.",
		" checkpointFile: ", checkpointFile)

	ret, err := os.ReadFile(checkpointFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.ReadFile failed.",
			" checkpointFile: ", checkpointFile,
			" err: ", err)
		return err
	}
	if len(ret) == 0 {
		Logger.WithContext(ctx).Debug(
			"checkpointFile empty.",
			" checkpointFile: ", checkpointFile)
		return nil
	}
	Logger.WithContext(ctx).Debug(
		"JCS:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *JCS) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *JCSDownloadCheckpoint) {

	Logger.WithContext(ctx).Debug(
		"JCS:sliceObject start.",
		" objectSize: ", objectSize,
		" partSize: ", partSize)

	cnt := objectSize / partSize
	if objectSize%partSize > 0 {
		cnt++
	}

	if cnt == 0 {
		downloadPart := JCSDownloadPartInfo{}
		downloadPart.PartNumber = 1
		dfc.DownloadParts = []JCSDownloadPartInfo{downloadPart}
	} else {
		downloadParts := make([]JCSDownloadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			downloadPart := JCSDownloadPartInfo{}
			downloadPart.PartNumber = i + 1
			downloadPart.Offset = i * partSize
			downloadPart.Length = partSize
			downloadParts = append(downloadParts, downloadPart)
		}
		dfc.DownloadParts = downloadParts
		if value := objectSize % partSize; value > 0 {
			dfc.DownloadParts[cnt-1].Length = dfc.ObjectInfo.Size - 1
		}
	}
	Logger.WithContext(ctx).Debug(
		"JCS:sliceObject finish.")
}

func (o *JCS) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	Logger.WithContext(ctx).Debug(
		"JCS:updateCheckpointFile start.",
		" checkpointFilePath: ", checkpointFilePath)

	result, err := xml.Marshal(fc)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"xml.Marshal failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return err
	}
	err = os.WriteFile(checkpointFilePath, result, 0640)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.WriteFile failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCS:updateCheckpointFile finish.")
	return err
}

func (o *JCS) Upload(
	ctx context.Context,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:Upload start.",
		" sourcePath: ", sourcePath,
		" taskId: ", taskId,
		" needPure: ", needPure)

	stat, err := os.Stat(sourcePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}
	var isDir = false
	if stat.IsDir() {
		isDir = true
		err = o.uploadFolder(ctx, sourcePath, taskId, needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS.uploadFolder failed.",
				" sourcePath: ", sourcePath,
				" taskId: ", taskId,
				" err: ", err)
			return err
		}
	} else {
		objectPath := filepath.Base(sourcePath)
		err = o.uploadFileResume(
			ctx,
			sourcePath,
			objectPath,
			taskId,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS.uploadFileResume failed.",
				" sourcePath: ", sourcePath,
				" objectPath: ", objectPath,
				" taskId: ", taskId,
				" err: ", err)
			return err
		}
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
			"task not exist.",
			" taskId: ", taskId)
		return errors.New("task not exist")
	}

	task := getTaskResp.Data.List[0].Task
	if TaskTypeMigrate == task.Type {
		migrateObjectTaskParams := new(MigrateObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), migrateObjectTaskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"MigrateObjectTaskParams Unmarshal failed.",
				" params: ", task.Params,
				" err: ", err)
			return err
		}
		objUuid := migrateObjectTaskParams.Request.ObjUuid
		nodeName := migrateObjectTaskParams.Request.TargetNodeName
		var location string
		if isDir {
			location = objUuid + "/" + filepath.Base(sourcePath) + "/"
		} else {
			location = objUuid + "/" + filepath.Base(sourcePath)
		}

		putObjectDeploymentReq := new(PutObjectDeploymentReq)
		putObjectDeploymentReq.ObjUuid = objUuid
		putObjectDeploymentReq.NodeName = nodeName
		putObjectDeploymentReq.Location = &location

		err, _ = UClient.PutObjectDeployment(ctx, putObjectDeploymentReq)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"UrchinClient.PutObjectDeployment failed.",
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"JCS:Upload finish.")
	return nil
}

func (o *JCS) uploadFolder(
	ctx context.Context,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFolder start.",
		" sourcePath: ", sourcePath,
		" taskId: ", taskId,
		" needPure: ", needPure)

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	uploadFolderRecord :=
		filepath.Dir(sourcePath) + "/" +
			filepath.Base(sourcePath) + ".upload_folder_record"

	if needPure {
		err = os.Remove(uploadFolderRecord)
		if nil != err {
			if !os.IsNotExist(err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" uploadFolderRecord: ", uploadFolderRecord,
					" err: ", err)
				return err
			}
		}
	} else {
		fileData, err := os.ReadFile(uploadFolderRecord)
		if nil == err {
			lines := strings.Split(string(fileData), "\n")
			for _, line := range lines {
				fileMap[strings.TrimSuffix(line, "\r")] = 0
			}
		} else if !os.IsNotExist(err) {
			Logger.WithContext(ctx).Error(
				"os.ReadFile failed.",
				" uploadFolderRecord: ", uploadFolderRecord,
				" err: ", err)
			return err
		}
	}

	objectPath := filepath.Base(sourcePath) + "/"
	err = o.NewFolderWithSignedUrl(ctx, objectPath, taskId)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:NewFolderWithSignedUrl failed.",
			" err: ", err)
		return err
	}

	pool, err := ants.NewPool(DefaultJCSUploadFileTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			" err: ", err)
		return err
	}
	defer pool.Release()

	var isAllSuccess = true
	var wg sync.WaitGroup
	err = filepath.Walk(
		sourcePath,
		func(filePath string, fileInfo os.FileInfo, err error) error {

			if nil != err {
				Logger.WithContext(ctx).Error(
					"filepath.Walk failed.",
					" sourcePath: ", sourcePath,
					" err: ", err)
				return err
			}
			wg.Add(1)
			err = pool.Submit(func() {
				defer func() {
					wg.Done()
					if err := recover(); nil != err {
						Logger.WithContext(ctx).Error(
							"JCS:uploadFileResume failed.",
							" err: ", err)
						isAllSuccess = false
					}
				}()
				relFilePath, err := filepath.Rel(sourcePath, filePath)
				if nil != err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"filepath.Rel failed.",
						" sourcePath: ", sourcePath,
						" filePath: ", filePath,
						" relFilePath: ", relFilePath,
						" err: ", err)
					return
				}
				objectKey := objectPath + relFilePath

				if _, exists := fileMap[objectKey]; exists {
					Logger.WithContext(ctx).Info(
						"already finish.",
						" objectKey: ", objectKey)
					return
				}
				if fileInfo.IsDir() {
					err = o.NewFolderWithSignedUrl(
						ctx,
						objectKey,
						taskId)
					if nil != err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"JCS:NewFolderWithSignedUrl failed.",
							" objectKey: ", objectKey,
							" err: ", err)
						return
					}
				} else {
					err = o.uploadFileResume(
						ctx,
						filePath,
						objectKey,
						taskId,
						needPure)
					if nil != err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"JCS:uploadFileResume failed.",
							" filePath: ", filePath,
							" objectKey: ", objectKey,
							" err: ", err)
						return
					}
				}
				fileMutex.Lock()
				defer fileMutex.Unlock()
				f, err := os.OpenFile(
					uploadFolderRecord,
					os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				if nil != err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"os.OpenFile failed.",
						" uploadFolderRecord: ", uploadFolderRecord,
						" err: ", err)
					return
				}
				defer func() {
					errMsg := f.Close()
					if errMsg != nil {
						Logger.WithContext(ctx).Warn(
							"close file failed.",
							" err: ", errMsg)
					}
				}()
				_, err = f.Write([]byte(objectKey + "\n"))
				if nil != err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"write file failed.",
						" uploadFolderRecord: ", uploadFolderRecord,
						" objectKey: ", objectKey,
						" err: ", err)
					return
				}
				return
			})
			if nil != err {
				Logger.WithContext(ctx).Error(
					"ants.Submit failed.",
					" err: ", err)
				return err
			}
			return nil
		})
	wg.Wait()

	if nil != err {
		Logger.WithContext(ctx).Error(
			"filepath.Walk failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}
	if !isAllSuccess {
		Logger.WithContext(ctx).Error(
			"JCS:uploadFolder not all success.",
			" sourcePath: ", sourcePath)
		return errors.New("uploadFolder not all success")
	} else {
		_err := os.Remove(uploadFolderRecord)
		if nil != _err {
			if !os.IsNotExist(_err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" uploadFolderRecord: ", uploadFolderRecord,
					" err: ", _err)
			}
		}
	}

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFolder finish.")
	return nil
}

func (o *JCS) uploadFile(
	ctx context.Context,
	sourceFile,
	objectPath string,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFile start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" taskId: ", taskId,
		" needPure: ", needPure)

	sourceFileStat, err := os.Stat(sourceFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	if DefaultJCSUploadMultiSize < sourceFileStat.Size() {
		err = o.uploadFileResume(
			ctx,
			sourceFile,
			objectPath,
			taskId,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:uploadFileResume failed.",
				" sourceFile: ", sourceFile,
				" objectPath: ", objectPath,
				" taskId: ", taskId,
				" needPure: ", needPure,
				" err: ", err)
			return err
		}
	} else {
		err = o.UploadFileWithSignedUrl(
			ctx,
			sourceFile,
			objectPath,
			taskId)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:UploadFileWithSignedUrl failed.",
				" sourceFile: ", sourceFile,
				" objectPath: ", objectPath,
				" taskId: ", taskId,
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFile finish.")
	return err
}

func (o *JCS) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectPath string,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFileResume start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" taskId: ", taskId,
		" needPure: ", needPure)

	uploadFileInput := new(JCSUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + ".upload_file_record"
	uploadFileInput.TaskNum = DefaultJCSUploadMultiTaskNum
	uploadFileInput.PartSize = DefaultPartSize

	if needPure {
		err = os.Remove(uploadFileInput.CheckpointFile)
		if nil != err {
			if !os.IsNotExist(err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" CheckpointFile: ", uploadFileInput.CheckpointFile,
					" err: ", err)
				return err
			}
		}
	}

	if uploadFileInput.PartSize < DefaultJCSMinPartSize {
		uploadFileInput.PartSize = DefaultJCSMinPartSize
	} else if uploadFileInput.PartSize > DefaultJCSMaxPartSize {
		uploadFileInput.PartSize = DefaultJCSMaxPartSize
	}

	err = o.resumeUpload(
		ctx,
		sourceFile,
		objectPath,
		taskId,
		uploadFileInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:resumeUpload failed.",
			" sourceFile: ", sourceFile,
			" objectPath: ", objectPath,
			" taskId: ", taskId,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFileResume finish.")
	return err
}

func (o *JCS) resumeUpload(
	ctx context.Context,
	sourceFile,
	objectPath string,
	taskId int32,
	input *JCSUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:resumeUpload start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" taskId: ", taskId)

	uploadFileStat, err := os.Stat(input.UploadFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" uploadFile: ", input.UploadFile,
			" err: ", err)
		return err
	}
	if uploadFileStat.IsDir() {
		Logger.WithContext(ctx).Error(
			"uploadFile can not be a folder.",
			" uploadFile: ", input.UploadFile)
		return errors.New("uploadFile can not be a folder")
	}

	ufc := &JCSUploadCheckpoint{}

	var needCheckpoint = true
	var checkpointFilePath = input.CheckpointFile
	var enableCheckpoint = input.EnableCheckpoint
	if enableCheckpoint {
		needCheckpoint, err = o.getUploadCheckpointFile(
			ctx,
			ufc,
			uploadFileStat,
			input)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:getUploadCheckpointFile failed.",
				" objectPath: ", objectPath,
				" taskId: ", taskId,
				" err: ", err)
			return err
		}
	}
	if needCheckpoint {
		err = o.prepareUpload(
			ctx,
			objectPath,
			taskId,
			ufc,
			uploadFileStat,
			input)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:prepareUpload failed.",
				" objectPath: ", objectPath,
				" taskId: ", taskId,
				" err: ", err)
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"JCS:updateCheckpointFile failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" err: ", err)
				return err
			}
		}
	}

	err = o.uploadPartConcurrent(
		ctx,
		sourceFile,
		taskId,
		ufc,
		input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:uploadPartConcurrent failed.",
			" sourceFile: ", sourceFile,
			" taskId: ", taskId,
			" err: ", err)
		return err
	}

	err = o.completeParts(
		ctx,
		taskId,
		ufc,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:completeParts failed.",
			" objectId: ", ufc.ObjectId,
			" taskId: ", taskId,
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCS:resumeUpload finish.")
	return err
}

func (o *JCS) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	taskId int32,
	ufc *JCSUploadCheckpoint,
	input *JCSUploadFileInput) error {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadPartConcurrent start.",
		" sourceFile: ", sourceFile,
		" taskId: ", taskId)

	var wg sync.WaitGroup
	pool, err := ants.NewPool(input.TaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			" err: ", err)
		return err
	}
	defer pool.Release()

	var uploadPartError atomic.Value
	var errFlag int32
	lock := new(sync.Mutex)

	for _, uploadPart := range ufc.UploadParts {
		if uploadPart.IsCompleted {
			continue
		}
		task := JCSUploadPartTask{
			ObjectId:         ufc.ObjectId,
			PartNumber:       uploadPart.PartNumber,
			SourceFile:       input.UploadFile,
			Offset:           uploadPart.Offset,
			PartSize:         uploadPart.PartSize,
			JcsClient:        o.jcsClient,
			EnableCheckpoint: input.EnableCheckpoint,
		}
		wg.Add(1)
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
			}()
			result := task.Run(ctx, sourceFile, taskId)
			err = o.handleUploadTaskResult(
				ctx,
				result,
				ufc,
				task.PartNumber,
				input.EnableCheckpoint,
				input.CheckpointFile,
				lock)
			if nil != err &&
				atomic.CompareAndSwapInt32(&errFlag, 0, 1) {

				Logger.WithContext(ctx).Error(
					"JCS:handleUploadTaskResult failed.",
					" partNumber: ", task.PartNumber,
					" checkpointFile: ", input.CheckpointFile,
					" err: ", err)
				uploadPartError.Store(err)
			}
			Logger.WithContext(ctx).Debug(
				"JCS:handleUploadTaskResult finish.")
			return
		})
	}
	wg.Wait()
	if err, ok := uploadPartError.Load().(error); ok {
		Logger.WithContext(ctx).Error(
			"uploadPartError load failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:uploadPartConcurrent finish.")
	return nil
}

func (o *JCS) getUploadCheckpointFile(
	ctx context.Context,
	ufc *JCSUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *JCSUploadFileInput) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:getUploadCheckpointFile start.")

	checkpointFilePath := input.CheckpointFile
	checkpointFileStat, err := os.Stat(checkpointFilePath)
	if nil != err {
		if !os.IsNotExist(err) {
			Logger.WithContext(ctx).Error(
				"os.Stat failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" err: ", err)
			return false, err
		}
		Logger.WithContext(ctx).Debug(
			"checkpointFilePath: ", checkpointFilePath, " not exist.")
		return true, nil
	}
	if checkpointFileStat.IsDir() {
		Logger.WithContext(ctx).Error(
			"checkpoint file can not be a folder.",
			" checkpointFilePath: ", checkpointFilePath)
		return false,
			errors.New("checkpoint file can not be a folder")
	}
	err = o.loadCheckpointFile(ctx, checkpointFilePath, ufc)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:loadCheckpointFile failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return true, nil
	} else if !ufc.IsValid(ctx, input.UploadFile, uploadFileStat) {
		_err := os.Remove(checkpointFilePath)
		if nil != _err {
			if !os.IsNotExist(_err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" err: ", _err)
			}
		}
	} else {
		Logger.WithContext(ctx).Debug(
			"JCS:loadCheckpointFile finish.",
			" checkpointFilePath: ", checkpointFilePath)
		return false, nil
	}
	Logger.WithContext(ctx).Debug(
		"JCS:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *JCS) prepareUpload(
	ctx context.Context,
	objectPath string,
	taskId int32,
	ufc *JCSUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *JCSUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:prepareUpload start.",
		" objectPath: ", objectPath,
		" taskId: ", taskId)

	newMultipartUploadOutput, err := o.NewMultipartUploadWithSignedUrl(
		ctx,
		objectPath,
		taskId)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:NewMultipartUploadWithSignedUrl failed.",
			" objectPath: ", objectPath,
			" taskId: ", taskId,
			" err: ", err)
		return err
	}

	ufc.ObjectId = newMultipartUploadOutput.Data.Object.ObjectID
	ufc.ObjectPath = objectPath
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = JCSFileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()

	err = o.sliceFile(ctx, input.PartSize, ufc)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:sliceFile failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:prepareUpload finish.")
	return err
}

func (o *JCS) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *JCSUploadCheckpoint) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:sliceFile start.",
		" partSize: ", partSize,
		" fileSize: ", ufc.FileInfo.Size)
	fileSize := ufc.FileInfo.Size
	cnt := fileSize / partSize
	if cnt >= 10000 {
		partSize = fileSize / 10000
		if fileSize%10000 != 0 {
			partSize++
		}
		cnt = fileSize / partSize
	}
	if fileSize%partSize != 0 {
		cnt++
	}

	if partSize > DefaultJCSMaxPartSize {
		Logger.WithContext(ctx).Error(
			"upload file part too large.",
			" partSize: ", partSize,
			" maxPartSize: ", DefaultJCSMaxPartSize)
		return fmt.Errorf("upload file part too large")
	}

	if cnt == 0 {
		uploadPart := JCSUploadPartInfo{}
		uploadPart.PartNumber = 1
		ufc.UploadParts = []JCSUploadPartInfo{uploadPart}
	} else {
		uploadParts := make([]JCSUploadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			uploadPart := JCSUploadPartInfo{}
			uploadPart.PartNumber = int32(i) + 1
			uploadPart.PartSize = partSize
			uploadPart.Offset = i * partSize
			uploadParts = append(uploadParts, uploadPart)
		}
		if value := fileSize % partSize; value != 0 {
			uploadParts[cnt-1].PartSize = value
		}
		ufc.UploadParts = uploadParts
	}
	Logger.WithContext(ctx).Debug(
		"JCS:sliceFile finish.")
	return nil
}

func (o *JCS) handleUploadTaskResult(
	ctx context.Context,
	result interface{},
	ufc *JCSUploadCheckpoint,
	partNum int32,
	enableCheckpoint bool,
	checkpointFilePath string,
	lock *sync.Mutex) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:handleUploadTaskResult start.",
		" checkpointFilePath: ", checkpointFilePath,
		" partNum: ", partNum)

	if _, ok := result.(*JCSBaseResponse); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"JCS:updateCheckpointFile failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" partNum: ", partNum,
					" err: ", _err)
			}
		}
	} else if result != errAbort {
		if _err, ok := result.(error); ok {
			Logger.WithContext(ctx).Error(
				"upload task result failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" partNum: ", partNum,
				" err: ", _err)
			err = _err
		}
	}
	Logger.WithContext(ctx).Debug(
		"JCS:handleUploadTaskResult finish.")
	return
}

func (o *JCS) completeParts(
	ctx context.Context,
	taskId int32,
	ufc *JCSUploadCheckpoint,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:completeParts start.",
		" objectId: ", ufc.ObjectId,
		" taskId: ", taskId,
		" checkpointFilePath: ", checkpointFilePath)

	parts := make([]int32, 0, len(ufc.UploadParts))
	for _, uploadPart := range ufc.UploadParts {
		parts = append(parts, uploadPart.PartNumber)
	}
	err = o.CompleteMultipartUploadWithSignedUrl(
		ctx,
		ufc.ObjectId,
		taskId,
		parts)
	if nil == err {
		if enableCheckpoint {
			_err := os.Remove(checkpointFilePath)
			if nil != _err {
				if !os.IsNotExist(_err) {
					Logger.WithContext(ctx).Error(
						"os.Remove failed.",
						" checkpointFilePath: ", checkpointFilePath,
						" err: ", _err)
				}
			}
		}
		Logger.WithContext(ctx).Debug(
			"JCS:CompleteMultipartUploadWithSignedUrl finish.")
		return err
	}
	Logger.WithContext(ctx).Error(
		"JCS.CompleteMultipartUpload failed.",
		" objectId: ", ufc.ObjectId,
		" taskId: ", taskId,
		" err: ", err)
	return err
}

type JCSUploadPartTask struct {
	ObjectId         int32
	ObjectPath       string
	PartNumber       int32
	SourceFile       string
	Offset           int64
	PartSize         int64
	JcsClient        *JCSClient
	EnableCheckpoint bool
}

func (task *JCSUploadPartTask) Run(
	ctx context.Context,
	sourceFile string,
	taskId int32) interface{} {

	Logger.WithContext(ctx).Debug(
		"JCSUploadPartTask:Run start.",
		" sourceFile: ", sourceFile,
		" taskId: ", taskId)

	fd, err := os.Open(sourceFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Open failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" sourceFile: ", sourceFile,
				" err: ", errMsg)
		}
	}()

	readerWrapper := new(ReaderWrapper)
	readerWrapper.Reader = fd

	readerWrapper.TotalCount = task.PartSize
	readerWrapper.Mark = task.Offset
	if _, err = fd.Seek(task.Offset, io.SeekStart); nil != err {
		Logger.WithContext(ctx).Error(
			"fd.Seek failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	createJCSPreSignedObjectUploadPartReq :=
		new(CreateJCSPreSignedObjectUploadPartReq)

	createJCSPreSignedObjectUploadPartReq.ObjectId = task.ObjectId
	createJCSPreSignedObjectUploadPartReq.Index = task.PartNumber
	createJCSPreSignedObjectUploadPartReq.TaskId = taskId
	err, createUploadPartSignedUrlResp :=
		UClient.CreateJCSPreSignedObjectUploadPart(
			ctx,
			createJCSPreSignedObjectUploadPartReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectUploadPart failed.",
			" objectId: ", task.ObjectId,
			" partNumber: ", task.PartNumber,
			" taskId: ", taskId,
			" err: ", err)
		return err
	}

	err, uploadPartOutput := task.JcsClient.UploadPartWithSignedUrl(
		ctx,
		createUploadPartSignedUrlResp.SignedUrl,
		readerWrapper)

	if nil != err {
		Logger.WithContext(ctx).Error(
			"JcsClient.UploadPartWithSignedUrl failed.",
			" signedUrl: ", createUploadPartSignedUrlResp.SignedUrl,
			" objectId: ", task.ObjectId,
			" partNumber: ", task.PartNumber,
			" taskId: ", taskId,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCSUploadPartTask:Run finish.")
	return uploadPartOutput
}

func (o *JCS) Download(
	ctx context.Context,
	targetPath string,
	taskId int32,
	bucketName string) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:Download start.",
		" targetPath: ", targetPath,
		" taskId: ", taskId,
		" bucketName: ", bucketName)

	continuationToken := ""
	for {
		listObjectsData, err := o.ListObjectsWithSignedUrl(
			ctx,
			taskId,
			continuationToken)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:ListObjectsWithSignedUrl start.",
				" taskId: ", taskId,
				" err: ", err)
			return err
		}
		err = o.downloadObjects(
			ctx,
			targetPath,
			taskId,
			listObjectsData)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:downloadObjects failed.",
				" targetPath: ", targetPath,
				" taskId: ", taskId,
				" bucketName: ", bucketName,
				" err: ", err)
			return err
		}
		if listObjectsData.IsTruncated {
			continuationToken = listObjectsData.NextContinuationToken
		} else {
			break
		}
	}
	Logger.WithContext(ctx).Debug(
		"JCS:Download finish.")
	return nil
}

func (o *JCS) downloadObjects(
	ctx context.Context,
	targetPath string,
	taskId int32,
	listObjectsData *JCSListData) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:downloadObjects start.",
		" targetPath: ", targetPath,
		" taskId: ", taskId)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			" taskId: ", taskId,
			" err: ", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		Logger.WithContext(ctx).Error(
			"task not exist. taskId: ", taskId)
		return errors.New("task not exist")
	}
	task := getTaskResp.Data.List[0].Task
	downloadObjectTaskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadObjectTaskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"DownloadObjectTaskParams Unmarshal failed.",
			" taskId: ", taskId,
			" params: ", task.Params,
			" err: ", err)
		return err
	}
	uuid := downloadObjectTaskParams.Request.ObjUuid

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	downloadFolderRecord := targetPath + uuid + ".download_folder_record"
	fileData, err := os.ReadFile(downloadFolderRecord)
	if nil == err {
		lines := strings.Split(string(fileData), "\n")
		for _, line := range lines {
			fileMap[strings.TrimSuffix(line, "\r")] = 0
		}
	} else if !os.IsNotExist(err) {
		Logger.WithContext(ctx).Error(
			"os.ReadFile failed.",
			" downloadFolderRecord: ", downloadFolderRecord,
			" err: ", err)
		return err
	}

	var isAllSuccess = true
	var wg sync.WaitGroup
	pool, err := ants.NewPool(DefaultJCSDownloadFileTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool for download Object  failed.",
			" err: ", err)
		return err
	}
	defer pool.Release()
	for index, object := range listObjectsData.Objects {
		Logger.WithContext(ctx).Debug(
			"object content.",
			" index: ", index,
			" ObjectID: ", object.ObjectID,
			" PackageID: ", object.PackageID,
			" Path: ", object.Path,
			" size: ", object.Size)
		// 处理文件
		itemObject := object
		if _, exists := fileMap[itemObject.Path]; exists {
			Logger.WithContext(ctx).Info(
				"file already success.",
				" Path: ", itemObject.Path)
			continue
		}
		wg.Add(1)
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
				if err := recover(); nil != err {
					Logger.WithContext(ctx).Error(
						"downloadFile failed.",
						" err: ", err)
					isAllSuccess = false
				}
			}()
			err = o.downloadPartWithSignedUrl(
				ctx,
				itemObject,
				targetPath+uuid+"/"+itemObject.Path,
				taskId)
			if nil != err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"JCS:downloadPartWithSignedUrl failed.",
					" objectPath: ", itemObject.Path,
					" targetFile: ", targetPath+itemObject.Path,
					" taskId: ", taskId,
					" err: ", err)
				return
			}
			fileMutex.Lock()
			defer fileMutex.Unlock()
			f, err := os.OpenFile(
				downloadFolderRecord,
				os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if nil != err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"os.OpenFile failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" err: ", err)
				return
			}
			defer func() {
				errMsg := f.Close()
				if errMsg != nil {
					Logger.WithContext(ctx).Warn(
						"close file failed.",
						" downloadFolderRecord: ", downloadFolderRecord,
						" err: ", errMsg)
				}
			}()
			_, err = f.Write([]byte(itemObject.Path + "\n"))
			if nil != err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"write file failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" objectKey: ", itemObject.Path,
					" err: ", err)
				return
			}
		})
		if nil != err {
			Logger.WithContext(ctx).Error(
				"ants.Submit failed.",
				" err: ", err)
			return err
		}
	}
	wg.Wait()
	if !isAllSuccess {
		Logger.WithContext(ctx).Error(
			"JCS:downloadObjects not all success.",
			" uuid: ", downloadObjectTaskParams.Request.ObjUuid)
		return errors.New("downloadObjects not all success")
	} else {
		_err := os.Remove(downloadFolderRecord)
		if nil != _err {
			if !os.IsNotExist(_err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" err: ", _err)
			}
		}
	}

	Logger.WithContext(ctx).Debug(
		"JCS:downloadObjects finish.")
	return nil
}

func (o *JCS) downloadPartWithSignedUrl(
	ctx context.Context,
	object *JCSObject,
	targetFile string,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:downloadPartWithSignedUrl start.",
		" ObjectID: ", object.ObjectID,
		" PackageID: ", object.PackageID,
		" Path: ", object.Path,
		" targetFile: ", targetFile,
		" taskId: ", taskId)

	downloadFileInput := new(JCSDownloadFileInput)
	downloadFileInput.Path = object.Path
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + ".download_file_record"
	downloadFileInput.TaskNum = DefaultJCSDownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize

	err = o.resumeDownload(
		ctx,
		taskId,
		object,
		downloadFileInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:resumeDownload failed.",
			" taskId: ", taskId,
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:downloadPartWithSignedUrl finish.")
	return
}

func (o *JCS) resumeDownload(
	ctx context.Context,
	taskId int32,
	object *JCSObject,
	input *JCSDownloadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:resumeDownload start.",
		" taskId: ", taskId)

	partSize := input.PartSize
	dfc := &JCSDownloadCheckpoint{}

	var needCheckpoint = true
	var checkpointFilePath = input.CheckpointFile
	var enableCheckpoint = input.EnableCheckpoint
	if enableCheckpoint {
		needCheckpoint, err = o.getDownloadCheckpointFile(
			ctx,
			dfc,
			input,
			object)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:getDownloadCheckpointFile failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" err: ", err)
			return err
		}
	}

	if needCheckpoint {
		objectSize, _ := strconv.ParseInt(object.Size, 10, 64)
		dfc.ObjectId = object.ObjectID
		dfc.DownloadFile = input.DownloadFile
		dfc.ObjectInfo = JCSObjectInfo{}
		dfc.ObjectInfo.Size = objectSize
		dfc.TempFileInfo = JCSTempFileInfo{}
		dfc.TempFileInfo.TempFileUrl = input.DownloadFile + ".tmp"
		dfc.TempFileInfo.Size = objectSize

		o.sliceObject(ctx, objectSize, partSize, dfc)
		err = o.prepareTempFile(ctx,
			dfc.TempFileInfo.TempFileUrl,
			dfc.TempFileInfo.Size)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:prepareTempFile failed.",
				" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
				" Size: ", dfc.TempFileInfo.Size,
				" err: ", err)
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"JCS:updateCheckpointFile failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" err: ", err)
				_errMsg := os.Remove(dfc.TempFileInfo.TempFileUrl)
				if _errMsg != nil {
					if !os.IsNotExist(_errMsg) {
						Logger.WithContext(ctx).Error(
							"os.Remove failed.",
							" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
							" err: ", _errMsg)
					}
				}
				return err
			}
		}
	}

	downloadFileError := o.downloadFileConcurrent(
		ctx, taskId, input, dfc)
	err = o.handleDownloadFileResult(
		ctx,
		dfc.TempFileInfo.TempFileUrl,
		enableCheckpoint,
		downloadFileError)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:handleDownloadFileResult failed.",
			" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
			" err: ", err)
		return err
	}

	err = os.Rename(dfc.TempFileInfo.TempFileUrl, input.DownloadFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Rename failed.",
			" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
			" DownloadFile: ", input.DownloadFile,
			" err: ", err)
		return err
	}
	if enableCheckpoint {
		_err := os.Remove(checkpointFilePath)
		if nil != _err {
			if !os.IsNotExist(_err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" err: ", _err)
			}
		}
	}
	Logger.WithContext(ctx).Debug(
		"JCS:resumeDownload finish.")
	return nil
}

func (o *JCS) downloadFileConcurrent(
	ctx context.Context,
	taskId int32,
	input *JCSDownloadFileInput,
	dfc *JCSDownloadCheckpoint) error {

	Logger.WithContext(ctx).Debug(
		"JCS:downloadFileConcurrent start.",
		" taskId: ", taskId)

	var wg sync.WaitGroup
	pool, err := ants.NewPool(input.TaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			" err: ", err)
		return err
	}
	defer pool.Release()

	var downloadPartError atomic.Value
	var errFlag int32
	lock := new(sync.Mutex)

	for _, downloadPart := range dfc.DownloadParts {
		if downloadPart.IsCompleted {
			continue
		}
		task := JCSDownloadPartTask{
			ObjectId:         dfc.ObjectId,
			Offset:           downloadPart.Offset,
			Length:           downloadPart.Length,
			JcsClient:        o.jcsClient,
			Jcs:              o,
			PartNumber:       downloadPart.PartNumber,
			TempFileURL:      dfc.TempFileInfo.TempFileUrl,
			EnableCheckpoint: input.EnableCheckpoint,
		}
		Logger.WithContext(ctx).Debug(
			"DownloadPartTask params.",
			" ObjectId: ", dfc.ObjectId,
			" Offset: ", downloadPart.Offset,
			" Length: ", downloadPart.Length,
			" PartNumber: ", downloadPart.PartNumber,
			" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
			" EnableCheckpoint: ", input.EnableCheckpoint)

		wg.Add(1)
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
			}()
			if 0 == dfc.ObjectInfo.Size {
				lock.Lock()
				defer lock.Unlock()
				dfc.DownloadParts[task.PartNumber-1].IsCompleted = true

				if input.EnableCheckpoint {
					err := o.updateCheckpointFile(
						ctx,
						dfc,
						input.CheckpointFile)
					if nil != err {
						Logger.WithContext(ctx).Error(
							"JCS:updateCheckpointFile failed.",
							" checkpointFile: ", input.CheckpointFile,
							" err: ", err)
						downloadPartError.Store(err)
					}
				}
				return
			} else {
				result := task.Run(ctx, taskId)
				err = o.handleDownloadTaskResult(
					ctx,
					result,
					dfc,
					task.PartNumber,
					input.EnableCheckpoint,
					input.CheckpointFile,
					lock)
				if nil != err &&
					atomic.CompareAndSwapInt32(&errFlag, 0, 1) {

					Logger.WithContext(ctx).Error(
						"JCS:handleDownloadTaskResult failed.",
						" partNumber: ", task.PartNumber,
						" checkpointFile: ", input.CheckpointFile,
						" err: ", err)
					downloadPartError.Store(err)
				}
				return
			}
		})
		if nil != err {
			Logger.WithContext(ctx).Error(
				"ants.Submit failed.",
				" err: ", err)
			return err
		}
	}
	wg.Wait()
	if err, ok := downloadPartError.Load().(error); ok {
		Logger.WithContext(ctx).Error(
			"downloadPartError failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:downloadFileConcurrent finish.")
	return nil
}

func (o *JCS) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *JCSDownloadCheckpoint,
	input *JCSDownloadFileInput,
	object *JCSObject) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:getDownloadCheckpointFile start.",
		" checkpointFile: ", input.CheckpointFile)

	checkpointFilePath := input.CheckpointFile
	checkpointFileStat, err := os.Stat(checkpointFilePath)
	if nil != err {
		if !os.IsNotExist(err) {
			Logger.WithContext(ctx).Error(
				"os.Stat failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" err: ", err)
			return false, err
		}
		Logger.WithContext(ctx).Debug(
			"checkpointFilePath: ", checkpointFilePath, " not exist.")
		return true, nil
	}
	if checkpointFileStat.IsDir() {
		Logger.WithContext(ctx).Error(
			"checkpointFilePath can not be a folder.",
			" checkpointFilePath: ", checkpointFilePath)
		return false,
			errors.New("checkpoint file can not be a folder")
	}
	err = o.loadCheckpointFile(ctx, checkpointFilePath, dfc)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:loadCheckpointFile failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return true, nil
	} else if !dfc.IsValid(ctx, input, object) {
		if dfc.TempFileInfo.TempFileUrl != "" {
			_err := os.Remove(dfc.TempFileInfo.TempFileUrl)
			if nil != _err {
				if !os.IsNotExist(_err) {
					Logger.WithContext(ctx).Error(
						"os.Remove failed.",
						" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
						" err: ", _err)
				}
			}
		}
		_err := os.Remove(checkpointFilePath)
		if nil != _err {
			if !os.IsNotExist(_err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" err: ", _err)
			}
		}
	} else {
		Logger.WithContext(ctx).Debug(
			"no need to check point.")
		return false, nil
	}
	Logger.WithContext(ctx).Debug(
		"need to check point.")
	return true, nil
}

func (o *JCS) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"JCS:prepareTempFile start.",
		" tempFileURL: ", tempFileURL,
		" fileSize: ", fileSize)

	parentDir := filepath.Dir(tempFileURL)
	stat, err := os.Stat(parentDir)
	if nil != err {
		if !os.IsNotExist(err) {
			Logger.WithContext(ctx).Error(
				"os.Stat failed.",
				" parentDir: ", parentDir,
				" err: ", err)
			return err
		}
		Logger.WithContext(ctx).Debug(
			"parentDir: ", parentDir, " not exist.")

		_err := os.MkdirAll(parentDir, os.ModePerm)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"os.MkdirAll failed.",
				" parentDir: ", parentDir,
				" err: ", _err)
			return _err
		}
	} else if !stat.IsDir() {
		Logger.WithContext(ctx).Error(
			"same file exists. parentDir: ", parentDir)
		return fmt.Errorf(
			"cannot create folder: %s due to a same file exists",
			parentDir)
	}

	err = o.createFile(ctx, tempFileURL, fileSize)
	if nil == err {
		Logger.WithContext(ctx).Debug(
			"JCS:createFile finish.",
			" tempFileURL: ", tempFileURL,
			" fileSize: ", fileSize)
		return nil
	}
	fd, err := os.OpenFile(
		tempFileURL,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0640)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.OpenFile failed.",
			" tempFileURL: ", tempFileURL,
			" err: ", err)
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" tempFileURL: ", tempFileURL,
				" err: ", errMsg)
		}
	}()
	if fileSize > 0 {
		_, err = fd.WriteAt([]byte("a"), fileSize-1)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"write file failed.",
				" tempFileURL: ", tempFileURL,
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"JCS:prepareTempFile finish.")
	return nil
}

func (o *JCS) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"JCS:createFile start.",
		" tempFileURL: ", tempFileURL,
		" fileSize: ", fileSize)

	fd, err := syscall.Open(
		tempFileURL,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0640)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"syscall.Open failed.",
			" tempFileURL: ", tempFileURL,
			" err: ", err)
		return err
	}
	defer func() {
		errMsg := syscall.Close(fd)
		if errMsg != nil {
			Logger.WithContext(ctx).Warn(
				"syscall.Close failed.",
				" tempFileURL: ", tempFileURL,
				" err: ", errMsg)
		}
	}()
	err = syscall.Ftruncate(fd, fileSize)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"syscall.Ftruncate failed.",
			" tempFileURL: ", tempFileURL,
			" fileSize: ", fileSize,
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:createFile finish.")
	return nil
}

func (o *JCS) handleDownloadTaskResult(
	ctx context.Context,
	result interface{},
	dfc *JCSDownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:handleDownloadTaskResult start.",
		" partNum: ", partNum,
		" checkpointFile: ", checkpointFile)

	if _, ok := result.(*JCSDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				Logger.WithContext(ctx).Warn(
					"JCS:updateCheckpointFile failed.",
					" checkpointFile: ", checkpointFile,
					" err: ", _err)
			}
		}
	} else if result != errAbort {
		if _err, ok := result.(error); ok {
			err = _err
		}
	}
	Logger.WithContext(ctx).Debug(
		"JCS:handleDownloadTaskResult finish.")
	return
}

func (o *JCS) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	Logger.WithContext(ctx).Debug(
		"JCS:handleDownloadFileResult start.",
		" tempFileURL: ", tempFileURL)

	if downloadFileError != nil {
		if !enableCheckpoint {
			_err := os.Remove(tempFileURL)
			if nil != _err {
				if !os.IsNotExist(_err) {
					Logger.WithContext(ctx).Error(
						"os.Remove failed.",
						" tempFileURL: ", tempFileURL,
						" err: ", _err)
				}
			}
		}
		Logger.WithContext(ctx).Debug(
			"JCS.handleDownloadFileResult finish.",
			" tempFileURL: ", tempFileURL,
			" downloadFileError: ", downloadFileError)
		return downloadFileError
	}

	Logger.WithContext(ctx).Debug(
		"JCS.handleDownloadFileResult finish.")
	return nil
}

func (o *JCS) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *JCSDownloadPartOutput) error {

	Logger.WithContext(ctx).Debug(
		"JCS:UpdateDownloadFile start.",
		" filePath: ", filePath,
		" offset: ", offset)

	fd, err := os.OpenFile(filePath, os.O_WRONLY, 0640)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.OpenFile failed.",
			" filePath: ", filePath,
			" err: ", err)
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" filePath: ", filePath,
				" err: ", errMsg)
		}
	}()
	_, err = fd.Seek(offset, 0)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"seek file failed.",
			" filePath: ", filePath,
			" offset: ", offset,
			" err: ", err)
		return err
	}
	fileWriter := bufio.NewWriterSize(fd, 65536)
	part := make([]byte, 8192)
	var readErr error
	var readCount, readTotal int
	for {
		readCount, readErr = downloadPartOutput.Body.Read(part)
		if readCount > 0 {
			writeCount, writeError := fileWriter.Write(part[0:readCount])
			if writeError != nil {
				Logger.WithContext(ctx).Error(
					"write file failed.",
					" filePath: ", filePath,
					" err: ", writeError)
				return writeError
			}
			if writeCount != readCount {
				Logger.WithContext(ctx).Error(
					" write file failed.",
					" filePath: ", filePath,
					" readCount: ", readCount,
					" writeCount: ", writeCount)
				return fmt.Errorf("failed to write to file."+
					" filePath: %s, expect: %d, actual: %d",
					filePath, readCount, writeCount)
			}
			readTotal = readTotal + readCount
		}
		if readErr != nil {
			if readErr != io.EOF {
				Logger.WithContext(ctx).Error(
					"read response body failed.",
					" err: ", readErr)
				return readErr
			}
			break
		}
	}
	err = fileWriter.Flush()
	if nil != err {
		Logger.WithContext(ctx).Error(
			"flush file failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCS:UpdateDownloadFile finish.",
		" readTotal: ", readTotal)
	return nil
}

type JCSDownloadPartTask struct {
	ObjectId         int32
	Offset           int64
	Length           int64
	JcsClient        *JCSClient
	Jcs              *JCS
	PartNumber       int64
	TempFileURL      string
	EnableCheckpoint bool
}

func (task *JCSDownloadPartTask) Run(
	ctx context.Context,
	taskId int32) interface{} {

	Logger.WithContext(ctx).Debug(
		"JCSDownloadPartTask:Run start.",
		" taskId: ", taskId,
		" partNumber: ", task.PartNumber)

	createJCSPreSignedObjectDownloadReq :=
		new(CreateJCSPreSignedObjectDownloadReq)

	createJCSPreSignedObjectDownloadReq.Offset = task.Offset
	createJCSPreSignedObjectDownloadReq.ObjectId = task.ObjectId
	createJCSPreSignedObjectDownloadReq.Length = task.Length
	createJCSPreSignedObjectDownloadReq.TaskId = taskId

	err, createJCSPreSignedObjectDownloadResp :=
		UClient.CreateJCSPreSignedObjectDownload(
			ctx,
			createJCSPreSignedObjectDownloadReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateGetObjectSignedUrl failed.",
			" err: ", err)
		return err
	}

	err, downloadPartOutput :=
		task.JcsClient.DownloadPartWithSignedUrl(
			ctx,
			createJCSPreSignedObjectDownloadResp.SignedUrl)

	if nil == err {
		Logger.WithContext(ctx).Debug(
			"JcsClient.DownloadPartWithSignedUrl finish.")
		defer func() {
			errMsg := downloadPartOutput.Body.Close()
			if errMsg != nil {
				Logger.WithContext(ctx).Warn(
					"close response body failed.")
			}
		}()
		_err := task.Jcs.UpdateDownloadFile(
			ctx,
			task.TempFileURL,
			task.Offset,
			downloadPartOutput)
		if nil != _err {
			if !task.EnableCheckpoint {
				Logger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					" partNumber: ", task.PartNumber)
			}
			Logger.WithContext(ctx).Error(
				"JCS.updateDownloadFile failed.",
				" err: ", _err)
			return _err
		}
		Logger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.")
		return downloadPartOutput
	}

	Logger.WithContext(ctx).Error(
		"JCSDownloadPartTask:Run failed.",
		" signedUrl: ", createJCSPreSignedObjectDownloadResp.SignedUrl,
		" err: ", err)
	return err
}
