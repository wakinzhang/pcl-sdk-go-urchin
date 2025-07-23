package adaptee

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

type JCSProxy struct {
	jcsProxyClient          *JCSProxyClient
	jcsUploadFileTaskNum    int
	jcsUploadMultiTaskNum   int
	jcsDownloadFileTaskNum  int
	jcsDownloadMultiTaskNum int
	jcsRateLimiter          *rate.Limiter
}

func (o *JCSProxy) Init(
	ctx context.Context,
	reqTimeout,
	maxConnection int) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:Init start.",
		zap.Int("reqTimeout", reqTimeout),
		zap.Int("maxConnection", maxConnection))

	o.jcsProxyClient = new(JCSProxyClient)
	o.jcsProxyClient.Init(ctx, reqTimeout, maxConnection)

	o.jcsUploadFileTaskNum = DefaultJCSUploadFileTaskNum
	o.jcsUploadMultiTaskNum = DefaultJCSUploadMultiTaskNum
	o.jcsDownloadFileTaskNum = DefaultJCSDownloadFileTaskNum
	o.jcsDownloadMultiTaskNum = DefaultJCSDownloadMultiTaskNum

	o.jcsRateLimiter = rate.NewLimiter(
		DefaultJCSRateLimit,
		DefaultJCSRateBurst)

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:Init finish.")
	return nil
}

func (o *JCSProxy) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("UploadMultiTaskNum", config.UploadMultiTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	o.jcsUploadFileTaskNum = int(config.UploadFileTaskNum)
	o.jcsUploadMultiTaskNum = int(config.UploadMultiTaskNum)
	o.jcsDownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.jcsDownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:SetConcurrency finish.")
	return nil
}

func (o *JCSProxy) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:SetRate start.")

	o.jcsRateLimiter = rateLimiter

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:SetRate finish.")
	return nil
}

func (o *JCSProxy) NewFolderWithSignedUrl(
	ctx context.Context,
	objectPath string,
	taskId int32) (err error) {

	if 0 < len(objectPath) &&
		'/' != objectPath[len(objectPath)-1] {

		objectPath = objectPath + "/"
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:NewFolderWithSignedUrl start.",
		zap.String("objectPath", objectPath),
		zap.Int32("taskId", taskId))

	createJCSPreSignedObjectUploadReq :=
		new(CreateJCSPreSignedObjectUploadReq)
	createJCSPreSignedObjectUploadReq.TaskId = taskId
	createJCSPreSignedObjectUploadReq.Source = objectPath

	err, createJCSPreSignedObjectUploadResp :=
		UClient.CreateJCSPreSignedObjectUpload(
			ctx,
			createJCSPreSignedObjectUploadReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectUpload"+
				" failed.",
			zap.Error(err))
		return err
	}

	err = o.jcsProxyClient.UploadFileWithSignedUrl(
		ctx,
		createJCSPreSignedObjectUploadResp.SignedUrl,
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsProxyClient.UploadFileWithSignedUrl failed.",
			zap.String("signedUrl",
				createJCSPreSignedObjectUploadResp.SignedUrl),
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:NewFolderWithSignedUrl finish.")

	return err
}

func (o *JCSProxy) UploadFileWithSignedUrl(
	ctx context.Context,
	sourceFile,
	objectPath string,
	taskId int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:UploadFileWithSignedUrl start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
		zap.Int32("taskId", taskId))

	createJCSPreSignedObjectUploadReq :=
		new(CreateJCSPreSignedObjectUploadReq)
	createJCSPreSignedObjectUploadReq.TaskId = taskId
	createJCSPreSignedObjectUploadReq.Source = objectPath

	err, createJCSPreSignedObjectUploadResp :=
		UClient.CreateJCSPreSignedObjectUpload(
			ctx,
			createJCSPreSignedObjectUploadReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectUpload"+
				" failed.",
			zap.Error(err))
		return err
	}

	fd, err := os.Open(sourceFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Open failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			ErrorLogger.WithContext(ctx).Warn(
				"close file failed.",
				zap.String("sourceFile", sourceFile),
				zap.Error(errMsg))
		}
	}()

	err = o.jcsProxyClient.UploadFileWithSignedUrl(
		ctx,
		createJCSPreSignedObjectUploadResp.SignedUrl,
		fd)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsProxyClient.UploadFileWithSignedUrl failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("signedUrl",
				createJCSPreSignedObjectUploadResp.SignedUrl),
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:UploadFileWithSignedUrl finish.")

	return err
}

func (o *JCSProxy) NewMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectPath string,
	taskId int32) (output *JCSNewMultiPartUploadResponse, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:NewMultipartUploadWithSignedUrl start.",
		zap.String("objectPath", objectPath),
		zap.Int32("taskId", taskId))

	createJCSPreSignedObjectNewMultipartUploadReq :=
		new(CreateJCSPreSignedObjectNewMultipartUploadReq)
	createJCSPreSignedObjectNewMultipartUploadReq.TaskId = taskId
	createJCSPreSignedObjectNewMultipartUploadReq.Source = objectPath

	err, createJCSPreSignedObjectNewMultipartUploadResp :=
		UClient.CreateJCSPreSignedObjectNewMultipartUpload(
			ctx,
			createJCSPreSignedObjectNewMultipartUploadReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectNewMultipartUpload"+
				" failed.",
			zap.Error(err))
		return output, err
	}

	// 初始化分段上传任务
	err, output = o.jcsProxyClient.NewMultiPartUploadWithSignedUrl(
		ctx,
		createJCSPreSignedObjectNewMultipartUploadResp.SignedUrl)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsProxyClient.NewMultiPartUploadWithSignedUrl failed.",
			zap.String("signedUrl",
				createJCSPreSignedObjectNewMultipartUploadResp.SignedUrl),
			zap.Error(err))
		return output, err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:NewMultipartUploadWithSignedUrl finish.")

	return output, err
}

func (o *JCSProxy) CompleteMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectId int32,
	taskId int32,
	indexes []int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:CompleteMultipartUploadWithSignedUrl start.",
		zap.Int32("objectId", objectId),
		zap.Int32("taskId", taskId))

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
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient:CreateCompleteMultipartUploadSignedUrl"+
				" failed.",
			zap.Error(err))
		return err
	}

	err, _ = o.jcsProxyClient.CompleteMultiPartUploadWithSignedUrl(
		ctx,
		createCompleteMultipartUploadSignedUrlResp.SignedUrl)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsProxyClient.CompleteMultipartUploadWithSignedUrl"+
				" failed.",
			zap.String("signedUrl",
				createCompleteMultipartUploadSignedUrlResp.SignedUrl),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy.CompleteMultipartUploadWithSignedUrl finish.")
	return nil
}

func (o *JCSProxy) ListObjectsWithSignedUrl(
	ctx context.Context,
	taskId int32,
	continuationToken string) (
	listObjectsData *JCSListData,
	err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:ListObjectsWithSignedUrl start.",
		zap.Int32("taskId", taskId),
		zap.String("continuationToken", continuationToken))

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
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectList failed.",
			zap.Error(err))
		return listObjectsData, err
	}

	err, listObjectsResponse :=
		o.jcsProxyClient.ListWithSignedUrl(
			ctx,
			createListObjectsSignedUrlResp.SignedUrl)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsProxyClient.ListWithSignedUrl failed.",
			zap.String("signedUrl",
				createListObjectsSignedUrlResp.SignedUrl),
			zap.Error(err))
		return listObjectsData, err
	}
	listObjectsData = new(JCSListData)
	listObjectsData = listObjectsResponse.Data

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:ListObjectsWithSignedUrl finish.")
	return listObjectsData, nil
}

func (o *JCSProxy) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:loadCheckpointFile start.",
		zap.String("checkpointFile", checkpointFile))

	ret, err := os.ReadFile(checkpointFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.ReadFile failed.",
			zap.String("checkpointFile", checkpointFile),
			zap.Error(err))
		return err
	}
	if len(ret) == 0 {
		InfoLogger.WithContext(ctx).Debug(
			"checkpointFile empty.",
			zap.String("checkpointFile", checkpointFile))
		return errors.New("checkpointFile empty")
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *JCSProxy) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *JCSDownloadCheckpoint) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:sliceObject start.",
		zap.Int64("objectSize", objectSize),
		zap.Int64("partSize", partSize))

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
			dfc.DownloadParts[cnt-1].Length = value
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:sliceObject finish.")
}

func (o *JCSProxy) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:updateCheckpointFile start.",
		zap.String("checkpointFilePath", checkpointFilePath))

	result, err := xml.Marshal(fc)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"xml.Marshal failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return err
	}
	err = os.WriteFile(checkpointFilePath, result, 0640)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.WriteFile failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return err
	}
	file, _ := os.OpenFile(checkpointFilePath, os.O_WRONLY, 0)
	defer func() {
		errMsg := file.Close()
		if errMsg != nil {
			ErrorLogger.WithContext(ctx).Warn(
				"close file failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(errMsg))
		}
	}()
	_ = file.Sync()

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:updateCheckpointFile finish.")
	return err
}

func (o *JCSProxy) Upload(
	ctx context.Context,
	userId string,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:Upload start.",
		zap.String("userId", userId),
		zap.String("sourcePath", sourcePath),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure))

	stat, err := os.Stat(sourcePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}
	if stat.IsDir() {
		err = o.uploadFolder(ctx, sourcePath, taskId, needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy.uploadFolder failed.",
				zap.String("sourcePath", sourcePath),
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return err
		}
	} else {
		objectPath := filepath.Base(sourcePath)
		err = o.uploadFile(
			ctx,
			sourcePath,
			objectPath,
			taskId,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy.uploadFile failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("objectPath", objectPath),
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:Upload finish.")
	return nil
}

func (o *JCSProxy) uploadFolder(
	ctx context.Context,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:uploadFolder start.",
		zap.String("sourcePath", sourcePath),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure))

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	uploadFolderRecord :=
		strings.TrimSuffix(sourcePath, "/") +
			"_" +
			strconv.FormatInt(int64(taskId), 10) +
			UploadFolderRecordSuffix
	InfoLogger.WithContext(ctx).Debug(
		"uploadFolderRecord file info.",
		zap.String("uploadFolderRecord", uploadFolderRecord))

	if needPure {
		err = os.Remove(uploadFolderRecord)
		if nil != err {
			if !os.IsNotExist(err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("uploadFolderRecord", uploadFolderRecord),
					zap.Error(err))
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
			ErrorLogger.WithContext(ctx).Error(
				"os.ReadFile failed.",
				zap.String("uploadFolderRecord", uploadFolderRecord),
				zap.Error(err))
			return err
		}
	}

	pool, err := ants.NewPool(o.jcsUploadFileTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			zap.Error(err))
		return err
	}
	defer pool.Release()

	var isAllSuccess = true
	var wg sync.WaitGroup
	err = filepath.Walk(
		sourcePath,
		func(filePath string, fileInfo os.FileInfo, err error) error {

			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"filepath.Walk failed.",
					zap.String("sourcePath", sourcePath),
					zap.Error(err))
				return err
			}

			if sourcePath == filePath {
				InfoLogger.WithContext(ctx).Debug(
					"root dir no need todo.")
				return nil
			}

			InfoLogger.WithContext(ctx).Debug(
				"RateLimiter.Wait start.")
			ctxRate, cancel := context.WithCancel(context.Background())
			err = o.jcsRateLimiter.Wait(ctxRate)
			if nil != err {
				cancel()
				ErrorLogger.WithContext(ctx).Error(
					"RateLimiter.Wait failed.",
					zap.Error(err))
				return err
			}
			cancel()
			InfoLogger.WithContext(ctx).Debug(
				"RateLimiter.Wait end.")

			wg.Add(1)
			err = pool.Submit(func() {
				defer func() {
					wg.Done()
					if _err := recover(); nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"JCSProxy:uploadFileResume failed.",
							zap.Any("error", _err))
						isAllSuccess = false
					}
				}()
				objectKey, _err := filepath.Rel(sourcePath, filePath)
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"filepath.Rel failed.",
						zap.String("sourcePath", sourcePath),
						zap.String("filePath", filePath),
						zap.String("objectKey", objectKey),
						zap.Error(_err))
					return
				}
				if strings.HasSuffix(objectKey, UploadFileRecordSuffix) {
					InfoLogger.WithContext(ctx).Info(
						"upload record file.",
						zap.String("objectKey", objectKey))
					return
				}
				if _, exists := fileMap[objectKey]; exists {
					InfoLogger.WithContext(ctx).Info(
						"already finish.",
						zap.String("objectKey", objectKey))
					return
				}
				if fileInfo.IsDir() {
					_err = o.NewFolderWithSignedUrl(
						ctx,
						objectKey,
						taskId)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"JCSProxy:NewFolderWithSignedUrl failed.",
							zap.String("objectKey", objectKey),
							zap.Error(_err))
						return
					}
				} else {
					_err = o.uploadFile(
						ctx,
						filePath,
						objectKey,
						taskId,
						needPure)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"JCSProxy:uploadFile failed.",
							zap.String("filePath", filePath),
							zap.String("objectKey", objectKey),
							zap.Error(_err))
						return
					}
				}
				fileMutex.Lock()
				defer fileMutex.Unlock()
				f, _err := os.OpenFile(
					uploadFolderRecord,
					os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"os.OpenFile failed.",
						zap.String("uploadFolderRecord",
							uploadFolderRecord),
						zap.Error(_err))
					return
				}
				defer func() {
					errMsg := f.Close()
					if errMsg != nil {
						ErrorLogger.WithContext(ctx).Warn(
							"close file failed.",
							zap.Error(errMsg))
					}
				}()
				_, _err = f.Write([]byte(objectKey + "\n"))
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"write file failed.",
						zap.String("uploadFolderRecord",
							uploadFolderRecord),
						zap.String("objectKey", objectKey),
						zap.Error(_err))
					return
				}
				return
			})
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"ants.Submit failed.",
					zap.Error(err))
				return err
			}
			return nil
		})
	wg.Wait()

	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"filepath.Walk failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}
	if !isAllSuccess {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:uploadFolder not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("uploadFolder not all success")
	} else {
		_err := os.Remove(uploadFolderRecord)
		if nil != _err {
			if !os.IsNotExist(_err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("uploadFolderRecord", uploadFolderRecord),
					zap.Error(_err))
			}
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:uploadFolder finish.")
	return nil
}

func (o *JCSProxy) uploadFile(
	ctx context.Context,
	sourceFile,
	objectPath string,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:uploadFile start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure))

	sourceFileStat, err := os.Stat(sourceFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
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
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy:uploadFileResume failed.",
				zap.String("sourceFile", sourceFile),
				zap.String("objectPath", objectPath),
				zap.Int32("taskId", taskId),
				zap.Bool("needPure", needPure),
				zap.Error(err))
			return err
		}
	} else {
		err = o.UploadFileWithSignedUrl(
			ctx,
			sourceFile,
			objectPath,
			taskId)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy:UploadFileWithSignedUrl failed.",
				zap.String("sourceFile", sourceFile),
				zap.String("objectPath", objectPath),
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:uploadFile finish.")
	return err
}

func (o *JCSProxy) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectPath string,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:uploadFileResume start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure))

	uploadFileInput := new(JCSUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile +
			"_" +
			strconv.FormatInt(int64(taskId), 10) +
			UploadFileRecordSuffix
	uploadFileInput.TaskNum = o.jcsUploadMultiTaskNum
	uploadFileInput.PartSize = DefaultPartSize
	if uploadFileInput.PartSize < DefaultJCSMinPartSize {
		uploadFileInput.PartSize = DefaultJCSMinPartSize
	} else if uploadFileInput.PartSize > DefaultJCSMaxPartSize {
		uploadFileInput.PartSize = DefaultJCSMaxPartSize
	}

	if needPure {
		err = os.Remove(uploadFileInput.CheckpointFile)
		if nil != err {
			if !os.IsNotExist(err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("CheckpointFile",
						uploadFileInput.CheckpointFile),
					zap.Error(err))
				return err
			}
		}
	}

	err = o.resumeUpload(
		ctx,
		sourceFile,
		objectPath,
		taskId,
		uploadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:resumeUpload failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("objectPath", objectPath),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:uploadFileResume finish.")
	return err
}

func (o *JCSProxy) resumeUpload(
	ctx context.Context,
	sourceFile,
	objectPath string,
	taskId int32,
	input *JCSUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:resumeUpload start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
		zap.Int32("taskId", taskId))

	uploadFileStat, err := os.Stat(input.UploadFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("uploadFile", input.UploadFile),
			zap.Error(err))
		return err
	}
	if uploadFileStat.IsDir() {
		ErrorLogger.WithContext(ctx).Error(
			"uploadFile can not be a folder.",
			zap.String("uploadFile", input.UploadFile))
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
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy:getUploadCheckpointFile failed.",
				zap.String("objectPath", objectPath),
				zap.Int32("taskId", taskId),
				zap.Error(err))
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
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy:prepareUpload failed.",
				zap.String("objectPath", objectPath),
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"JCSProxy:updateCheckpointFile failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Error(err))
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
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:uploadPartConcurrent failed.",
			zap.String("sourceFile", sourceFile),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return err
	}

	err = o.completeParts(
		ctx,
		taskId,
		ufc,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:completeParts failed.",
			zap.Int32("objectId", ufc.ObjectId),
			zap.Int32("taskId", taskId),
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:resumeUpload finish.")
	return err
}

func (o *JCSProxy) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	taskId int32,
	ufc *JCSUploadCheckpoint,
	input *JCSUploadFileInput) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:uploadPartConcurrent start.",
		zap.String("sourceFile", sourceFile),
		zap.Int32("taskId", taskId))

	var wg sync.WaitGroup
	pool, err := ants.NewPool(input.TaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			zap.Error(err))
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
		task := JCSProxyUploadPartTask{
			ObjectId:         ufc.ObjectId,
			ObjectPath:       ufc.ObjectPath,
			PartNumber:       uploadPart.PartNumber,
			SourceFile:       input.UploadFile,
			Offset:           uploadPart.Offset,
			PartSize:         uploadPart.PartSize,
			JcsProxyClient:   o.jcsProxyClient,
			EnableCheckpoint: input.EnableCheckpoint,
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.jcsRateLimiter.Wait(ctxRate)
		if nil != err {
			cancel()
			ErrorLogger.WithContext(ctx).Error(
				"RateLimiter.Wait failed.",
				zap.Error(err))
			return err
		}
		cancel()
		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait end.")

		wg.Add(1)
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
			}()
			result := task.Run(ctx, sourceFile, taskId)
			_err := o.handleUploadTaskResult(
				ctx,
				result,
				ufc,
				task.PartNumber,
				input.EnableCheckpoint,
				input.CheckpointFile,
				lock)
			if nil != _err &&
				atomic.CompareAndSwapInt32(&errFlag, 0, 1) {

				ErrorLogger.WithContext(ctx).Error(
					"JCSProxy:handleUploadTaskResult failed.",
					zap.Int32("partNumber", task.PartNumber),
					zap.String("checkpointFile", input.CheckpointFile),
					zap.Error(_err))
				uploadPartError.Store(_err)
			}
			InfoLogger.WithContext(ctx).Debug(
				"JCSProxy:handleUploadTaskResult finish.")
			return
		})
	}
	wg.Wait()
	if err, ok := uploadPartError.Load().(error); ok {
		ErrorLogger.WithContext(ctx).Error(
			"uploadPartError load failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:uploadPartConcurrent finish.")
	return nil
}

func (o *JCSProxy) getUploadCheckpointFile(
	ctx context.Context,
	ufc *JCSUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *JCSUploadFileInput) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:getUploadCheckpointFile start.")

	checkpointFilePath := input.CheckpointFile
	checkpointFileStat, err := os.Stat(checkpointFilePath)
	if nil != err {
		if !os.IsNotExist(err) {
			ErrorLogger.WithContext(ctx).Error(
				"os.Stat failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(err))
			return false, err
		}
		InfoLogger.WithContext(ctx).Debug(
			"checkpointFilePath not exist.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return true, nil
	}
	if checkpointFileStat.IsDir() {
		ErrorLogger.WithContext(ctx).Error(
			"checkpoint file can not be a folder.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return false,
			errors.New("checkpoint file can not be a folder")
	}
	err = o.loadCheckpointFile(ctx, checkpointFilePath, ufc)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:loadCheckpointFile failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return true, nil
	} else if !ufc.IsValid(ctx, input.UploadFile, uploadFileStat) {
		_err := os.Remove(checkpointFilePath)
		if nil != _err {
			if !os.IsNotExist(_err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Error(_err))
			}
		}
	} else {
		InfoLogger.WithContext(ctx).Debug(
			"JCSProxy:loadCheckpointFile finish.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return false, nil
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *JCSProxy) prepareUpload(
	ctx context.Context,
	objectPath string,
	taskId int32,
	ufc *JCSUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *JCSUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:prepareUpload start.",
		zap.String("objectPath", objectPath),
		zap.Int32("taskId", taskId))

	newMultipartUploadOutput, err := o.NewMultipartUploadWithSignedUrl(
		ctx,
		objectPath,
		taskId)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:NewMultipartUploadWithSignedUrl failed.",
			zap.String("objectPath", objectPath),
			zap.Int32("taskId", taskId),
			zap.Error(err))
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
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:sliceFile failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:prepareUpload finish.")
	return err
}

func (o *JCSProxy) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *JCSUploadCheckpoint) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:sliceFile start.",
		zap.Int64("partSize", partSize),
		zap.Int64("fileSize", ufc.FileInfo.Size))
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
		ErrorLogger.WithContext(ctx).Error(
			"upload file part too large.",
			zap.Int64("partSize", partSize),
			zap.Int64("maxPartSize", DefaultJCSMaxPartSize))
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
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:sliceFile finish.")
	return nil
}

func (o *JCSProxy) handleUploadTaskResult(
	ctx context.Context,
	result interface{},
	ufc *JCSUploadCheckpoint,
	partNum int32,
	enableCheckpoint bool,
	checkpointFilePath string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:handleUploadTaskResult start.",
		zap.String("checkpointFilePath", checkpointFilePath),
		zap.Int32("partNum", partNum))

	if _, ok := result.(*JCSBaseResponse); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"JCSProxy:updateCheckpointFile failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Int32("partNum", partNum),
					zap.Error(_err))
			}
		}
	} else if result != ErrAbort {
		if _err, ok := result.(error); ok {
			ErrorLogger.WithContext(ctx).Error(
				"upload task result failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Int32("partNum", partNum),
				zap.Error(_err))
			err = _err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:handleUploadTaskResult finish.")
	return
}

func (o *JCSProxy) completeParts(
	ctx context.Context,
	taskId int32,
	ufc *JCSUploadCheckpoint,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:completeParts start.",
		zap.Int32("objectId", ufc.ObjectId),
		zap.Int32("taskId", taskId),
		zap.String("checkpointFilePath", checkpointFilePath))

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
					ErrorLogger.WithContext(ctx).Error(
						"os.Remove failed.",
						zap.String("checkpointFilePath",
							checkpointFilePath),
						zap.Error(_err))
				}
			}
		}
		InfoLogger.WithContext(ctx).Debug(
			"JCSProxy:CompleteMultipartUploadWithSignedUrl finish.")
		return err
	}
	ErrorLogger.WithContext(ctx).Error(
		"JCSProxy.CompleteMultipartUpload failed.",
		zap.Int32("objectId", ufc.ObjectId),
		zap.Int32("taskId", taskId),
		zap.Error(err))
	return err
}

type JCSProxyUploadPartTask struct {
	ObjectId         int32
	ObjectPath       string
	PartNumber       int32
	SourceFile       string
	Offset           int64
	PartSize         int64
	JcsProxyClient   *JCSProxyClient
	EnableCheckpoint bool
}

func (task *JCSProxyUploadPartTask) Run(
	ctx context.Context,
	sourceFile string,
	taskId int32) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyUploadPartTask:Run start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", task.ObjectPath),
		zap.Int32("partNumber", task.PartNumber),
		zap.Int32("taskId", taskId))

	fd, err := os.Open(sourceFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Open failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("objectPath", task.ObjectPath),
			zap.Int32("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			ErrorLogger.WithContext(ctx).Warn(
				"close file failed.",
				zap.String("sourceFile", sourceFile),
				zap.String("objectPath", task.ObjectPath),
				zap.Int32("partNumber", task.PartNumber),
				zap.Error(errMsg))
		}
	}()

	readerWrapper := new(ReaderWrapper)
	readerWrapper.Reader = fd

	readerWrapper.TotalCount = task.PartSize
	readerWrapper.Mark = task.Offset
	if _, err = fd.Seek(task.Offset, io.SeekStart); nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"fd.Seek failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
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
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateJCSPreSignedObjectUploadPart failed.",
			zap.Int32("objectId", task.ObjectId),
			zap.String("objectPath", task.ObjectPath),
			zap.Int32("partNumber", task.PartNumber),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return err
	}

	err, uploadPartOutput := task.JcsProxyClient.UploadPartWithSignedUrl(
		ctx,
		createUploadPartSignedUrlResp.SignedUrl,
		readerWrapper)

	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JcsProxyClient.UploadPartWithSignedUrl failed.",
			zap.String("signedUrl",
				createUploadPartSignedUrlResp.SignedUrl),
			zap.Int32("objectId", task.ObjectId),
			zap.String("objectPath", task.ObjectPath),
			zap.Int32("partNumber", task.PartNumber),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyUploadPartTask:Run finish.",
		zap.String("objectPath", task.ObjectPath),
		zap.Int32("partNumber", task.PartNumber))
	return uploadPartOutput
}

func (o *JCSProxy) Download(
	ctx context.Context,
	userId string,
	targetPath string,
	taskId int32,
	bucketName string) (err error) {

	if '/' != targetPath[len(targetPath)-1] {
		targetPath = targetPath + "/"
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:Download start.",
		zap.String("userId", userId),
		zap.String("targetPath", targetPath),
		zap.Int32("taskId", taskId),
		zap.String("bucketName", bucketName))

	getTaskReq := new(GetTaskReq)
	getTaskReq.UserId = userId
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		ErrorLogger.WithContext(ctx).Error(
			"task not exist.",
			zap.Int32("taskId", taskId))
		return errors.New("task not exist")
	}
	task := getTaskResp.Data.List[0].Task
	downloadObjectTaskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadObjectTaskParams)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"DownloadObjectTaskParams Unmarshal failed.",
			zap.Int32("taskId", taskId),
			zap.String("params", task.Params),
			zap.Error(err))
		return err
	}

	uuid := downloadObjectTaskParams.Request.ObjUuid
	downloadFolderRecord := targetPath +
		uuid +
		fmt.Sprintf("_%d_tmp", taskId) +
		DownloadFolderRecordSuffix
	tmpTargetPath := targetPath + uuid + fmt.Sprintf("_%d_tmp/", taskId)

	continuationToken := ""
	for {
		listObjectsData, err := o.ListObjectsWithSignedUrl(
			ctx,
			taskId,
			continuationToken)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy:ListObjectsWithSignedUrl start.",
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return err
		}
		err = o.downloadObjects(
			ctx,
			userId,
			tmpTargetPath,
			taskId,
			listObjectsData,
			downloadFolderRecord)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy:downloadObjects failed.",
				zap.String("userId", userId),
				zap.String("tmpTargetPath", tmpTargetPath),
				zap.Int32("taskId", taskId),
				zap.String("bucketName", bucketName),
				zap.String("downloadFolderRecord", downloadFolderRecord),
				zap.Error(err))
			return err
		}
		if listObjectsData.IsTruncated {
			continuationToken = listObjectsData.NextContinuationToken
		} else {
			break
		}
	}

	fromPath := tmpTargetPath
	toPath := targetPath + uuid + fmt.Sprintf("_%d", taskId)
	err = os.Rename(fromPath, toPath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Rename failed.",
			zap.String("fromPath", fromPath),
			zap.String("toPath", toPath),
			zap.Error(err))
		return err
	}

	err = os.Remove(downloadFolderRecord)
	if nil != err {
		if !os.IsNotExist(err) {
			ErrorLogger.WithContext(ctx).Error(
				"os.Remove failed.",
				zap.String("downloadFolderRecord", downloadFolderRecord),
				zap.Error(err))
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:Download finish.")
	return nil
}

func (o *JCSProxy) downloadObjects(
	ctx context.Context,
	userId string,
	targetPath string,
	taskId int32,
	listObjectsData *JCSListData,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:downloadObjects start.",
		zap.String("userId", userId),
		zap.String("targetPath", targetPath),
		zap.Int32("taskId", taskId),
		zap.String("downloadFolderRecord", downloadFolderRecord))

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	fileData, err := os.ReadFile(downloadFolderRecord)
	if nil == err {
		lines := strings.Split(string(fileData), "\n")
		for _, line := range lines {
			fileMap[strings.TrimSuffix(line, "\r")] = 0
		}
	} else if !os.IsNotExist(err) {
		ErrorLogger.WithContext(ctx).Error(
			"os.ReadFile failed.",
			zap.String("downloadFolderRecord", downloadFolderRecord),
			zap.Error(err))
		return err
	}

	var isAllSuccess = true
	var wg sync.WaitGroup
	pool, err := ants.NewPool(o.jcsDownloadFileTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool for download Object failed.",
			zap.Error(err))
		return err
	}
	defer pool.Release()
	for index, object := range listObjectsData.Objects {
		InfoLogger.WithContext(ctx).Debug(
			"object content.",
			zap.Int("index", index),
			zap.Int32("objectID", object.ObjectID),
			zap.Int32("packageID", object.PackageID),
			zap.String("path", object.Path),
			zap.String("size", object.Size))
		itemObject := object
		if _, exists := fileMap[itemObject.Path]; exists {
			InfoLogger.WithContext(ctx).Info(
				"file already success.",
				zap.String("path", itemObject.Path))
			continue
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.jcsRateLimiter.Wait(ctxRate)
		if nil != err {
			cancel()
			ErrorLogger.WithContext(ctx).Error(
				"RateLimiter.Wait failed.",
				zap.Error(err))
			return err
		}
		cancel()
		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait end.")

		wg.Add(1)
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
				if _err := recover(); nil != _err {
					ErrorLogger.WithContext(ctx).Error(
						"downloadFile failed.",
						zap.Any("error", _err))
					isAllSuccess = false
				}
			}()

			if "0" == itemObject.Size &&
				'/' == itemObject.Path[len(itemObject.Path)-1] {

				itemPath := targetPath + itemObject.Path
				_err := os.MkdirAll(itemPath, os.ModePerm)
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"os.MkdirAll failed.",
						zap.String("itemPath", itemPath),
						zap.Error(_err))
					return
				}
			} else {
				targetFile := targetPath + itemObject.Path
				_err := o.downloadPartWithSignedUrl(
					ctx,
					itemObject,
					targetFile,
					taskId)
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"JCSProxy:downloadPartWithSignedUrl failed.",
						zap.String("objectPath", itemObject.Path),
						zap.String("targetFile", targetFile),
						zap.Int32("taskId", taskId),
						zap.Error(_err))
					return
				}
			}
			fileMutex.Lock()
			defer fileMutex.Unlock()
			f, _err := os.OpenFile(
				downloadFolderRecord,
				os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if nil != _err {
				isAllSuccess = false
				ErrorLogger.WithContext(ctx).Error(
					"os.OpenFile failed.",
					zap.String("downloadFolderRecord",
						downloadFolderRecord),
					zap.Error(_err))
				return
			}
			defer func() {
				errMsg := f.Close()
				if errMsg != nil {
					ErrorLogger.WithContext(ctx).Warn(
						"close file failed.",
						zap.String("downloadFolderRecord",
							downloadFolderRecord),
						zap.Error(errMsg))
				}
			}()
			_, _err = f.Write([]byte(itemObject.Path + "\n"))
			if nil != _err {
				isAllSuccess = false
				ErrorLogger.WithContext(ctx).Error(
					"write file failed.",
					zap.String("downloadFolderRecord",
						downloadFolderRecord),
					zap.String("objectKey", itemObject.Path),
					zap.Error(_err))
				return
			}
		})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"ants.Submit failed.",
				zap.Error(err))
			return err
		}
	}
	wg.Wait()
	if !isAllSuccess {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:downloadObjects not all success.")
		return errors.New("downloadObjects not all success")
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:downloadObjects finish.")
	return nil
}

func (o *JCSProxy) downloadPartWithSignedUrl(
	ctx context.Context,
	object *JCSObject,
	targetFile string,
	taskId int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:downloadPartWithSignedUrl start.",
		zap.Int32("objectID", object.ObjectID),
		zap.Int32("packageID", object.PackageID),
		zap.String("path", object.Path),
		zap.String("targetFile", targetFile),
		zap.Int32("taskId", taskId))

	downloadFileInput := new(JCSDownloadFileInput)
	downloadFileInput.Path = object.Path
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + DownloadFileRecordSuffix
	downloadFileInput.TaskNum = o.jcsDownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize

	err = o.resumeDownload(
		ctx,
		taskId,
		object,
		downloadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:resumeDownload failed.",
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:downloadPartWithSignedUrl finish.")
	return
}

func (o *JCSProxy) resumeDownload(
	ctx context.Context,
	taskId int32,
	object *JCSObject,
	input *JCSDownloadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:resumeDownload start.",
		zap.Int32("taskId", taskId))

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
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy:getDownloadCheckpointFile failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(err))
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
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy:prepareTempFile failed.",
				zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
				zap.Int64("Size", dfc.TempFileInfo.Size),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"JCSProxy:updateCheckpointFile failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Error(err))
				_errMsg := os.Remove(dfc.TempFileInfo.TempFileUrl)
				if _errMsg != nil {
					if !os.IsNotExist(_errMsg) {
						ErrorLogger.WithContext(ctx).Error(
							"os.Remove failed.",
							zap.String("TempFileUrl",
								dfc.TempFileInfo.TempFileUrl),
							zap.Error(_errMsg))
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
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:handleDownloadFileResult failed.",
			zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
			zap.Error(err))
		return err
	}

	err = os.Rename(dfc.TempFileInfo.TempFileUrl, input.DownloadFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Rename failed.",
			zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
			zap.String("DownloadFile", input.DownloadFile),
			zap.Error(err))
		return err
	}
	if enableCheckpoint {
		_err := os.Remove(checkpointFilePath)
		if nil != _err {
			if !os.IsNotExist(_err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Error(_err))
			}
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:resumeDownload finish.")
	return nil
}

func (o *JCSProxy) downloadFileConcurrent(
	ctx context.Context,
	taskId int32,
	input *JCSDownloadFileInput,
	dfc *JCSDownloadCheckpoint) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:downloadFileConcurrent start.",
		zap.Int32("taskId", taskId))

	var wg sync.WaitGroup
	pool, err := ants.NewPool(input.TaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			zap.Error(err))
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
		task := JCSProxyDownloadPartTask{
			ObjectId:         dfc.ObjectId,
			Offset:           downloadPart.Offset,
			Length:           downloadPart.Length,
			JcsProxyClient:   o.jcsProxyClient,
			JcsProxy:         o,
			PartNumber:       downloadPart.PartNumber,
			TempFileURL:      dfc.TempFileInfo.TempFileUrl,
			EnableCheckpoint: input.EnableCheckpoint,
		}
		InfoLogger.WithContext(ctx).Debug(
			"DownloadPartTask params.",
			zap.Int32("objectId", dfc.ObjectId),
			zap.Int64("offset", downloadPart.Offset),
			zap.Int64("length", downloadPart.Length),
			zap.Int64("partNumber", downloadPart.PartNumber),
			zap.String("tempFileUrl", dfc.TempFileInfo.TempFileUrl),
			zap.Bool("enableCheckpoint", input.EnableCheckpoint))

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.jcsRateLimiter.Wait(ctxRate)
		if nil != err {
			cancel()
			ErrorLogger.WithContext(ctx).Error(
				"RateLimiter.Wait failed.",
				zap.Error(err))
			return err
		}
		cancel()
		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait end.")

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
					_err := o.updateCheckpointFile(
						ctx,
						dfc,
						input.CheckpointFile)
					if nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"JCSProxy:updateCheckpointFile failed.",
							zap.String("checkpointFile",
								input.CheckpointFile),
							zap.Error(_err))
						downloadPartError.Store(_err)
					}
				}
				return
			} else {
				result := task.Run(ctx, taskId)
				_err := o.handleDownloadTaskResult(
					ctx,
					result,
					dfc,
					task.PartNumber,
					input.EnableCheckpoint,
					input.CheckpointFile,
					lock)
				if nil != _err &&
					atomic.CompareAndSwapInt32(&errFlag, 0, 1) {

					ErrorLogger.WithContext(ctx).Error(
						"JCSProxy:handleDownloadTaskResult failed.",
						zap.Int64("partNumber", task.PartNumber),
						zap.String("checkpointFile", input.CheckpointFile),
						zap.Error(_err))
					downloadPartError.Store(_err)
				}
				return
			}
		})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"ants.Submit failed.",
				zap.Error(err))
			return err
		}
	}
	wg.Wait()
	if err, ok := downloadPartError.Load().(error); ok {
		ErrorLogger.WithContext(ctx).Error(
			"downloadPartError failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:downloadFileConcurrent finish.")
	return nil
}

func (o *JCSProxy) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *JCSDownloadCheckpoint,
	input *JCSDownloadFileInput,
	object *JCSObject) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:getDownloadCheckpointFile start.",
		zap.String("checkpointFile", input.CheckpointFile))

	checkpointFilePath := input.CheckpointFile
	checkpointFileStat, err := os.Stat(checkpointFilePath)
	if nil != err {
		if !os.IsNotExist(err) {
			ErrorLogger.WithContext(ctx).Error(
				"os.Stat failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(err))
			return false, err
		}
		InfoLogger.WithContext(ctx).Debug(
			"checkpointFilePath not exist.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return true, nil
	}
	if checkpointFileStat.IsDir() {
		ErrorLogger.WithContext(ctx).Error(
			"checkpointFilePath can not be a folder.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return false,
			errors.New("checkpoint file can not be a folder")
	}
	err = o.loadCheckpointFile(ctx, checkpointFilePath, dfc)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:loadCheckpointFile failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return true, nil
	} else if !dfc.IsValid(ctx, input, object) {
		if dfc.TempFileInfo.TempFileUrl != "" {
			_err := os.Remove(dfc.TempFileInfo.TempFileUrl)
			if nil != _err {
				if !os.IsNotExist(_err) {
					ErrorLogger.WithContext(ctx).Error(
						"os.Remove failed.",
						zap.String("TempFileUrl",
							dfc.TempFileInfo.TempFileUrl),
						zap.Error(_err))
				}
			}
		}
		_err := os.Remove(checkpointFilePath)
		if nil != _err {
			if !os.IsNotExist(_err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Error(_err))
			}
		}
	} else {
		InfoLogger.WithContext(ctx).Debug(
			"no need to check point.")
		return false, nil
	}
	InfoLogger.WithContext(ctx).Debug(
		"need to check point.")
	return true, nil
}

func (o *JCSProxy) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:prepareTempFile start.",
		zap.String("tempFileURL", tempFileURL),
		zap.Int64("fileSize", fileSize))

	parentDir := filepath.Dir(tempFileURL)
	stat, err := os.Stat(parentDir)
	if nil != err {
		if !os.IsNotExist(err) {
			ErrorLogger.WithContext(ctx).Error(
				"os.Stat failed.",
				zap.String("parentDir", parentDir),
				zap.Error(err))
			return err
		}
		InfoLogger.WithContext(ctx).Debug(
			"parentDir not exist.",
			zap.String("parentDir", parentDir))

		_err := os.MkdirAll(parentDir, os.ModePerm)
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"os.MkdirAll failed.",
				zap.String("parentDir", parentDir),
				zap.Error(_err))
			return _err
		}
	} else if !stat.IsDir() {
		ErrorLogger.WithContext(ctx).Error(
			"same file exists.",
			zap.String("parentDir", parentDir))
		return fmt.Errorf(
			"cannot create folder: %s due to a same file exists",
			parentDir)
	}

	err = o.createFile(ctx, tempFileURL, fileSize)
	if nil == err {
		InfoLogger.WithContext(ctx).Debug(
			"JCSProxy:createFile finish.",
			zap.String("tempFileURL", tempFileURL),
			zap.Int64("fileSize", fileSize))
		return nil
	}
	fd, err := os.OpenFile(
		tempFileURL,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0640)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.OpenFile failed.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(err))
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			ErrorLogger.WithContext(ctx).Warn(
				"close file failed.",
				zap.String("tempFileURL", tempFileURL),
				zap.Error(errMsg))
		}
	}()
	if fileSize > 0 {
		_, err = fd.WriteAt([]byte("a"), fileSize-1)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"write file failed.",
				zap.String("tempFileURL", tempFileURL),
				zap.Error(err))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:prepareTempFile finish.")
	return nil
}

func (o *JCSProxy) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:createFile start.",
		zap.String("tempFileURL", tempFileURL),
		zap.Int64("fileSize", fileSize))

	fd, err := syscall.Open(
		tempFileURL,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0640)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"syscall.Open failed.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(err))
		return err
	}
	defer func() {
		errMsg := syscall.Close(fd)
		if errMsg != nil {
			ErrorLogger.WithContext(ctx).Warn(
				"syscall.Close failed.",
				zap.String("tempFileURL", tempFileURL),
				zap.Error(errMsg))
		}
	}()
	err = syscall.Ftruncate(fd, fileSize)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"syscall.Ftruncate failed.",
			zap.String("tempFileURL", tempFileURL),
			zap.Int64("fileSize", fileSize),
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:createFile finish.")
	return nil
}

func (o *JCSProxy) handleDownloadTaskResult(
	ctx context.Context,
	result interface{},
	dfc *JCSDownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:handleDownloadTaskResult start.",
		zap.Int64("partNum", partNum),
		zap.String("checkpointFile", checkpointFile))

	if _, ok := result.(*JCSDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Warn(
					"JCSProxy:updateCheckpointFile failed.",
					zap.String("checkpointFile", checkpointFile),
					zap.Error(_err))
			}
		}
	} else if result != ErrAbort {
		if _err, ok := result.(error); ok {
			err = _err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:handleDownloadTaskResult finish.")
	return
}

func (o *JCSProxy) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:handleDownloadFileResult start.",
		zap.String("tempFileURL", tempFileURL))

	if downloadFileError != nil {
		if !enableCheckpoint {
			_err := os.Remove(tempFileURL)
			if nil != _err {
				if !os.IsNotExist(_err) {
					ErrorLogger.WithContext(ctx).Error(
						"os.Remove failed.",
						zap.String("tempFileURL", tempFileURL),
						zap.Error(_err))
				}
			}
		}
		InfoLogger.WithContext(ctx).Debug(
			"JCSProxy.handleDownloadFileResult finish.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(downloadFileError))
		return downloadFileError
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy.handleDownloadFileResult finish.")
	return nil
}

func (o *JCSProxy) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *JCSDownloadPartOutput) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:UpdateDownloadFile start.",
		zap.String("filePath", filePath),
		zap.Int64("offset", offset))

	fd, err := os.OpenFile(filePath, os.O_WRONLY, 0640)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.OpenFile failed.",
			zap.String("filePath", filePath),
			zap.Error(err))
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			ErrorLogger.WithContext(ctx).Warn(
				"close file failed.",
				zap.String("filePath", filePath),
				zap.Error(errMsg))
		}
	}()
	_, err = fd.Seek(offset, 0)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"seek file failed.",
			zap.String("filePath", filePath),
			zap.Int64("offset", offset),
			zap.Error(err))
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
				ErrorLogger.WithContext(ctx).Error(
					"write file failed.",
					zap.String("filePath", filePath),
					zap.Error(writeError))
				return writeError
			}
			if writeCount != readCount {
				ErrorLogger.WithContext(ctx).Error(
					" write file failed.",
					zap.String("filePath", filePath),
					zap.Int("readCount", readCount),
					zap.Int("writeCount", writeCount))
				return fmt.Errorf("failed to write to file."+
					" filePath: %s, expect: %d, actual: %d",
					filePath, readCount, writeCount)
			}
			readTotal = readTotal + readCount
		}
		if readErr != nil {
			if readErr != io.EOF {
				ErrorLogger.WithContext(ctx).Error(
					"read response body failed.",
					zap.Error(readErr))
				return readErr
			}
			break
		}
	}
	err = fileWriter.Flush()
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"flush file failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxy:UpdateDownloadFile finish.",
		zap.Int("readTotal", readTotal))
	return nil
}

type JCSProxyDownloadPartTask struct {
	ObjectId         int32
	Offset           int64
	Length           int64
	JcsProxyClient   *JCSProxyClient
	JcsProxy         *JCSProxy
	PartNumber       int64
	TempFileURL      string
	EnableCheckpoint bool
}

func (task *JCSProxyDownloadPartTask) Run(
	ctx context.Context,
	taskId int32) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyDownloadPartTask:Run start.",
		zap.Int32("taskId", taskId),
		zap.Int32("objectId", task.ObjectId),
		zap.Int64("partNumber", task.PartNumber))

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
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateGetObjectSignedUrl failed.",
			zap.Int32("objectId", task.ObjectId),
			zap.Int64("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	}

	err, downloadPartOutput :=
		task.JcsProxyClient.DownloadPartWithSignedUrl(
			ctx,
			createJCSPreSignedObjectDownloadResp.SignedUrl)

	if nil == err {
		InfoLogger.WithContext(ctx).Debug(
			"JcsProxyClient.DownloadPartWithSignedUrl finish.",
			zap.Int32("objectId", task.ObjectId),
			zap.Int64("partNumber", task.PartNumber))
		defer func() {
			errMsg := downloadPartOutput.Body.Close()
			if errMsg != nil {
				ErrorLogger.WithContext(ctx).Warn(
					"close response body failed.",
					zap.Int32("objectId", task.ObjectId),
					zap.Int64("partNumber", task.PartNumber))
			}
		}()
		_err := task.JcsProxy.UpdateDownloadFile(
			ctx,
			task.TempFileURL,
			task.Offset,
			downloadPartOutput)
		if nil != _err {
			if !task.EnableCheckpoint {
				ErrorLogger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					zap.Int32("objectId", task.ObjectId),
					zap.Int64("partNumber", task.PartNumber))
			}
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy.updateDownloadFile failed.",
				zap.Int32("objectId", task.ObjectId),
				zap.Int64("partNumber", task.PartNumber),
				zap.Error(_err))
			return _err
		}
		InfoLogger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.",
			zap.Int32("objectId", task.ObjectId),
			zap.Int64("partNumber", task.PartNumber))
		return downloadPartOutput
	}

	ErrorLogger.WithContext(ctx).Error(
		"JCSProxyDownloadPartTask:Run failed.",
		zap.String("signedUrl",
			createJCSPreSignedObjectDownloadResp.SignedUrl),
		zap.Int32("objectId", task.ObjectId),
		zap.Int64("partNumber", task.PartNumber),
		zap.Error(err))
	return err
}
