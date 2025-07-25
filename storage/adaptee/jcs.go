package adaptee

import (
	"bufio"
	"context"
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

type JCS struct {
	jcsClient               *JCSClient
	jcsUploadFileTaskNum    int
	jcsUploadMultiTaskNum   int
	jcsDownloadFileTaskNum  int
	jcsDownloadMultiTaskNum int
	jcsRateLimiter          *rate.Limiter
}

func (o *JCS) Init(
	ctx context.Context,
	accessKey,
	secretKey,
	endPoint,
	authService,
	authRegion string,
	userID,
	bucketID int32,
	bucketName string,
	reqTimeout,
	maxConnection int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:Init start.",
		zap.String("endPoint", endPoint),
		zap.String("authService", authService),
		zap.String("authRegion", authRegion),
		zap.Int32("userId", userID),
		zap.Int32("bucketID", bucketID),
		zap.String("bucketName", bucketName),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

	o.jcsClient = new(JCSClient)
	o.jcsClient.Init(
		ctx,
		accessKey,
		secretKey,
		endPoint,
		authService,
		authRegion,
		userID,
		bucketID,
		bucketName,
		reqTimeout,
		maxConnection)

	o.jcsUploadFileTaskNum = DefaultJCSUploadFileTaskNum
	o.jcsUploadMultiTaskNum = DefaultJCSUploadMultiTaskNum
	o.jcsDownloadFileTaskNum = DefaultJCSDownloadFileTaskNum
	o.jcsDownloadMultiTaskNum = DefaultJCSDownloadMultiTaskNum

	o.jcsRateLimiter = rate.NewLimiter(
		DefaultJCSRateLimit,
		DefaultJCSRateBurst)

	InfoLogger.WithContext(ctx).Debug(
		"JCS:Init finish.")
	return nil
}

func (o *JCS) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("UploadMultiTaskNum", config.UploadMultiTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	o.jcsUploadFileTaskNum = int(config.UploadFileTaskNum)
	o.jcsUploadMultiTaskNum = int(config.UploadMultiTaskNum)
	o.jcsDownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.jcsDownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	InfoLogger.WithContext(ctx).Debug(
		"JCS:SetConcurrency finish.")
	return nil
}

func (o *JCS) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:SetRate start.")

	o.jcsRateLimiter = rateLimiter

	InfoLogger.WithContext(ctx).Debug(
		"JCS:SetRate finish.")
	return nil
}

func (o *JCS) Mkdir(
	ctx context.Context,
	input interface{}) (err error) {

	var packageId int32
	var path string

	if jcsMkdirInput, ok := input.(JCSMkdirInput); ok {
		packageId = jcsMkdirInput.PackageId
		path = jcsMkdirInput.Path
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:Mkdir start.",
		zap.Int32("packageId", packageId),
		zap.String("path", path))

	err = o.jcsClient.UploadFile(
		ctx,
		packageId,
		path,
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.UploadFile mkdir failed.",
			zap.Int32("packageId", packageId),
			zap.String("path", path),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:Mkdir finish.")
	return nil
}

func (o *JCS) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:loadCheckpointFile start.",
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
		"JCS:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *JCS) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *JCSDownloadCheckpoint) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:sliceObject start.",
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
		"JCS:sliceObject finish.")
}

func (o *JCS) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:updateCheckpointFile start.",
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
		"JCS:updateCheckpointFile finish.")
	return err
}

func (o *JCS) Upload(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	var packageId int32
	var needPure bool
	if jcsUploadInput, ok := input.(JCSUploadInput); ok {
		sourcePath = jcsUploadInput.SourcePath
		targetPath = jcsUploadInput.TargetPath
		packageId = jcsUploadInput.PackageId
		needPure = jcsUploadInput.NeedPure
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:Upload start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
		zap.Int32("packageId", packageId),
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
		err = o.uploadFolder(
			ctx,
			packageId,
			sourcePath,
			targetPath,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCS.uploadFolder failed.",
				zap.Int32("packageId", packageId),
				zap.String("sourcePath", sourcePath),
				zap.String("targetPath", targetPath),
				zap.Error(err))
			return err
		}
	} else {
		objectPath := targetPath + filepath.Base(sourcePath)
		err = o.uploadFile(
			ctx,
			packageId,
			sourcePath,
			objectPath,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCS.uploadFile failed.",
				zap.Int32("packageId", packageId),
				zap.String("sourcePath", sourcePath),
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCS:Upload finish.")
	return nil
}

func (o *JCS) uploadFolder(
	ctx context.Context,
	packageId int32,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:uploadFolder start.",
		zap.Int32("packageId", packageId),
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
		zap.Bool("needPure", needPure))

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	uploadFolderRecord :=
		strings.TrimSuffix(sourcePath, "/") + UploadFolderRecordSuffix
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

			wg.Add(1)
			err = pool.Submit(func() {
				defer func() {
					wg.Done()
					if _err := recover(); nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"JCS:uploadFileResume failed.",
							zap.Any("error", _err))
						isAllSuccess = false
					}
				}()
				relPath, _err := filepath.Rel(
					filepath.Dir(sourcePath),
					filePath)
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"filepath.Rel failed.",
						zap.String("sourcePath", sourcePath),
						zap.String("filePath", filePath),
						zap.String("relPath", relPath),
						zap.Error(_err))
					return
				}
				objectPath := targetPath + relPath
				if strings.HasSuffix(objectPath, UploadFileRecordSuffix) {
					InfoLogger.WithContext(ctx).Info(
						"upload record file.",
						zap.String("objectPath", objectPath))
					return
				}
				if _, exists := fileMap[objectPath]; exists {
					InfoLogger.WithContext(ctx).Info(
						"already finish.",
						zap.String("objectPath", objectPath))
					return
				}

				InfoLogger.WithContext(ctx).Debug(
					"RateLimiter.Wait start.")
				ctxRate, cancel := context.WithCancel(context.Background())
				_err = o.jcsRateLimiter.Wait(ctxRate)
				if nil != _err {
					isAllSuccess = false
					cancel()
					ErrorLogger.WithContext(ctx).Error(
						"RateLimiter.Wait failed.",
						zap.Error(_err))
					return
				}
				cancel()
				InfoLogger.WithContext(ctx).Debug(
					"RateLimiter.Wait end.")

				if fileInfo.IsDir() {
					if 0 < len(objectPath) &&
						'/' != objectPath[len(objectPath)-1] {

						objectPath = objectPath + "/"
					}

					input := JCSMkdirInput{}
					input.PackageId = packageId
					input.Path = objectPath
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"JCS.Mkdir failed.",
							zap.Int32("packageId", packageId),
							zap.String("objectPath", objectPath),
							zap.Error(_err))
						return
					}
				} else {
					_err = o.uploadFile(
						ctx,
						packageId,
						filePath,
						objectPath,
						needPure)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"JCS:uploadFile failed.",
							zap.Int32("packageId", packageId),
							zap.String("filePath", filePath),
							zap.String("objectPath", objectPath),
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
				_, _err = f.Write([]byte(objectPath + "\n"))
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"write file failed.",
						zap.String("uploadFolderRecord",
							uploadFolderRecord),
						zap.String("objectPath", objectPath),
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
			"JCS:uploadFolder not all success.",
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
		"JCS:uploadFolder finish.")
	return nil
}

func (o *JCS) uploadFile(
	ctx context.Context,
	packageId int32,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:uploadFile start.",
		zap.Int32("packageId", packageId),
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
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
			packageId,
			sourceFile,
			objectPath,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCS:uploadFileResume failed.",
				zap.Int32("packageId", packageId),
				zap.String("sourceFile", sourceFile),
				zap.String("objectPath", objectPath),
				zap.Bool("needPure", needPure),
				zap.Error(err))
			return err
		}
	} else {
		err = o.uploadFileStream(
			ctx,
			packageId,
			sourceFile,
			objectPath)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCS:uploadFileStream failed.",
				zap.Int32("packageId", packageId),
				zap.String("sourceFile", sourceFile),
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:uploadFile finish.")
	return err
}

func (o *JCS) uploadFileStream(
	ctx context.Context,
	packageId int32,
	sourceFile,
	objectPath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:uploadFileStream start.",
		zap.Int32("packageId", packageId),
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath))

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

	err = o.jcsClient.UploadFile(
		ctx,
		packageId,
		objectPath,
		fd)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.UploadFile failed.",
			zap.Int32("packageId", packageId),
			zap.String("objectPath", objectPath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:uploadFileStream finish.")
	return err
}

func (o *JCS) uploadFileResume(
	ctx context.Context,
	packageId int32,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:uploadFileResume start.",
		zap.Int32("packageId", packageId),
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
		zap.Bool("needPure", needPure))

	uploadFileInput := new(JCSUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + UploadFileRecordSuffix
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
		packageId,
		sourceFile,
		objectPath,
		uploadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCS:resumeUpload failed.",
			zap.Int32("packageId", packageId),
			zap.String("sourceFile", sourceFile),
			zap.String("objectPath", objectPath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:uploadFileResume finish.")
	return err
}

func (o *JCS) resumeUpload(
	ctx context.Context,
	packageId int32,
	sourceFile,
	objectPath string,
	input *JCSUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:resumeUpload start.",
		zap.Int32("packageId", packageId),
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath))

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
				"JCS:getUploadCheckpointFile failed.",
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}
	if needCheckpoint {
		err = o.prepareUpload(
			ctx,
			packageId,
			objectPath,
			ufc,
			uploadFileStat,
			input)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCS:prepareUpload failed.",
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"JCS:updateCheckpointFile failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Error(err))
				return err
			}
		}
	}

	err = o.uploadPartConcurrent(
		ctx,
		sourceFile,
		ufc,
		input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCS:uploadPartConcurrent failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return err
	}

	err = o.completeParts(
		ctx,
		ufc,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCS:completeParts failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:resumeUpload finish.")
	return err
}

func (o *JCS) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	ufc *JCSUploadCheckpoint,
	input *JCSUploadFileInput) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:uploadPartConcurrent start.",
		zap.String("sourceFile", sourceFile))

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
		task := JCSUploadPartTask{
			ObjectId:         ufc.ObjectId,
			ObjectPath:       ufc.ObjectPath,
			PartNumber:       uploadPart.PartNumber,
			SourceFile:       input.UploadFile,
			Offset:           uploadPart.Offset,
			PartSize:         uploadPart.PartSize,
			JcsClient:        o.jcsClient,
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
			result := task.Run(ctx, sourceFile)
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
					"JCS:handleUploadTaskResult failed.",
					zap.Int32("partNumber", task.PartNumber),
					zap.String("checkpointFile", input.CheckpointFile),
					zap.Error(_err))
				uploadPartError.Store(_err)
			}
			InfoLogger.WithContext(ctx).Debug(
				"JCS:handleUploadTaskResult finish.")
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
		"JCS:uploadPartConcurrent finish.")
	return nil
}

func (o *JCS) getUploadCheckpointFile(
	ctx context.Context,
	ufc *JCSUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *JCSUploadFileInput) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:getUploadCheckpointFile start.")

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
			"JCS:loadCheckpointFile failed.",
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
			"JCS:loadCheckpointFile finish.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return false, nil
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCS:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *JCS) prepareUpload(
	ctx context.Context,
	packageId int32,
	objectPath string,
	ufc *JCSUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *JCSUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:prepareUpload start.",
		zap.Int32("packageId", packageId),
		zap.String("objectPath", objectPath))

	newMultipartUploadOutput, err := o.jcsClient.NewMultiPartUpload(
		ctx,
		packageId,
		objectPath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxy:NewMultipartUploadWithSignedUrl failed.",
			zap.Int32("packageId", packageId),
			zap.String("objectPath", objectPath),
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
			"JCS:sliceFile failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCS:prepareUpload finish.")
	return err
}

func (o *JCS) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *JCSUploadCheckpoint) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:sliceFile start.",
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

	InfoLogger.WithContext(ctx).Debug(
		"JCS:handleUploadTaskResult start.",
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
					"JCS:updateCheckpointFile failed.",
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
		"JCS:handleUploadTaskResult finish.")
	return
}

func (o *JCS) completeParts(
	ctx context.Context,
	ufc *JCSUploadCheckpoint,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:completeParts start.",
		zap.Bool("enableCheckpoint", enableCheckpoint),
		zap.String("checkpointFilePath", checkpointFilePath))

	parts := make([]int32, 0, len(ufc.UploadParts))
	for _, uploadPart := range ufc.UploadParts {
		parts = append(parts, uploadPart.PartNumber)
	}

	err = o.jcsClient.CompleteMultiPartUpload(
		ctx,
		ufc.ObjectId,
		parts)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.CompleteMultiPartUpload failed.",
			zap.Int32("ObjectId", ufc.ObjectId),
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
		"JCS:completeParts finish.")
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
	sourceFile string) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"JCSUploadPartTask:Run start.",
		zap.String("sourceFile", sourceFile),
		zap.Int32("partNumber", task.PartNumber))

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

	readerWrapper := new(ReaderWrapper)
	readerWrapper.Reader = fd

	readerWrapper.TotalCount = task.PartSize
	readerWrapper.Mark = task.Offset
	if _, err = fd.Seek(task.Offset, io.SeekStart); nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"fd.Seek failed.",
			zap.String("sourceFile", sourceFile),
			zap.Int32("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	}

	err, resp := task.JcsClient.UploadPart(
		ctx,
		task.ObjectId,
		task.PartNumber,
		task.ObjectPath,
		readerWrapper)

	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JcsClient.UploadPart failed.",
			zap.String("sourceFile", sourceFile),
			zap.Int32("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSUploadPartTask:Run finish.",
		zap.String("sourceFile", sourceFile),
		zap.Int32("partNumber", task.PartNumber))
	return resp
}

func (o *JCS) Download(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	var packageId int32
	if jcsDownloadInput, ok := input.(JCSDownloadInput); ok {
		sourcePath = jcsDownloadInput.SourcePath
		targetPath = jcsDownloadInput.TargetPath
		packageId = jcsDownloadInput.PackageId
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:Download start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
		zap.Int32("packageId", packageId))

	err = os.MkdirAll(targetPath, os.ModePerm)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.MkdirAll failed.",
			zap.String("targetPath", targetPath),
			zap.Error(err))
		return
	}

	downloadFolderRecord :=
		strings.TrimSuffix(targetPath, "/") + DownloadFolderRecordSuffix
	InfoLogger.WithContext(ctx).Debug(
		"downloadFolderRecord file info.",
		zap.String("downloadFolderRecord", downloadFolderRecord))

	continuationToken := ""
	for {
		listObjectsData := new(JCSListData)
		listObjectsData, err = o.jcsClient.List(
			ctx,
			packageId,
			sourcePath,
			true,
			false,
			DefaultJCSListLimit,
			continuationToken)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"jcsClient:List failed.",
				zap.Int32("packageId", packageId),
				zap.String("sourcePath", sourcePath),
				zap.String("continuationToken", continuationToken),
				zap.Error(err))
			return err
		}

		err = o.downloadObjects(
			ctx,
			sourcePath,
			targetPath,
			listObjectsData,
			downloadFolderRecord)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCS:downloadObjects failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("targetPath", targetPath),
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
		"JCS:Download finish.",
		zap.String("sourcePath", sourcePath))
	return nil
}

func (o *JCS) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	listObjectsData *JCSListData,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:downloadObjects start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
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
			"ants.NewPool for download object failed.",
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
				_err := o.downloadPart(
					ctx,
					itemObject,
					targetFile)
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"JCS:downloadPart failed.",
						zap.String("objectPath", itemObject.Path),
						zap.String("targetFile", targetFile),
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
					zap.String("objectPath", itemObject.Path),
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
			"JCS:downloadObjects not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("downloadObjects not all success")
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:downloadObjects finish.")
	return nil
}

func (o *JCS) downloadPart(
	ctx context.Context,
	object *JCSObject,
	targetFile string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:downloadPart start.",
		zap.String("objectPath", object.Path),
		zap.String("targetFile", targetFile))

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
		object,
		downloadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCS:resumeDownload failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"JCS:downloadPart finish.")
	return
}

func (o *JCS) resumeDownload(
	ctx context.Context,
	object *JCSObject,
	input *JCSDownloadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:resumeDownload start.")

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
				"JCS:getDownloadCheckpointFile failed.",
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
				"JCS:prepareTempFile failed.",
				zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
				zap.Int64("Size", dfc.TempFileInfo.Size),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"JCS:updateCheckpointFile failed.",
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

	downloadFileError := o.downloadFileConcurrent(ctx, input, dfc)
	err = o.handleDownloadFileResult(
		ctx,
		dfc.TempFileInfo.TempFileUrl,
		enableCheckpoint,
		downloadFileError)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCS:handleDownloadFileResult failed.",
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
		"JCS:resumeDownload finish.")
	return nil
}

func (o *JCS) downloadFileConcurrent(
	ctx context.Context,
	input *JCSDownloadFileInput,
	dfc *JCSDownloadCheckpoint) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:downloadFileConcurrent start.")

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
		InfoLogger.WithContext(ctx).Debug(
			"DownloadPartTask params.",
			zap.Int64("Offset", downloadPart.Offset),
			zap.Int64("Length", downloadPart.Length),
			zap.Int64("PartNumber", downloadPart.PartNumber),
			zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
			zap.Bool("EnableCheckpoint", input.EnableCheckpoint))

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
							"JCS:updateCheckpointFile failed.",
							zap.String("checkpointFile",
								input.CheckpointFile),
							zap.Error(_err))
						downloadPartError.Store(_err)
					}
				}
				return
			} else {
				result := task.Run(ctx)
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
						"JCS:handleDownloadTaskResult failed.",
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
		"JCS:downloadFileConcurrent finish.")
	return nil
}

func (o *JCS) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *JCSDownloadCheckpoint,
	input *JCSDownloadFileInput,
	object *JCSObject) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:getDownloadCheckpointFile start.",
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
			"JCS:loadCheckpointFile failed.",
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

func (o *JCS) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:prepareTempFile start.",
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
			"JCS:createFile finish.",
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
		"JCS:prepareTempFile finish.")
	return nil
}

func (o *JCS) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:createFile start.",
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

	InfoLogger.WithContext(ctx).Debug(
		"JCS:handleDownloadTaskResult start.",
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
					"JCS:updateCheckpointFile failed.",
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
		"JCS:handleDownloadTaskResult finish.")
	return
}

func (o *JCS) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:handleDownloadFileResult start.",
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
			"JCS.handleDownloadFileResult finish.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(downloadFileError))
		return downloadFileError
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS.handleDownloadFileResult finish.")
	return nil
}

func (o *JCS) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *JCSDownloadPartOutput) error {

	InfoLogger.WithContext(ctx).Debug(
		"JCS:UpdateDownloadFile start.",
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
		"JCS:UpdateDownloadFile finish.",
		zap.Int("readTotal", readTotal))
	return nil
}

func (o *JCS) Delete(
	ctx context.Context,
	input interface{}) (err error) {

	var packageId int32

	if jcsDeleteInput, ok := input.(JCSDeleteInput); ok {
		packageId = jcsDeleteInput.PackageId
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:Delete start.",
		zap.Int32("packageId", packageId))

	err = o.jcsClient.DeletePackage(ctx, packageId)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.DeletePackage failed.",
			zap.Int32("packageId", packageId),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCS:DeletePackage finish.")
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
	ctx context.Context) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"JCSDownloadPartTask:Run start.",
		zap.Int32("objectId", task.ObjectId),
		zap.Int64("partNumber", task.PartNumber))

	err, downloadPartOutput :=
		task.JcsClient.DownloadPart(
			ctx,
			task.ObjectId,
			task.Offset,
			task.Length)

	if nil == err {
		InfoLogger.WithContext(ctx).Debug(
			"JcsClient.DownloadPart finish.",
			zap.Int32("objectId", task.ObjectId),
			zap.Int64("partNumber", task.PartNumber))
		defer func() {
			errMsg := downloadPartOutput.Body.Close()
			if errMsg != nil {
				ErrorLogger.WithContext(ctx).Warn(
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
				ErrorLogger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					zap.Int32("objectId", task.ObjectId),
					zap.Int64("partNumber", task.PartNumber))
			}
			ErrorLogger.WithContext(ctx).Error(
				"JCS.updateDownloadFile failed.",
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
		"JCSDownloadPartTask:Run failed.",
		zap.Int32("objectId", task.ObjectId),
		zap.Int64("partNumber", task.PartNumber),
		zap.Error(err))
	return err
}
