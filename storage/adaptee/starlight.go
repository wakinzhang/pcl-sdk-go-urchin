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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type StarLight struct {
	slClient               *SLClient
	slUploadFileTaskNum    int
	slUploadMultiTaskNum   int //starlight并发度仅支持1，只能有一个端点
	slDownloadFileTaskNum  int
	slDownloadMultiTaskNum int
	slRateLimiter          *rate.Limiter
}

func (o *StarLight) Init(
	ctx context.Context,
	username,
	password,
	endpoint string,
	reqTimeout,
	maxConnection int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Init start.",
		zap.String("endpoint", endpoint),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

	o.slClient = new(SLClient)
	o.slClient.Init(
		ctx,
		username,
		password,
		endpoint,
		reqTimeout,
		maxConnection)

	o.slUploadFileTaskNum = DefaultSLUploadFileTaskNum
	//starlight并发度仅支持1，只能有一个端点
	o.slUploadMultiTaskNum = DefaultSLUploadMultiTaskNum
	o.slDownloadFileTaskNum = DefaultSLDownloadFileTaskNum
	o.slDownloadMultiTaskNum = DefaultSLDownloadMultiTaskNum

	o.slRateLimiter = rate.NewLimiter(
		DefaultSLRateLimit,
		DefaultSLRateBurst)

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Init finish.")
	return nil
}

func (o *StarLight) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	o.slUploadFileTaskNum = int(config.UploadFileTaskNum)
	o.slDownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.slDownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:SetConcurrency finish.")
	return nil
}

func (o *StarLight) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:SetRate start.")

	o.slRateLimiter = rateLimiter

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:SetRate finish.")
	return nil
}

func (o *StarLight) Mkdir(
	ctx context.Context,
	input interface{}) (err error) {

	var target string
	if starLightMkdirInput, ok := input.(StarLightMkdirInput); ok {
		target = starLightMkdirInput.Target
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Mkdir start.",
		zap.String("target", target))

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_err := o.slClient.Mkdir(ctx, target)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"slClient.Mkdir failed.",
					zap.String("target", target),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight.Mkdir failed.",
			zap.String("target", target),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Mkdir finish.")
	return nil
}

func (o *StarLight) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:loadCheckpointFile start.",
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
		"StarLight:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *StarLight) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *SLDownloadCheckpoint) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:sliceObject start.",
		zap.Int64("objectSize", objectSize),
		zap.Int64("partSize", partSize))

	cnt := objectSize / partSize
	if objectSize%partSize > 0 {
		cnt++
	}

	if cnt == 0 {
		downloadPart := SLDownloadPartInfo{}
		downloadPart.PartNumber = 1
		dfc.DownloadParts = []SLDownloadPartInfo{downloadPart}
	} else {
		downloadParts := make([]SLDownloadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			downloadPart := SLDownloadPartInfo{}
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
		"StarLight:sliceObject finish.")
}

func (o *StarLight) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:updateCheckpointFile start.",
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
		"StarLight:updateCheckpointFile finish.")
	return err
}

func (o *StarLight) Upload(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	var needPure bool
	if starLightUploadInput, ok := input.(StarLightUploadInput); ok {
		sourcePath = starLightUploadInput.SourcePath
		targetPath = starLightUploadInput.TargetPath
		needPure = starLightUploadInput.NeedPure
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Upload start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
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
		err = o.uploadFolder(ctx, sourcePath, targetPath, needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"StarLight.uploadFolder failed.",
				zap.String("sourcePath", sourcePath),
				zap.Error(err))
			return err
		}
	} else {
		objectPath := targetPath + filepath.Base(sourcePath)
		err = o.uploadFileResume(
			ctx,
			sourcePath,
			objectPath,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"StarLight.uploadFileResume failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Upload finish.")
	return nil
}

func (o *StarLight) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:uploadFolder start.",
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

	starLightMkdirInput := StarLightMkdirInput{}
	starLightMkdirInput.Target = targetPath
	err = o.Mkdir(ctx, starLightMkdirInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight.Mkdir failed.",
			zap.String("targetPath", targetPath),
			zap.Error(err))
		return err
	}

	pool, err := ants.NewPool(o.slUploadFileTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			zap.Error(err))
		return err
	}
	defer pool.Release()

	var isAllSuccess = true
	var dirWaitGroup sync.WaitGroup
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
			err = o.slRateLimiter.Wait(ctxRate)
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

			dirWaitGroup.Add(1)
			err = pool.Submit(func() {
				defer func() {
					dirWaitGroup.Done()
					if _err := recover(); nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"StarLight:uploadFileResume failed.",
							zap.Any("error", _err))
						isAllSuccess = false
					}
				}()

				if fileInfo.IsDir() {
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
					if _, exists := fileMap[objectPath]; exists {
						InfoLogger.WithContext(ctx).Info(
							"already finish.",
							zap.String("objectPath", objectPath))
						return
					}

					input := StarLightMkdirInput{}
					input.Target = objectPath
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"StarLight.Mkdir failed.",
							zap.String("objectPath", objectPath),
							zap.Error(_err))
						return
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
	dirWaitGroup.Wait()

	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"filepath.Walk failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}
	if !isAllSuccess {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:uploadFolder not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("uploadFolder not all success")
	}

	var fileWaitGroup sync.WaitGroup
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
			err = o.slRateLimiter.Wait(ctxRate)
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

			fileWaitGroup.Add(1)
			err = pool.Submit(func() {
				defer func() {
					fileWaitGroup.Done()
					if _err := recover(); nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"StarLight:uploadFileResume failed.",
							zap.Any("error", _err))
						isAllSuccess = false
					}
				}()

				if !fileInfo.IsDir() {
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
					_err = o.uploadFileResume(
						ctx,
						filePath,
						objectPath,
						needPure)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"StarLight:uploadFileResume failed.",
							zap.String("filePath", filePath),
							zap.String("objectPath", objectPath),
							zap.Error(_err))
						return
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
	fileWaitGroup.Wait()

	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"filepath.Walk failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}
	if !isAllSuccess {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:uploadFolder not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("uploadFolder not all success")
	}

	_err := os.Remove(uploadFolderRecord)
	if nil != _err {
		if !os.IsNotExist(_err) {
			ErrorLogger.WithContext(ctx).Error(
				"os.Remove failed.",
				zap.String("uploadFolderRecord", uploadFolderRecord),
				zap.Error(_err))
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:uploadFolder finish.")
	return nil
}

func (o *StarLight) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:uploadFileResume start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
		zap.Bool("needPure", needPure))

	uploadFileInput := new(SLUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + UploadFileRecordSuffix
	uploadFileInput.TaskNum = o.slUploadMultiTaskNum
	uploadFileInput.PartSize = DefaultPartSize
	if uploadFileInput.PartSize < DefaultSLMinPartSize {
		uploadFileInput.PartSize = DefaultSLMinPartSize
	} else if uploadFileInput.PartSize > DefaultSLMaxPartSize {
		uploadFileInput.PartSize = DefaultSLMaxPartSize
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
		uploadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:resumeUpload failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("objectPath", objectPath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:uploadFileResume finish.")
	return err
}

func (o *StarLight) resumeUpload(
	ctx context.Context,
	sourceFile,
	objectPath string,
	input *SLUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:resumeUpload start.",
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

	ufc := &SLUploadCheckpoint{}

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
				"StarLight:getUploadCheckpointFile failed.",
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}
	if needCheckpoint {
		err = o.prepareUpload(
			ctx,
			objectPath,
			ufc,
			uploadFileStat,
			input)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"StarLight:prepareUpload failed.",
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"StarLight:updateCheckpointFile failed.",
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
			"StarLight:uploadPartConcurrent failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return err
	}

	err = o.completeParts(
		ctx,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:completeParts failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:resumeUpload finish.")
	return err
}

func (o *StarLight) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	ufc *SLUploadCheckpoint,
	input *SLUploadFileInput) error {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:uploadPartConcurrent start.",
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
		task := SLUploadPartTask{
			ObjectPath:       ufc.ObjectPath,
			PartNumber:       uploadPart.PartNumber,
			SourceFile:       input.UploadFile,
			Offset:           uploadPart.Offset,
			PartSize:         uploadPart.PartSize,
			TotalSize:        ufc.FileInfo.Size,
			SlClient:         o.slClient,
			EnableCheckpoint: input.EnableCheckpoint,
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.slRateLimiter.Wait(ctxRate)
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
					"StarLight:handleUploadTaskResult failed.",
					zap.Int32("partNumber", task.PartNumber),
					zap.String("checkpointFile", input.CheckpointFile),
					zap.Error(_err))
				uploadPartError.Store(_err)
			}
			InfoLogger.WithContext(ctx).Debug(
				"StarLight:handleUploadTaskResult finish.")
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
		"StarLight:uploadPartConcurrent finish.")
	return nil
}

func (o *StarLight) getUploadCheckpointFile(
	ctx context.Context,
	ufc *SLUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *SLUploadFileInput) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:getUploadCheckpointFile start.")

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
			"StarLight:loadCheckpointFile failed.",
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
			"StarLight:loadCheckpointFile finish.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return false, nil
	}
	InfoLogger.WithContext(ctx).Debug(
		"StarLight:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *StarLight) prepareUpload(
	ctx context.Context,
	objectPath string,
	ufc *SLUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *SLUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:prepareUpload start.",
		zap.String("objectPath", objectPath))

	ufc.ObjectPath = objectPath
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = SLFileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()

	err = o.sliceFile(ctx, input.PartSize, ufc)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:sliceFile failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"StarLight:prepareUpload finish.")
	return err
}

func (o *StarLight) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *SLUploadCheckpoint) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:sliceFile start.",
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

	if partSize > DefaultSLMaxPartSize {
		ErrorLogger.WithContext(ctx).Error(
			"upload file part too large.",
			zap.Int64("partSize", partSize),
			zap.Int("maxPartSize", DefaultSLMaxPartSize))
		return fmt.Errorf("upload file part too large")
	}

	if cnt == 0 {
		uploadPart := SLUploadPartInfo{}
		uploadPart.PartNumber = 1
		ufc.UploadParts = []SLUploadPartInfo{uploadPart}
	} else {
		uploadParts := make([]SLUploadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			uploadPart := SLUploadPartInfo{}
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
		"StarLight:sliceFile finish.")
	return nil
}

func (o *StarLight) handleUploadTaskResult(
	ctx context.Context,
	result interface{},
	ufc *SLUploadCheckpoint,
	partNum int32,
	enableCheckpoint bool,
	checkpointFilePath string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:handleUploadTaskResult start.",
		zap.String("checkpointFilePath", checkpointFilePath),
		zap.Int32("partNum", partNum))

	if _, ok := result.(*SLBaseResponse); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"StarLight:updateCheckpointFile failed.",
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
		"StarLight:handleUploadTaskResult finish.")
	return
}

func (o *StarLight) completeParts(
	ctx context.Context,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:completeParts start.",
		zap.Bool("enableCheckpoint", enableCheckpoint),
		zap.String("checkpointFilePath", checkpointFilePath))

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
		"StarLight:completeParts finish.")
	return err
}

type SLUploadPartTask struct {
	ObjectPath       string
	PartNumber       int32
	SourceFile       string
	Offset           int64
	PartSize         int64
	TotalSize        int64
	SlClient         *SLClient
	EnableCheckpoint bool
}

func (task *SLUploadPartTask) Run(
	ctx context.Context,
	sourceFile string) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"SLUploadPartTask:Run start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", task.ObjectPath),
		zap.Int32("partNumber", task.PartNumber))

	err, resp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			fd, _err := os.Open(sourceFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"os.Open failed.",
					zap.String("sourceFile", sourceFile),
					zap.String("objectPath", task.ObjectPath),
					zap.Int32("partNumber", task.PartNumber),
					zap.Error(_err))
				return _err, nil
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
			if _, _err = fd.Seek(task.Offset, io.SeekStart); nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"fd.Seek failed.",
					zap.String("sourceFile", sourceFile),
					zap.String("objectPath", task.ObjectPath),
					zap.Int32("partNumber", task.PartNumber),
					zap.Error(_err))
				return _err, nil
			}

			contentRange := fmt.Sprintf("bytes=%d-%d/%d",
				task.Offset,
				task.Offset+task.PartSize-1,
				task.TotalSize)

			_err, respTmp := task.SlClient.UploadChunks(
				ctx,
				task.ObjectPath,
				contentRange,
				readerWrapper)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"SlClient.UploadChunks failed.",
					zap.String("objectPath", task.ObjectPath),
					zap.Int32("partNumber", task.PartNumber),
					zap.Error(_err))
				return _err, nil
			}
			return _err, respTmp
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SLUploadPartTask.Run failed.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int32("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"SLUploadPartTask:Run finish.",
		zap.String("objectPath", task.ObjectPath),
		zap.Int32("partNumber", task.PartNumber))
	return resp
}

func (o *StarLight) Download(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	if starLightDownloadInput, ok := input.(StarLightDownloadInput); ok {
		sourcePath = starLightDownloadInput.SourcePath
		targetPath = starLightDownloadInput.TargetPath
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Download start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath))

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

	err = o.downloadBatch(
		ctx,
		sourcePath,
		targetPath,
		downloadFolderRecord)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:downloadBatch failed.",
			zap.String("sourcePath", sourcePath),
			zap.String("targetPath", targetPath),
			zap.String("downloadFolderRecord", downloadFolderRecord),
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
		"StarLight:Download finish.")
	return nil
}

func (o *StarLight) downloadBatch(
	ctx context.Context,
	sourcePath,
	targetPath,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:downloadBatch start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
		zap.String("downloadFolderRecord", downloadFolderRecord))

	err, listOutputTmp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			output := new(SLListOutput)
			_err, output := o.slClient.List(ctx, sourcePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"slClient:List start.",
					zap.String("sourcePath", sourcePath),
					zap.Error(_err))
			}
			return _err, output
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"slClient:List failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}
	listOutput := new(SLListOutput)
	isValid := false
	if listOutput, isValid = listOutputTmp.(*SLListOutput); !isValid {
		ErrorLogger.WithContext(ctx).Error(
			"response invalid.")
		return errors.New("response invalid")
	}

	objects := make([]*SLObject, 0)
	folders := make([]*SLObject, 0)
	for _, object := range listOutput.Spec {
		if SLObjectTypeFile == object.Type {
			objects = append(objects, object)
		} else {
			folders = append(folders, object)
		}
	}

	for _, folder := range folders {
		itemPath := strings.TrimSuffix(targetPath, "/") + folder.Path
		err = os.MkdirAll(itemPath, os.ModePerm)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"os.MkdirAll failed.",
				zap.String("itemPath", itemPath),
				zap.Error(err))
			return
		}
	}

	err = o.downloadObjects(
		ctx,
		sourcePath,
		targetPath,
		objects,
		downloadFolderRecord)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:downloadObjects failed.",
			zap.String("sourcePath", sourcePath),
			zap.String("targetPath", targetPath),
			zap.String("downloadFolderRecord", downloadFolderRecord),
			zap.Error(err))
		return err
	}
	for _, folder := range folders {
		err = o.downloadBatch(
			ctx,
			folder.Path,
			targetPath,
			downloadFolderRecord)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"StarLight:downloadBatch failed.",
				zap.String("sourcePath", folder.Path),
				zap.String("targetPath", targetPath),
				zap.String("downloadFolderRecord", downloadFolderRecord),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:downloadBatch finish.",
		zap.String("sourcePath", sourcePath))
	return nil
}

func (o *StarLight) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	objects []*SLObject,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:downloadObjects start.",
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
	pool, err := ants.NewPool(o.slDownloadFileTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool for download Object failed.",
			zap.Error(err))
		return err
	}
	defer pool.Release()
	for index, object := range objects {
		InfoLogger.WithContext(ctx).Debug(
			"object content.",
			zap.Int("index", index),
			zap.String("path", object.Path),
			zap.Int64("size", object.Size))
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
		err = o.slRateLimiter.Wait(ctxRate)
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
			targetFile := strings.TrimSuffix(targetPath, "/") +
				itemObject.Path
			_err := o.downloadPart(
				ctx,
				itemObject,
				targetFile)
			if nil != _err {
				isAllSuccess = false
				ErrorLogger.WithContext(ctx).Error(
					"StarLight:downloadPart failed.",
					zap.String("objectPath", itemObject.Path),
					zap.String("targetFile", targetFile),
					zap.Error(_err))
				return
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
			"StarLight:downloadObjects not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("downloadObjects not all success")
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:downloadObjects finish.")
	return nil
}

func (o *StarLight) downloadPart(
	ctx context.Context,
	object *SLObject,
	targetFile string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:downloadPart start.",
		zap.String("path", object.Path),
		zap.String("targetFile", targetFile))

	downloadFileInput := new(SLDownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + DownloadFileRecordSuffix
	downloadFileInput.TaskNum = o.slDownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize

	err = o.resumeDownload(
		ctx,
		object,
		downloadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:resumeDownload failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"StarLight:downloadPart finish.")
	return
}

func (o *StarLight) resumeDownload(
	ctx context.Context,
	object *SLObject,
	input *SLDownloadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:resumeDownload start.")

	partSize := input.PartSize
	dfc := &SLDownloadCheckpoint{}

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
				"StarLight:getDownloadCheckpointFile failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(err))
			return err
		}
	}

	if needCheckpoint {
		dfc.ObjectPath = object.Path
		dfc.DownloadFile = input.DownloadFile
		dfc.ObjectInfo = SLObjectInfo{}
		dfc.ObjectInfo.Size = object.Size
		dfc.TempFileInfo = SLTempFileInfo{}
		dfc.TempFileInfo.TempFileUrl = input.DownloadFile + ".tmp"
		dfc.TempFileInfo.Size = object.Size

		o.sliceObject(ctx, object.Size, partSize, dfc)
		err = o.prepareTempFile(ctx,
			dfc.TempFileInfo.TempFileUrl,
			dfc.TempFileInfo.Size)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"StarLight:prepareTempFile failed.",
				zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
				zap.Int64("Size", dfc.TempFileInfo.Size),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"StarLight:updateCheckpointFile failed.",
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
		ctx, input, dfc)
	err = o.handleDownloadFileResult(
		ctx,
		dfc.TempFileInfo.TempFileUrl,
		enableCheckpoint,
		downloadFileError)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight:handleDownloadFileResult failed.",
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
		"StarLight:resumeDownload finish.")
	return nil
}

func (o *StarLight) downloadFileConcurrent(
	ctx context.Context,
	input *SLDownloadFileInput,
	dfc *SLDownloadCheckpoint) error {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:downloadFileConcurrent start.")

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

		begin := downloadPart.Offset
		end := downloadPart.Offset + downloadPart.Length - 1

		contentRange := fmt.Sprintf("bytes=%d-%d", begin, end)

		task := SLDownloadPartTask{
			ObjectPath:       dfc.ObjectPath,
			DownloadFile:     dfc.DownloadFile,
			Offset:           downloadPart.Offset,
			Length:           downloadPart.Length,
			Range:            contentRange,
			SlClient:         o.slClient,
			Sl:               o,
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
		err = o.slRateLimiter.Wait(ctxRate)
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
							"StarLight:updateCheckpointFile failed.",
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
						"StarLight:handleDownloadTaskResult failed.",
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
		"StarLight:downloadFileConcurrent finish.")
	return nil
}

func (o *StarLight) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *SLDownloadCheckpoint,
	input *SLDownloadFileInput,
	object *SLObject) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:getDownloadCheckpointFile start.",
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
			"StarLight:loadCheckpointFile failed.",
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

func (o *StarLight) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:prepareTempFile start.",
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
			"StarLight:createFile finish.",
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
		"StarLight:prepareTempFile finish.")
	return nil
}

func (o *StarLight) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:createFile start.",
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
		"StarLight:createFile finish.")
	return nil
}

func (o *StarLight) handleDownloadTaskResult(
	ctx context.Context,
	result interface{},
	dfc *SLDownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:handleDownloadTaskResult start.",
		zap.Int64("partNum", partNum),
		zap.String("checkpointFile", checkpointFile))

	if _, ok := result.(*SLDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Warn(
					"StarLight:updateCheckpointFile failed.",
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
		"StarLight:handleDownloadTaskResult finish.")
	return
}

func (o *StarLight) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:handleDownloadFileResult start.",
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
			"StarLight.handleDownloadFileResult finish.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(downloadFileError))
		return downloadFileError
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight.handleDownloadFileResult finish.")
	return nil
}

func (o *StarLight) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *SLDownloadPartOutput) error {

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:UpdateDownloadFile start.",
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
		"StarLight:UpdateDownloadFile finish.",
		zap.Int("readTotal", readTotal))
	return nil
}

func (o *StarLight) Delete(
	ctx context.Context,
	input interface{}) (err error) {

	var path string
	if starLightDeleteInput, ok := input.(StarLightDeleteInput); ok {
		path = starLightDeleteInput.Path
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Delete start.",
		zap.String("path", path))

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_err := o.slClient.Rm(ctx, path)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"slClient.Rm failed.",
					zap.String("path", path),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"StarLight.Delete failed.",
			zap.String("path", path),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"StarLight:Delete finish.")
	return nil
}

type SLDownloadPartTask struct {
	ObjectPath       string
	DownloadFile     string
	Offset           int64
	Length           int64
	Range            string
	SlClient         *SLClient
	Sl               *StarLight
	PartNumber       int64
	TempFileURL      string
	EnableCheckpoint bool
}

func (task *SLDownloadPartTask) Run(
	ctx context.Context) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"SLDownloadPartTask:Run start.",
		zap.String("objectPath", task.ObjectPath),
		zap.Int64("partNumber", task.PartNumber))

	err, downloadPartOutputTmp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			output := new(SLDownloadPartOutput)
			_err, output := task.SlClient.DownloadChunks(
				ctx,
				task.ObjectPath,
				task.Range)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"SlClient:DownloadChunks failed.",
					zap.String("objectPath", task.ObjectPath),
					zap.Int64("partNumber", task.PartNumber),
					zap.Error(_err))
			}
			return _err, output
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SLDownloadPartTask:Run failed.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int64("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	} else {
		downloadPartOutput := new(SLDownloadPartOutput)
		isValid := false
		if downloadPartOutput, isValid =
			downloadPartOutputTmp.(*SLDownloadPartOutput); !isValid {

			ErrorLogger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		InfoLogger.WithContext(ctx).Debug(
			"SlClient.DownloadChunks finish.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int64("partNumber", task.PartNumber))
		defer func() {
			errMsg := downloadPartOutput.Body.Close()
			if errMsg != nil {
				ErrorLogger.WithContext(ctx).Warn(
					"close response body failed.",
					zap.String("objectPath", task.ObjectPath),
					zap.Int64("partNumber", task.PartNumber))
			}
		}()
		err = task.Sl.UpdateDownloadFile(
			ctx,
			task.TempFileURL,
			task.Offset,
			downloadPartOutput)
		if nil != err {
			if !task.EnableCheckpoint {
				ErrorLogger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					zap.String("objectPath", task.ObjectPath),
					zap.Int64("partNumber", task.PartNumber))
			}
			ErrorLogger.WithContext(ctx).Error(
				"SL.updateDownloadFile failed.",
				zap.String("objectPath", task.ObjectPath),
				zap.Int64("partNumber", task.PartNumber),
				zap.Error(err))
			return err
		}
		InfoLogger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int64("partNumber", task.PartNumber))
		return downloadPartOutput
	}
}
