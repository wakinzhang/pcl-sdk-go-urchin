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

type Sugon struct {
	sugonClient               *SugonClient
	sugonUploadFileTaskNum    int
	sugonUploadMultiTaskNum   int
	sugonDownloadFileTaskNum  int
	sugonDownloadMultiTaskNum int
	sugonRateLimiter          *rate.Limiter
}

func (o *Sugon) Init(
	ctx context.Context,
	user,
	password,
	endpoint,
	url,
	orgId,
	clusterId string,
	reqTimeout,
	maxConnection int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Init start.",
		zap.String("endpoint", endpoint),
		zap.String("url", url),
		zap.String("orgId", orgId),
		zap.String("clusterId", clusterId),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

	o.sugonClient = new(SugonClient)
	o.sugonClient.Init(
		ctx,
		user,
		password,
		endpoint,
		url,
		orgId,
		clusterId,
		reqTimeout,
		maxConnection)

	o.sugonUploadFileTaskNum = DefaultSugonUploadFileTaskNum
	o.sugonUploadMultiTaskNum = DefaultSugonUploadMultiTaskNum
	o.sugonDownloadFileTaskNum = DefaultSugonDownloadFileTaskNum
	o.sugonDownloadMultiTaskNum = DefaultSugonDownloadMultiTaskNum

	o.sugonRateLimiter = rate.NewLimiter(
		DefaultSugonRateLimit,
		DefaultSugonRateBurst)

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Init finish.")
	return nil
}

func (o *Sugon) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("UploadMultiTaskNum", config.UploadMultiTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	o.sugonUploadFileTaskNum = int(config.UploadFileTaskNum)
	o.sugonUploadMultiTaskNum = int(config.UploadMultiTaskNum)
	o.sugonDownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.sugonDownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:SetConcurrency finish.")
	return nil
}

func (o *Sugon) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:SetRate start.")

	o.sugonRateLimiter = rateLimiter

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:SetRate finish.")
	return nil
}

func (o *Sugon) Mkdir(
	ctx context.Context,
	input interface{}) (err error) {

	var path string
	if sugonMkdirInput, ok := input.(SugonMkdirInput); ok {
		path = sugonMkdirInput.Path
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Mkdir start.",
		zap.String("path", path))

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_err := o.sugonClient.Mkdir(ctx, path)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sugonClient.Mkdir failed.",
					zap.String("path", path),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Sugon.Mkdir failed.",
			zap.String("path", path),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Mkdir finish.")
	return nil
}

func (o *Sugon) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:loadCheckpointFile start.",
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
		"Sugon:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *Sugon) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *SugonDownloadCheckpoint) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:sliceObject start.",
		zap.Int64("objectSize", objectSize),
		zap.Int64("partSize", partSize))

	cnt := objectSize / partSize
	if objectSize%partSize > 0 {
		cnt++
	}

	if cnt == 0 {
		downloadPart := SugonDownloadPartInfo{}
		downloadPart.PartNumber = 1
		dfc.DownloadParts = []SugonDownloadPartInfo{downloadPart}
	} else {
		downloadParts := make([]SugonDownloadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			downloadPart := SugonDownloadPartInfo{}
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
		"Sugon:sliceObject finish.")
}

func (o *Sugon) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:updateCheckpointFile start.",
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
		"Sugon:updateCheckpointFile finish.")
	return err
}

func (o *Sugon) Upload(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	var needPure bool
	if sugonUploadInput, ok := input.(SugonUploadInput); ok {
		sourcePath = sugonUploadInput.SourcePath
		targetPath = sugonUploadInput.TargetPath
		needPure = sugonUploadInput.NeedPure
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Upload start.",
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
				"Sugon.uploadFolder failed.",
				zap.String("sourcePath", sourcePath),
				zap.Error(err))
			return err
		}
	} else {
		objectPath := targetPath + filepath.Base(sourcePath)
		err = o.uploadFile(
			ctx,
			sourcePath,
			objectPath,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"Sugon.uploadFile failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Upload finish.")
	return nil
}

func (o *Sugon) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:uploadFolder start.",
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

	sugonMkdirInput := SugonMkdirInput{}
	sugonMkdirInput.Path = targetPath
	err = o.Mkdir(ctx, sugonMkdirInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Sugon.Mkdir failed.",
			zap.String("targetPath", targetPath),
			zap.Error(err))
		return err
	}

	pool, err := ants.NewPool(o.sugonUploadFileTaskNum)
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
			err = o.sugonRateLimiter.Wait(ctxRate)
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
							"Sugon:uploadFileResume failed.",
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

					input := SugonMkdirInput{}
					input.Path = objectPath
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"Sugon.Mkdir failed.",
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
			"Sugon:uploadFolder not all success.",
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
			err = o.sugonRateLimiter.Wait(ctxRate)
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
							"Sugon:uploadFileResume failed.",
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
					_err = o.uploadFile(
						ctx,
						filePath,
						objectPath,
						needPure)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"Sugon:uploadFile failed.",
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
			"Sugon:uploadFolder not all success.",
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
		"Sugon:uploadFolder finish.")
	return nil
}

func (o *Sugon) uploadFile(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:uploadFile start.",
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

	if DefaultSugonUploadMultiSize < sourceFileStat.Size() {
		err = o.uploadFileResume(
			ctx,
			sourceFile,
			objectPath,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"Sugon:uploadFileResume failed.",
				zap.String("sourceFile", sourceFile),
				zap.String("objectPath", objectPath),
				zap.Bool("needPure", needPure),
				zap.Error(err))
			return err
		}
	} else {
		err = o.uploadFileStream(
			ctx,
			sourceFile,
			objectPath)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"Sugon:uploadFileStream failed.",
				zap.String("sourceFile", sourceFile),
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:uploadFile finish.")
	return err
}

func (o *Sugon) uploadFileStream(
	ctx context.Context,
	sourceFile,
	objectPath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:uploadFileStream start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath))

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			fd, _err := os.Open(sourceFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"os.Open failed.",
					zap.String("sourceFile", sourceFile),
					zap.Error(_err))
				return _err
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
			fileName := filepath.Base(objectPath)
			path := filepath.Dir(objectPath)
			_err = o.sugonClient.Upload(
				ctx,
				fileName,
				path,
				fd)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sugonClient.Upload failed.",
					zap.String("fileName", fileName),
					zap.String("path", path),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Sugon.uploadFileStream failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("objectPath", objectPath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:uploadFileStream finish.")
	return err
}

func (o *Sugon) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:uploadFileResume start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
		zap.Bool("needPure", needPure))

	uploadFileInput := new(SugonUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.FileName = filepath.Base(sourceFile)
	uploadFileInput.RelativePath = filepath.Base(sourceFile)
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + UploadFileRecordSuffix
	uploadFileInput.TaskNum = o.sugonUploadMultiTaskNum
	uploadFileInput.PartSize = DefaultPartSize
	if uploadFileInput.PartSize < DefaultSugonMinPartSize {
		uploadFileInput.PartSize = DefaultSugonMinPartSize
	} else if uploadFileInput.PartSize > DefaultSugonMaxPartSize {
		uploadFileInput.PartSize = DefaultSugonMaxPartSize
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
			"Sugon:resumeUpload failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("objectPath", objectPath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:uploadFileResume finish.")
	return err
}

func (o *Sugon) resumeUpload(
	ctx context.Context,
	sourceFile,
	objectPath string,
	input *SugonUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:resumeUpload start.",
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

	ufc := &SugonUploadCheckpoint{}

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
				"Sugon:getUploadCheckpointFile failed.",
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
				"Sugon:prepareUpload failed.",
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"Sugon:updateCheckpointFile failed.",
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
			"Sugon:uploadPartConcurrent failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return err
	}

	err = o.completeParts(
		ctx,
		input,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Sugon:completeParts failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:resumeUpload finish.")
	return err
}

func (o *Sugon) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	ufc *SugonUploadCheckpoint,
	input *SugonUploadFileInput) error {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:uploadPartConcurrent start.",
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
		task := SugonUploadPartTask{
			File:             sourceFile,
			FileName:         input.FileName,
			Path:             filepath.Dir(input.ObjectPath),
			RelativePath:     input.FileName,
			ChunkNumber:      uploadPart.PartNumber,
			TotalChunks:      ufc.TotalParts,
			Offset:           uploadPart.Offset,
			ChunkSize:        input.PartSize,
			CurrentChunkSize: uploadPart.PartSize,
			TotalSize:        ufc.FileInfo.Size,
			SClient:          o.sugonClient,
			EnableCheckpoint: input.EnableCheckpoint,
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.sugonRateLimiter.Wait(ctxRate)
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
				task.ChunkNumber,
				input.EnableCheckpoint,
				input.CheckpointFile,
				lock)
			if nil != _err &&
				atomic.CompareAndSwapInt32(&errFlag, 0, 1) {

				ErrorLogger.WithContext(ctx).Error(
					"Sugon:handleUploadTaskResult failed.",
					zap.Int32("chunkNumber", task.ChunkNumber),
					zap.String("checkpointFile", input.CheckpointFile),
					zap.Error(_err))
				uploadPartError.Store(_err)
			}
			InfoLogger.WithContext(ctx).Debug(
				"Sugon:handleUploadTaskResult finish.")
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
		"Sugon:uploadPartConcurrent finish.")
	return nil
}

func (o *Sugon) getUploadCheckpointFile(
	ctx context.Context,
	ufc *SugonUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *SugonUploadFileInput) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:getUploadCheckpointFile start.")

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
			"Sugon:loadCheckpointFile failed.",
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
			"Sugon:loadCheckpointFile finish.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return false, nil
	}
	InfoLogger.WithContext(ctx).Debug(
		"Sugon:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *Sugon) prepareUpload(
	ctx context.Context,
	objectPath string,
	ufc *SugonUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *SugonUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:prepareUpload start.",
		zap.String("objectPath", objectPath))

	ufc.ObjectPath = objectPath
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = SugonFileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()

	err = o.sliceFile(ctx, input.PartSize, ufc)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Sugon:sliceFile failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"Sugon:prepareUpload finish.")
	return err
}

func (o *Sugon) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *SugonUploadCheckpoint) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:sliceFile start.",
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

	if partSize > DefaultSugonMaxPartSize {
		ErrorLogger.WithContext(ctx).Error(
			"upload file part too large.",
			zap.Int64("partSize", partSize),
			zap.Int("maxPartSize", DefaultSugonMaxPartSize))
		return fmt.Errorf("upload file part too large")
	}

	if cnt == 0 {
		uploadPart := SugonUploadPartInfo{}
		uploadPart.PartNumber = 1
		ufc.UploadParts = []SugonUploadPartInfo{uploadPart}
	} else {
		uploadParts := make([]SugonUploadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			uploadPart := SugonUploadPartInfo{}
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
	ufc.TotalParts = int32(len(ufc.UploadParts))

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:sliceFile finish.")
	return nil
}

func (o *Sugon) handleUploadTaskResult(
	ctx context.Context,
	result interface{},
	ufc *SugonUploadCheckpoint,
	partNum int32,
	enableCheckpoint bool,
	checkpointFilePath string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:handleUploadTaskResult start.",
		zap.String("checkpointFilePath", checkpointFilePath),
		zap.Int32("partNum", partNum))

	if _, ok := result.(*SugonBaseResponse); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"Sugon:updateCheckpointFile failed.",
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
		"Sugon:handleUploadTaskResult finish.")
	return
}

func (o *Sugon) completeParts(
	ctx context.Context,
	input *SugonUploadFileInput,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:completeParts start.",
		zap.Bool("enableCheckpoint", enableCheckpoint),
		zap.String("checkpointFilePath", checkpointFilePath))

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_err := o.sugonClient.MergeChunks(
				ctx,
				input.FileName,
				filepath.Dir(input.ObjectPath),
				input.RelativePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sugonClient.MergeChunks failed.",
					zap.String("fileName", input.FileName),
					zap.String("path", filepath.Dir(input.ObjectPath)),
					zap.String("relativePath", input.RelativePath),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Sugon.completeParts failed.",
			zap.String("fileName", input.FileName),
			zap.String("path", filepath.Dir(input.ObjectPath)),
			zap.String("relativePath", input.RelativePath),
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
		"Sugon:completeParts finish.")
	return err
}

type SugonUploadPartTask struct {
	File             string
	FileName         string
	Path             string
	RelativePath     string
	ChunkNumber      int32
	TotalChunks      int32
	TotalSize        int64
	ChunkSize        int64
	Offset           int64
	CurrentChunkSize int64
	SClient          *SugonClient
	EnableCheckpoint bool
}

func (task *SugonUploadPartTask) Run(
	ctx context.Context,
	sourceFile string) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"SugonUploadPartTask:Run start.",
		zap.String("sourceFile", sourceFile),
		zap.String("path", task.Path),
		zap.Int32("chunkNumber", task.ChunkNumber))

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
					zap.String("path", task.Path),
					zap.Int32("chunkNumber", task.ChunkNumber),
					zap.Error(_err))
				return _err, nil
			}
			defer func() {
				errMsg := fd.Close()
				if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
					ErrorLogger.WithContext(ctx).Warn(
						"close file failed.",
						zap.String("sourceFile", sourceFile),
						zap.String("path", task.Path),
						zap.Int32("chunkNumber", task.ChunkNumber),
						zap.Error(errMsg))
				}
			}()
			readerWrapper := new(ReaderWrapper)
			readerWrapper.Reader = fd

			readerWrapper.TotalCount = task.CurrentChunkSize
			readerWrapper.Mark = task.Offset
			if _, _err = fd.Seek(task.Offset, io.SeekStart); nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"fd.Seek failed.",
					zap.String("sourceFile", sourceFile),
					zap.String("path", task.Path),
					zap.Int32("chunkNumber", task.ChunkNumber),
					zap.Error(_err))
				return _err, nil
			}
			respTmp := new(SugonBaseResponse)
			_err, respTmp = task.SClient.UploadChunks(
				ctx,
				task.File,
				task.FileName,
				task.Path,
				task.RelativePath,
				task.ChunkNumber,
				task.TotalChunks,
				task.TotalSize,
				task.ChunkSize,
				task.CurrentChunkSize,
				readerWrapper)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"SClient.UploadChunks failed.",
					zap.String("file", task.File),
					zap.String("fileName", task.FileName),
					zap.String("path", task.Path),
					zap.String("relativePath", task.RelativePath),
					zap.Int32("chunkNumber", task.ChunkNumber),
					zap.Int32("totalChunks", task.TotalChunks),
					zap.Int64("totalSize", task.TotalSize),
					zap.Int64("chunkSize", task.ChunkSize),
					zap.Int64("currentChunkSize", task.CurrentChunkSize),
					zap.Error(_err))
				return _err, nil
			}
			return _err, respTmp
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonUploadPartTask.Run failed.",
			zap.String("path", task.Path),
			zap.Int32("chunkNumber", task.ChunkNumber),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"SugonUploadPartTask:Run finish.",
		zap.String("path", task.Path),
		zap.Int32("chunkNumber", task.ChunkNumber))
	return resp
}

func (o *Sugon) Download(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	if sugonDownloadInput, ok := input.(SugonDownloadInput); ok {
		sourcePath = sugonDownloadInput.SourcePath
		targetPath = sugonDownloadInput.TargetPath
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Download start.",
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
			"Sugon:downloadBatch failed.",
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
		"Sugon:Download finish.")
	return nil
}

func (o *Sugon) downloadBatch(
	ctx context.Context,
	sourcePath,
	targetPath,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:downloadBatch start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
		zap.String("downloadFolderRecord", downloadFolderRecord))

	var start int32 = 0
	var limit int32 = DefaultSugonListLimit
	for {
		err, sugonListResponseDataTmp := RetryV4(
			ctx,
			Attempts,
			Delay*time.Second,
			func() (error, interface{}) {
				output := new(SugonListResponseData)
				_err, output := o.sugonClient.List(
					ctx,
					sourcePath,
					start,
					limit)
				if nil != _err {
					ErrorLogger.WithContext(ctx).Error(
						"sugonClient:List start.",
						zap.String("sourcePath", sourcePath),
						zap.Int32("start", start),
						zap.Int32("limit", limit),
						zap.Error(_err))
				}
				return _err, output
			})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"sugonClient:List failed.",
				zap.String("sourcePath", sourcePath),
				zap.Int32("start", start),
				zap.Int32("limit", limit),
				zap.Error(err))
			return err
		}

		sugonListResponseData := new(SugonListResponseData)
		isValid := false
		if sugonListResponseData, isValid =
			sugonListResponseDataTmp.(*SugonListResponseData); !isValid {

			ErrorLogger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		for _, folder := range sugonListResponseData.Children {
			itemPath := strings.TrimSuffix(targetPath, "/") + folder.Path
			err = os.MkdirAll(itemPath, os.ModePerm)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"os.MkdirAll failed.",
					zap.String("itemPath", itemPath),
					zap.Error(err))
				return err
			}
		}

		err = o.downloadObjects(
			ctx,
			sourcePath,
			targetPath,
			sugonListResponseData.FileList,
			downloadFolderRecord)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"Sugon:downloadObjects failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("targetPath", targetPath),
				zap.String("downloadFolderRecord", downloadFolderRecord),
				zap.Error(err))
			return err
		}

		for _, folder := range sugonListResponseData.Children {
			var sugonDownloadInput SugonDownloadInput
			sugonDownloadInput.SourcePath = folder.Path
			sugonDownloadInput.TargetPath = targetPath

			err = o.downloadBatch(
				ctx,
				folder.Path,
				targetPath,
				downloadFolderRecord)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"Sugon:downloadBatch failed.",
					zap.String("sourcePath", folder.Path),
					zap.String("targetPath", targetPath),
					zap.String("downloadFolderRecord",
						downloadFolderRecord),
					zap.Error(err))
				return err
			}
		}

		start = start + limit
		if start >= sugonListResponseData.Total {
			break
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:downloadBatch finish.",
		zap.String("sourcePath", sourcePath))
	return nil
}

func (o *Sugon) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	objects []*SugonFileInfo,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:downloadObjects start.",
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
	pool, err := ants.NewPool(o.sugonDownloadFileTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool for download object failed.",
			zap.Error(err))
		return err
	}
	defer pool.Release()
	for index, object := range objects {
		InfoLogger.WithContext(ctx).Debug(
			"object content.",
			zap.Int("index", index),
			zap.String("path", object.Path),
			zap.Bool("isDirectory", object.IsDirectory))

		itemObject := object

		if itemObject.IsDirectory {
			InfoLogger.WithContext(ctx).Info(
				"dir already process, pass.",
				zap.String("path", itemObject.Path))
			continue
		}

		if _, exists := fileMap[itemObject.Path]; exists {
			InfoLogger.WithContext(ctx).Info(
				"file already success.",
				zap.String("path", itemObject.Path))
			continue
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.sugonRateLimiter.Wait(ctxRate)
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
					"Sugon:downloadPart failed.",
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
			"Sugon:downloadObjects not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("downloadObjects not all success")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:downloadObjects finish.")
	return nil
}

func (o *Sugon) downloadPart(
	ctx context.Context,
	object *SugonFileInfo,
	targetFile string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:downloadPart start.",
		zap.String("objectPath", object.Path),
		zap.String("targetFile", targetFile))

	downloadFileInput := new(SugonDownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + DownloadFileRecordSuffix
	downloadFileInput.TaskNum = o.sugonDownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize

	err = o.resumeDownload(
		ctx,
		object,
		downloadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Sugon:resumeDownload failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"Sugon:downloadPart finish.")
	return
}

func (o *Sugon) resumeDownload(
	ctx context.Context,
	object *SugonFileInfo,
	input *SugonDownloadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:resumeDownload start.")

	partSize := input.PartSize
	dfc := &SugonDownloadCheckpoint{}

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
				"Sugon:getDownloadCheckpointFile failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(err))
			return err
		}
	}

	if needCheckpoint {
		dfc.ObjectPath = object.Path
		dfc.DownloadFile = input.DownloadFile
		dfc.ObjectInfo = SugonFileInfo{}
		dfc.ObjectInfo.Size = object.Size
		dfc.TempFileInfo = SugonTempFileInfo{}
		dfc.TempFileInfo.TempFileUrl = input.DownloadFile + ".tmp"
		dfc.TempFileInfo.Size = object.Size

		o.sliceObject(ctx, object.Size, partSize, dfc)
		err = o.prepareTempFile(ctx,
			dfc.TempFileInfo.TempFileUrl,
			dfc.TempFileInfo.Size)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"Sugon:prepareTempFile failed.",
				zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
				zap.Int64("Size", dfc.TempFileInfo.Size),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"Sugon:updateCheckpointFile failed.",
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
			"Sugon:handleDownloadFileResult failed.",
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
		"Sugon:resumeDownload finish.")
	return nil
}

func (o *Sugon) downloadFileConcurrent(
	ctx context.Context,
	input *SugonDownloadFileInput,
	dfc *SugonDownloadCheckpoint) error {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:downloadFileConcurrent start.")

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
		end := downloadPart.Offset + downloadPart.Length

		contentRange := fmt.Sprintf("bytes=%d-%d", begin, end)

		task := SugonDownloadPartTask{
			ObjectPath:       dfc.ObjectPath,
			DownloadFile:     dfc.DownloadFile,
			Offset:           downloadPart.Offset,
			Length:           downloadPart.Length,
			Range:            contentRange,
			SClient:          o.sugonClient,
			S:                o,
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
		err = o.sugonRateLimiter.Wait(ctxRate)
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
							"Sugon:updateCheckpointFile failed.",
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
						"Sugon:handleDownloadTaskResult failed.",
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
		"Sugon:downloadFileConcurrent finish.")
	return nil
}

func (o *Sugon) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *SugonDownloadCheckpoint,
	input *SugonDownloadFileInput,
	object *SugonFileInfo) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:getDownloadCheckpointFile start.",
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
			"Sugon:loadCheckpointFile failed.",
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

func (o *Sugon) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:prepareTempFile start.",
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
			"Sugon:createFile finish.",
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
		"Sugon:prepareTempFile finish.")
	return nil
}

func (o *Sugon) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:createFile start.",
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
		"Sugon:createFile finish.")
	return nil
}

func (o *Sugon) handleDownloadTaskResult(
	ctx context.Context,
	result interface{},
	dfc *SugonDownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:handleDownloadTaskResult start.",
		zap.Int64("partNum", partNum),
		zap.String("checkpointFile", checkpointFile))

	if _, ok := result.(*SugonDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Warn(
					"Sugon:updateCheckpointFile failed.",
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
		"Sugon:handleDownloadTaskResult finish.")
	return
}

func (o *Sugon) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:handleDownloadFileResult start.",
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
			"Sugon.handleDownloadFileResult finish.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(downloadFileError))
		return downloadFileError
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon.handleDownloadFileResult finish.")
	return nil
}

func (o *Sugon) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *SugonDownloadPartOutput) error {

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:UpdateDownloadFile start.",
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
		"Sugon:UpdateDownloadFile finish.",
		zap.Int("readTotal", readTotal))
	return nil
}

func (o *Sugon) Delete(
	ctx context.Context,
	input interface{}) (err error) {

	var path string
	if sugonDeleteInput, ok := input.(SugonDeleteInput); ok {
		path = sugonDeleteInput.Path
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Delete start.",
		zap.String("path", path))

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_err := o.sugonClient.Delete(ctx, path)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sugonClient.Delete failed.",
					zap.String("path", path),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Sugon.Delete failed.",
			zap.String("path", path),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Sugon:Delete finish.")
	return nil
}

type SugonDownloadPartTask struct {
	ObjectPath       string
	DownloadFile     string
	Offset           int64
	Length           int64
	Range            string
	SClient          *SugonClient
	S                *Sugon
	PartNumber       int64
	TempFileURL      string
	EnableCheckpoint bool
}

func (task *SugonDownloadPartTask) Run(
	ctx context.Context) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"SugonDownloadPartTask:Run start.",
		zap.String("objectPath", task.ObjectPath),
		zap.Int64("partNumber", task.PartNumber))

	err, downloadPartOutputTmp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			output := new(SugonDownloadPartOutput)
			_err, output := task.SClient.DownloadChunks(
				ctx,
				task.ObjectPath,
				task.Range)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"SClient:DownloadChunks failed.",
					zap.String("objectPath", task.ObjectPath),
					zap.Int64("partNumber", task.PartNumber),
					zap.Error(_err))
			}
			return _err, output
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonDownloadPartTask:Run failed.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int64("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	} else {
		downloadPartOutput := new(SugonDownloadPartOutput)
		isValid := false
		if downloadPartOutput, isValid =
			downloadPartOutputTmp.(*SugonDownloadPartOutput); !isValid {

			ErrorLogger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		InfoLogger.WithContext(ctx).Debug(
			"SugonDownloadPartTask.Run finish.",
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
		_err := task.S.UpdateDownloadFile(
			ctx,
			task.TempFileURL,
			task.Offset,
			downloadPartOutput)
		if nil != _err {
			if !task.EnableCheckpoint {
				ErrorLogger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					zap.String("objectPath", task.ObjectPath),
					zap.Int64("partNumber", task.PartNumber))
			}
			ErrorLogger.WithContext(ctx).Error(
				"Sugon.updateDownloadFile failed.",
				zap.String("objectPath", task.ObjectPath),
				zap.Int64("partNumber", task.PartNumber),
				zap.Error(_err))
			return _err
		}
		InfoLogger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int64("partNumber", task.PartNumber))
		return downloadPartOutput
	}
}
