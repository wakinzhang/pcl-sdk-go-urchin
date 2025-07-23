package adaptee

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
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

type Scow struct {
	sClient               *ScowClient
	sUploadFileTaskNum    int
	sUploadMultiTaskNum   int
	sDownloadFileTaskNum  int
	sDownloadMultiTaskNum int
	sRateLimiter          *rate.Limiter
}

func (o *Scow) Init(
	ctx context.Context,
	username,
	password,
	endpoint,
	url,
	clusterId string,
	reqTimeout,
	maxConnection int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:Init start.",
		zap.String("endpoint", endpoint),
		zap.String("url", url),
		zap.String("clusterId", clusterId),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

	o.sClient = new(ScowClient)
	o.sClient.Init(
		ctx,
		username,
		password,
		endpoint,
		url,
		clusterId,
		reqTimeout,
		maxConnection)

	o.sUploadFileTaskNum = DefaultScowUploadFileTaskNum
	o.sUploadMultiTaskNum = DefaultScowUploadMultiTaskNum
	o.sDownloadFileTaskNum = DefaultScowDownloadFileTaskNum
	o.sDownloadMultiTaskNum = DefaultScowDownloadMultiTaskNum

	o.sRateLimiter = rate.NewLimiter(
		DefaultScowRateLimit,
		DefaultScowRateBurst)

	InfoLogger.WithContext(ctx).Debug(
		"Scow:Init finish.")
	return nil
}

func (o *Scow) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("UploadMultiTaskNum", config.UploadMultiTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	o.sUploadFileTaskNum = int(config.UploadFileTaskNum)
	o.sUploadMultiTaskNum = int(config.UploadMultiTaskNum)
	o.sDownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.sDownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	InfoLogger.WithContext(ctx).Debug(
		"Scow:SetConcurrency finish.")
	return nil
}

func (o *Scow) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:SetRate start.")

	o.sRateLimiter = rateLimiter

	InfoLogger.WithContext(ctx).Debug(
		"Scow:SetRate finish.")
	return nil
}

func (o *Scow) Mkdir(
	ctx context.Context,
	input interface{}) (err error) {

	var path string
	if scowMkdirInput, ok := input.(ScowMkdirInput); ok {
		path = scowMkdirInput.Path
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:Mkdir start.",
		zap.String("path", path))

	err = RetryV1(
		ctx,
		ScowAttempts,
		ScowDelay*time.Second,
		func() error {
			_err := o.sClient.Mkdir(ctx, path)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sClient.Mkdir failed.",
					zap.String("path", path),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Scow.Mkdir failed.",
			zap.String("path", path),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:Mkdir finish.")
	return nil
}

func (o *Scow) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:loadCheckpointFile start.",
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
		"Scow:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *Scow) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *ScowDownloadCheckpoint) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:sliceObject start.",
		zap.Int64("objectSize", objectSize),
		zap.Int64("partSize", partSize))

	cnt := objectSize / partSize
	if objectSize%partSize > 0 {
		cnt++
	}

	if cnt == 0 {
		downloadPart := ScowDownloadPartInfo{}
		downloadPart.PartNumber = 1
		dfc.DownloadParts = []ScowDownloadPartInfo{downloadPart}
	} else {
		downloadParts := make([]ScowDownloadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			downloadPart := ScowDownloadPartInfo{}
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
		"Scow:sliceObject finish.")
}

func (o *Scow) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:updateCheckpointFile start.",
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
		"Scow:updateCheckpointFile finish.")
	return err
}

func (o *Scow) Upload(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	var needPure bool
	if scowUploadInput, ok := input.(ScowUploadInput); ok {
		sourcePath = scowUploadInput.SourcePath
		targetPath = scowUploadInput.TargetPath
		needPure = scowUploadInput.NeedPure
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:Upload start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
		zap.Bool("needPure", needPure))

	err = RetryV1(
		ctx,
		ScowAttempts,
		ScowDelay*time.Second,
		func() error {
			_err := o.sClient.Mkdir(ctx, targetPath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sClient.Mkdir failed.",
					zap.String("targetPath", targetPath),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"sClient.Mkdir failed.",
			zap.String("targetPath", targetPath),
			zap.Error(err))
		return err
	}

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
				"Scow.uploadFolder failed.",
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
				"Scow.uploadFile failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"Scow:Upload finish.")
	return nil
}

func (o *Scow) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:uploadFolder start.",
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

	scowMkdirInput := ScowMkdirInput{}
	scowMkdirInput.Path = targetPath
	err = o.Mkdir(ctx, scowMkdirInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Scow.Mkdir failed.",
			zap.String("targetPath", targetPath),
			zap.Error(err))
		return err
	}

	pool, err := ants.NewPool(o.sUploadFileTaskNum)
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
			err = o.sRateLimiter.Wait(ctxRate)
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
							"Scow:uploadFileResume failed.",
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
					InfoLogger.WithContext(ctx).Debug(
						"file is dir.",
						zap.String("fileName", fileInfo.Name()),
						zap.String("filePath", filePath),
						zap.String("relPath", relPath),
						zap.String("objectPath", objectPath))

					input := ScowMkdirInput{}
					input.Path = objectPath
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"Scow.Mkdir failed.",
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
			"Scow:uploadFolder not all success.",
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
			err = o.sRateLimiter.Wait(ctxRate)
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
							"Scow:uploadFileResume failed.",
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
							"Scow:uploadFile failed.",
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
			"Scow:uploadFolder not all success.",
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
		"Scow:uploadFolder finish.")
	return nil
}

func (o *Scow) uploadFile(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:uploadFile start.",
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

	if DefaultScowUploadMultiSize < sourceFileStat.Size() {
		err, exist := o.sClient.CheckExist(ctx, objectPath)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"sClient.CheckExist failed.",
				zap.Error(err))
			return err
		}
		if true == exist {
			InfoLogger.WithContext(ctx).Info(
				"Path already exist, no need to upload.",
				zap.String("objectPath", objectPath))
			return err
		}
		err = o.uploadFileResume(
			ctx,
			sourceFile,
			objectPath,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"Scow:uploadFileResume failed.",
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
				"Scow:uploadFileStream failed.",
				zap.String("sourceFile", sourceFile),
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:uploadFile finish.")
	return err
}

func (o *Scow) uploadFileStream(
	ctx context.Context,
	sourceFile,
	objectPath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:uploadFileStream start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath))

	err = RetryV1(
		ctx,
		ScowAttempts,
		ScowDelay*time.Second,
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

			_err = o.sClient.Upload(
				ctx,
				sourceFile,
				objectPath,
				fd)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sClient.Upload failed.",
					zap.String("sourceFile", sourceFile),
					zap.String("objectPath", objectPath),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Scow.uploadFileStream failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("objectPath", objectPath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:uploadFileStream finish.")
	return err
}

func (o *Scow) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:uploadFileResume start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectPath", objectPath),
		zap.Bool("needPure", needPure))

	uploadFileInput := new(ScowUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.FileName = filepath.Base(sourceFile)
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + UploadFileRecordSuffix
	uploadFileInput.TaskNum = o.sUploadMultiTaskNum
	uploadFileInput.PartSize = DefaultPartSize
	if uploadFileInput.PartSize < DefaultScowMinPartSize {
		uploadFileInput.PartSize = DefaultScowMinPartSize
	} else if uploadFileInput.PartSize > DefaultScowMaxPartSize {
		uploadFileInput.PartSize = DefaultScowMaxPartSize
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
	hash := md5.New()
	if _, err = io.Copy(hash, fd); nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.Copy failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return err
	}
	md5Value := hex.EncodeToString(hash.Sum(nil))
	uploadFileInput.Md5 = md5Value

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
			"Scow:resumeUpload failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("objectPath", objectPath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:uploadFileResume finish.")
	return err
}

func (o *Scow) resumeUpload(
	ctx context.Context,
	sourceFile,
	objectPath string,
	input *ScowUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:resumeUpload start.",
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

	ufc := &ScowUploadCheckpoint{}

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
				"Scow:getUploadCheckpointFile failed.",
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
				"Scow:prepareUpload failed.",
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"Scow:updateCheckpointFile failed.",
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
			"Scow:uploadPartConcurrent failed.",
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
			"Scow:completeParts failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:resumeUpload finish.")
	return err
}

func (o *Scow) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	ufc *ScowUploadCheckpoint,
	input *ScowUploadFileInput) error {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:uploadPartConcurrent start.",
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
		task := ScowUploadPartTask{
			ObjectPath:       filepath.Dir(ufc.ObjectPath),
			FileName:         input.FileName,
			Md5:              input.Md5,
			PartNumber:       uploadPart.PartNumber,
			SourceFile:       input.UploadFile,
			Offset:           uploadPart.Offset,
			PartSize:         uploadPart.PartSize,
			TotalSize:        ufc.FileInfo.Size,
			SClient:          o.sClient,
			EnableCheckpoint: input.EnableCheckpoint,
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.sRateLimiter.Wait(ctxRate)
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
					"Scow:handleUploadTaskResult failed.",
					zap.Int32("partNumber", task.PartNumber),
					zap.String("checkpointFile", input.CheckpointFile),
					zap.Error(_err))
				uploadPartError.Store(_err)
			}
			InfoLogger.WithContext(ctx).Debug(
				"Scow:handleUploadTaskResult finish.")
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
		"Scow:uploadPartConcurrent finish.")
	return nil
}

func (o *Scow) getUploadCheckpointFile(
	ctx context.Context,
	ufc *ScowUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *ScowUploadFileInput) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:getUploadCheckpointFile start.")

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
			"Scow:loadCheckpointFile failed.",
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
			"Scow:loadCheckpointFile finish.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return false, nil
	}
	InfoLogger.WithContext(ctx).Debug(
		"Scow:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *Scow) prepareUpload(
	ctx context.Context,
	objectPath string,
	ufc *ScowUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *ScowUploadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:prepareUpload start.",
		zap.String("objectPath", objectPath))

	ufc.ObjectPath = objectPath
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = ScowFileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()

	err = o.sliceFile(ctx, input.PartSize, ufc)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Scow:sliceFile failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"Scow:prepareUpload finish.")
	return err
}

func (o *Scow) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *ScowUploadCheckpoint) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:sliceFile start.",
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

	if partSize > DefaultScowMaxPartSize {
		ErrorLogger.WithContext(ctx).Error(
			"upload file part too large.",
			zap.Int64("partSize", partSize),
			zap.Int("maxPartSize", DefaultScowMaxPartSize))
		return fmt.Errorf("upload file part too large")
	}

	if cnt == 0 {
		uploadPart := ScowUploadPartInfo{}
		uploadPart.PartNumber = 1
		ufc.UploadParts = []ScowUploadPartInfo{uploadPart}
	} else {
		uploadParts := make([]ScowUploadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			uploadPart := ScowUploadPartInfo{}
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
		"Scow:sliceFile finish.")
	return nil
}

func (o *Scow) handleUploadTaskResult(
	ctx context.Context,
	result interface{},
	ufc *ScowUploadCheckpoint,
	partNum int32,
	enableCheckpoint bool,
	checkpointFilePath string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:handleUploadTaskResult start.",
		zap.String("checkpointFilePath", checkpointFilePath),
		zap.Int32("partNum", partNum))

	if _, ok := result.(*ScowBaseMessageResponse); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"Scow:updateCheckpointFile failed.",
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
		"Scow:handleUploadTaskResult finish.")
	return
}

func (o *Scow) completeParts(
	ctx context.Context,
	input *ScowUploadFileInput,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:completeParts start.",
		zap.Bool("enableCheckpoint", enableCheckpoint),
		zap.String("checkpointFilePath", checkpointFilePath),
		zap.String("fileName", input.FileName),
		zap.String("objectPath: ", input.ObjectPath),
		zap.String("md5", input.Md5))

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_err := o.sClient.MergeChunks(
				ctx,
				input.FileName,
				filepath.Dir(input.ObjectPath),
				input.Md5)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sClient.MergeChunks failed.",
					zap.String("fileName", input.FileName),
					zap.String("path", filepath.Dir(input.ObjectPath)),
					zap.String("md5", input.Md5),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Scow.completeParts failed.",
			zap.String("fileName", input.FileName),
			zap.String("path", filepath.Dir(input.ObjectPath)),
			zap.String("md5", input.Md5),
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
		"Scow:completeParts finish.")
	return err
}

type ScowUploadPartTask struct {
	ObjectPath       string
	FileName         string
	Md5              string
	PartNumber       int32
	SourceFile       string
	Offset           int64
	PartSize         int64
	TotalSize        int64
	SClient          *ScowClient
	EnableCheckpoint bool
}

func (task *ScowUploadPartTask) Run(
	ctx context.Context,
	sourceFile string) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"ScowUploadPartTask:Run start.",
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
			_err, respTmp := task.SClient.UploadChunks(
				ctx,
				task.FileName,
				task.ObjectPath,
				task.Md5,
				task.PartNumber,
				readerWrapper)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"SClient.UploadChunks failed.",
					zap.String("fileName", task.FileName),
					zap.String("objectPath", task.ObjectPath),
					zap.String("md5", task.Md5),
					zap.Int32("partNumber", task.PartNumber),
					zap.Error(_err))
				return _err, nil
			}
			return _err, respTmp
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowUploadPartTask.Run failed.",
			zap.String("fileName", task.FileName),
			zap.String("objectPath", task.ObjectPath),
			zap.String("md5", task.Md5),
			zap.Int32("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ScowUploadPartTask:Run finish.",
		zap.String("objectPath", task.ObjectPath),
		zap.Int32("partNumber", task.PartNumber))
	return resp
}

func (o *Scow) Download(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	if scowDownloadInput, ok := input.(ScowDownloadInput); ok {
		sourcePath = scowDownloadInput.SourcePath
		targetPath = scowDownloadInput.TargetPath
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:Download start.",
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
			"Scow:downloadBatch failed.",
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
		"Scow:Download finish.")
	return nil
}

func (o *Scow) downloadBatch(
	ctx context.Context,
	sourcePath,
	targetPath,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:downloadBatch start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
		zap.String("downloadFolderRecord", downloadFolderRecord))

	err, scowListResponseBodyTmp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			output := new(ScowListResponseBody)
			_err, output := o.sClient.List(ctx, sourcePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sClient:List start.",
					zap.String("sourcePath", sourcePath),
					zap.Error(_err))
			}
			return _err, output
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"sClient:List start.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}
	scowListResponseBody := new(ScowListResponseBody)
	isValid := false
	if scowListResponseBody, isValid =
		scowListResponseBodyTmp.(*ScowListResponseBody); !isValid {

		ErrorLogger.WithContext(ctx).Error(
			"response invalid.")
		return errors.New("response invalid")
	}

	objects := make([]*ScowObject, 0)
	folders := make([]*ScowObject, 0)
	for _, object := range scowListResponseBody.ScowObjects {
		object.PathExt = sourcePath + object.Name
		if ScowObjectTypeFile == object.Type {
			objects = append(objects, object)
		} else {
			object.PathExt = object.PathExt + "/"
			folders = append(folders, object)
		}
	}

	for _, folder := range folders {
		itemPath := strings.TrimSuffix(targetPath, "/") +
			folder.PathExt
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
			"Scow:downloadObjects failed.",
			zap.String("sourcePath", sourcePath),
			zap.String("targetPath", targetPath),
			zap.String("downloadFolderRecord", downloadFolderRecord),
			zap.Error(err))
		return err
	}
	for _, folder := range folders {
		var scowDownloadInput ScowDownloadInput
		scowDownloadInput.SourcePath = folder.PathExt
		scowDownloadInput.TargetPath = targetPath

		err = o.downloadBatch(
			ctx,
			folder.PathExt,
			targetPath,
			downloadFolderRecord)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"Scow:downloadBatch failed.",
				zap.String("sourcePath", folder.PathExt),
				zap.String("targetPath", targetPath),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:downloadBatch finish.",
		zap.String("sourcePath", sourcePath))
	return nil
}

func (o *Scow) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	objects []*ScowObject,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:downloadObjects start.",
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
	pool, err := ants.NewPool(o.sDownloadFileTaskNum)
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
			zap.String("name: ", object.Name),
			zap.Int64("size", object.Size))
		itemObject := object
		if _, exists := fileMap[itemObject.PathExt]; exists {
			InfoLogger.WithContext(ctx).Info(
				"file already success.",
				zap.String("objectPath", itemObject.PathExt))
			continue
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.sRateLimiter.Wait(ctxRate)
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
				itemObject.PathExt
			_err := o.downloadPart(
				ctx,
				itemObject,
				targetFile)
			if nil != _err {
				isAllSuccess = false
				ErrorLogger.WithContext(ctx).Error(
					"Scow:downloadPart failed.",
					zap.String("objectName", itemObject.Name),
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
			_, _err = f.Write([]byte(itemObject.PathExt + "\n"))
			if nil != _err {
				isAllSuccess = false
				ErrorLogger.WithContext(ctx).Error(
					"write file failed.",
					zap.String("downloadFolderRecord",
						downloadFolderRecord),
					zap.String("objectPath", itemObject.PathExt),
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
			"Scow:downloadObjects not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("downloadObjects not all success")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:downloadObjects finish.")
	return nil
}

func (o *Scow) downloadPart(
	ctx context.Context,
	object *ScowObject,
	targetFile string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:downloadPart start.",
		zap.String("name: ", object.Name),
		zap.String("targetFile", targetFile))

	downloadFileInput := new(ScowDownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + DownloadFileRecordSuffix
	downloadFileInput.TaskNum = o.sDownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize

	err = o.resumeDownload(
		ctx,
		object,
		downloadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Scow:resumeDownload failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"Scow:downloadPart finish.")
	return
}

func (o *Scow) resumeDownload(
	ctx context.Context,
	object *ScowObject,
	input *ScowDownloadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:resumeDownload start.")

	partSize := input.PartSize
	dfc := &ScowDownloadCheckpoint{}

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
				"Scow:getDownloadCheckpointFile failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(err))
			return err
		}
	}

	if needCheckpoint {
		dfc.ObjectPath = object.PathExt
		dfc.DownloadFile = input.DownloadFile
		dfc.ObjectInfo = ScowObjectInfo{}
		dfc.ObjectInfo.Size = object.Size
		dfc.TempFileInfo = ScowTempFileInfo{}
		dfc.TempFileInfo.TempFileUrl = input.DownloadFile + ".tmp"
		dfc.TempFileInfo.Size = object.Size

		o.sliceObject(ctx, object.Size, partSize, dfc)
		err = o.prepareTempFile(ctx,
			dfc.TempFileInfo.TempFileUrl,
			dfc.TempFileInfo.Size)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"Scow:prepareTempFile failed.",
				zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
				zap.Int64("Size", dfc.TempFileInfo.Size),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"Scow:updateCheckpointFile failed.",
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
			"Scow:handleDownloadFileResult failed.",
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
		"Scow:resumeDownload finish.")
	return nil
}

func (o *Scow) downloadFileConcurrent(
	ctx context.Context,
	input *ScowDownloadFileInput,
	dfc *ScowDownloadCheckpoint) error {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:downloadFileConcurrent start.")

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

		task := ScowDownloadPartTask{
			ObjectPath:       dfc.ObjectPath,
			DownloadFile:     dfc.DownloadFile,
			Offset:           downloadPart.Offset,
			Length:           downloadPart.Length,
			Range:            contentRange,
			SClient:          o.sClient,
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
		err = o.sRateLimiter.Wait(ctxRate)
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
							"Scow:updateCheckpointFile failed.",
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
						"Scow:handleDownloadTaskResult failed.",
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
		"Scow:downloadFileConcurrent finish.")
	return nil
}

func (o *Scow) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *ScowDownloadCheckpoint,
	input *ScowDownloadFileInput,
	object *ScowObject) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:getDownloadCheckpointFile start.",
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
			"Scow:loadCheckpointFile failed.",
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

func (o *Scow) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:prepareTempFile start.",
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
			"Scow:createFile finish.",
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
		"Scow:prepareTempFile finish.")
	return nil
}

func (o *Scow) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:createFile start.",
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
		"Scow:createFile finish.")
	return nil
}

func (o *Scow) handleDownloadTaskResult(
	ctx context.Context,
	result interface{},
	dfc *ScowDownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:handleDownloadTaskResult start.",
		zap.Int64("partNum", partNum),
		zap.String("checkpointFile", checkpointFile))

	if _, ok := result.(*ScowDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Warn(
					"Scow:updateCheckpointFile failed.",
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
		"Scow:handleDownloadTaskResult finish.")
	return
}

func (o *Scow) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:handleDownloadFileResult start.",
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
			"Scow.handleDownloadFileResult finish.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(downloadFileError))
		return downloadFileError
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow.handleDownloadFileResult finish.")
	return nil
}

func (o *Scow) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *ScowDownloadPartOutput) error {

	InfoLogger.WithContext(ctx).Debug(
		"Scow:UpdateDownloadFile start.",
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
		"Scow:UpdateDownloadFile finish.",
		zap.Int("readTotal", readTotal))
	return nil
}

func (o *Scow) Delete(
	ctx context.Context,
	input interface{}) (err error) {

	var path, target string
	if scowDeleteInput, ok := input.(ScowDeleteInput); ok {
		path = scowDeleteInput.Path
		target = scowDeleteInput.Target
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:Delete start.",
		zap.String("path", path),
		zap.String("target", target))

	err = RetryV1(
		ctx,
		ScowAttempts,
		ScowDelay*time.Second,
		func() error {
			_err := o.sClient.Delete(ctx, path, target)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"sClient.Delete failed.",
					zap.String("path", path),
					zap.String("target", target),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Scow.Delete failed.",
			zap.String("path", path),
			zap.String("target", target),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Scow:Delete finish.")
	return nil
}

type ScowDownloadPartTask struct {
	ObjectPath       string
	DownloadFile     string
	Offset           int64
	Length           int64
	Range            string
	SClient          *ScowClient
	S                *Scow
	PartNumber       int64
	TempFileURL      string
	EnableCheckpoint bool
}

func (task *ScowDownloadPartTask) Run(
	ctx context.Context) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"ScowDownloadPartTask:Run start.",
		zap.String("objectPath", task.ObjectPath),
		zap.Int64("partNumber", task.PartNumber))

	err, downloadPartOutputTmp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			output := new(ScowDownloadPartOutput)
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
			"ScowDownloadPartTask:Run failed.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int64("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	} else {
		downloadPartOutput := new(ScowDownloadPartOutput)
		isValid := false
		if downloadPartOutput, isValid =
			downloadPartOutputTmp.(*ScowDownloadPartOutput); !isValid {

			ErrorLogger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		InfoLogger.WithContext(ctx).Debug(
			"ScowDownloadPartTask.Run finish.",
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
				"Scow.updateDownloadFile failed.",
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
