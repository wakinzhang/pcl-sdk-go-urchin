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

type ParaCloud struct {
	pcClient               *ParaCloudClient
	pcUploadFileTaskNum    int
	pcDownloadFileTaskNum  int
	pcDownloadMultiTaskNum int
	pcRateLimiter          *rate.Limiter
}

func (o *ParaCloud) Init(
	ctx context.Context,
	username,
	password,
	endpoint string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Init start.",
		zap.String("endpoint", endpoint))

	o.pcClient = new(ParaCloudClient)
	o.pcClient.Init(
		ctx,
		username,
		password,
		endpoint)

	o.pcUploadFileTaskNum = DefaultParaCloudUploadFileTaskNum
	o.pcDownloadFileTaskNum = DefaultParaCloudDownloadFileTaskNum
	o.pcDownloadMultiTaskNum = DefaultParaCloudDownloadMultiTaskNum

	o.pcRateLimiter = rate.NewLimiter(
		DefaultParaCloudRateLimit,
		DefaultParaCloudRateBurst)

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Init finish.")
	return nil
}

func (o *ParaCloud) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	o.pcUploadFileTaskNum = int(config.UploadFileTaskNum)
	o.pcDownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.pcDownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:SetConcurrency finish.")
	return nil
}

func (o *ParaCloud) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:SetRate start.")

	o.pcRateLimiter = rateLimiter

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:SetRate finish.")
	return nil
}

func (o *ParaCloud) Mkdir(
	ctx context.Context,
	input interface{}) (err error) {

	var sourceFolder, targetFolder string
	if paraCloudMkdirInput, ok := input.(ParaCloudMkdirInput); ok {
		sourceFolder = paraCloudMkdirInput.SourceFolder
		targetFolder = paraCloudMkdirInput.TargetFolder
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Mkdir start.",
		zap.String("sourceFolder", sourceFolder),
		zap.String("targetFolder", targetFolder))

	err = RetryV1(
		ctx,
		ParaCloudAttempts,
		ParaCloudDelay*time.Second,
		func() error {
			_err := o.pcClient.Mkdir(ctx, sourceFolder, targetFolder)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"pcClient.Mkdir failed.",
					zap.String("sourceFolder", sourceFolder),
					zap.String("targetFolder", targetFolder),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ParaCloud.Mkdir failed.",
			zap.String("sourceFolder", sourceFolder),
			zap.String("targetFolder", targetFolder),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Mkdir finish.")
	return nil
}

func (o *ParaCloud) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:loadCheckpointFile start.",
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
		"ParaCloud:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *ParaCloud) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *PCDownloadCheckpoint) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:sliceObject start.",
		zap.Int64("objectSize", objectSize),
		zap.Int64("partSize", partSize))

	cnt := objectSize / partSize
	if objectSize%partSize > 0 {
		cnt++
	}

	if cnt == 0 {
		downloadPart := PCDownloadPartInfo{}
		downloadPart.PartNumber = 1
		dfc.DownloadParts = []PCDownloadPartInfo{downloadPart}
	} else {
		downloadParts := make([]PCDownloadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			downloadPart := PCDownloadPartInfo{}
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
		"ParaCloud:sliceObject finish.")
}

func (o *ParaCloud) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:updateCheckpointFile start.",
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
		"ParaCloud:updateCheckpointFile finish.")
	return err
}

func (o *ParaCloud) Upload(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	var needPure bool
	if paraCloudUploadInput, ok := input.(ParaCloudUploadInput); ok {
		sourcePath = paraCloudUploadInput.SourcePath
		targetPath = paraCloudUploadInput.TargetPath
		needPure = paraCloudUploadInput.NeedPure
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Upload start.",
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
				"ParaCloud.uploadFolder failed.",
				zap.String("sourcePath", sourcePath),
				zap.Error(err))
			return err
		}
	} else {
		objectPath := targetPath + filepath.Base(sourcePath)
		err = RetryV1(
			ctx,
			ParaCloudAttempts,
			ParaCloudDelay*time.Second,
			func() error {
				_err := o.pcClient.Upload(ctx, sourcePath, objectPath)
				if nil != _err {
					ErrorLogger.WithContext(ctx).Error(
						"pcClient.Upload failed.",
						zap.String("sourcePath", sourcePath),
						zap.String("objectPath", objectPath),
						zap.Error(_err))
				}
				return _err
			})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"ParaCloud.Upload failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("objectPath", objectPath),
				zap.Error(err))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Upload finish.")
	return nil
}

func (o *ParaCloud) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:uploadFolder start.",
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

	paraCloudMkdirInput := ParaCloudMkdirInput{}
	paraCloudMkdirInput.SourceFolder = sourcePath
	paraCloudMkdirInput.TargetFolder = targetPath
	err = o.Mkdir(ctx, paraCloudMkdirInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ParaCloud.Mkdir failed.",
			zap.String("sourcePath", sourcePath),
			zap.String("targetPath", targetPath),
			zap.Error(err))
		return err
	}

	mkdirPool, err := ants.NewPool(DefaultParaCloudMkdirTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool mkdirPool failed.",
			zap.Error(err))
		return err
	}
	defer mkdirPool.Release()

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

			dirWaitGroup.Add(1)
			err = mkdirPool.Submit(func() {
				defer func() {
					dirWaitGroup.Done()
					if _err := recover(); nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"pcClient.Upload failed.",
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
						"RateLimiter.Wait start.")
					ctxRate, cancel := context.WithCancel(context.Background())
					_err = o.pcRateLimiter.Wait(ctxRate)
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

					input := ParaCloudMkdirInput{}
					input.SourceFolder = filePath
					input.TargetFolder = objectPath
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"ParaCloud.Mkdir failed.",
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
			"ParaCloud:uploadFolder not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("uploadFolder not all success")
	}

	uploadFilePool, err := ants.NewPool(o.pcUploadFileTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool uploadFilePool failed.",
			zap.Error(err))
		return err
	}
	defer uploadFilePool.Release()

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

			fileWaitGroup.Add(1)
			err = uploadFilePool.Submit(func() {
				defer func() {
					fileWaitGroup.Done()
					if _err := recover(); nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"pcClient.Upload failed.",
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
					if _, exists := fileMap[objectPath]; exists {
						InfoLogger.WithContext(ctx).Info(
							"already finish.",
							zap.String("objectPath", objectPath))
						return
					}

					InfoLogger.WithContext(ctx).Debug(
						"RateLimiter.Wait start.")
					ctxRate, cancel := context.WithCancel(context.Background())
					_err = o.pcRateLimiter.Wait(ctxRate)
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

					_err = RetryV1(
						ctx,
						ParaCloudAttempts,
						ParaCloudDelay*time.Second,
						func() error {
							__err := o.pcClient.Upload(
								ctx,
								filePath,
								objectPath)
							if nil != __err {
								ErrorLogger.WithContext(ctx).Error(
									"pcClient.Upload failed.",
									zap.String("filePath", filePath),
									zap.String("objectPath", objectPath),
									zap.Error(__err))
							}
							return __err
						})
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"pcClient.Upload failed.",
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
			"ParaCloud:uploadFolder not all success.",
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
		"ParaCloud:uploadFolder finish.")
	return nil
}

func (o *ParaCloud) Download(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	if paraCloudDownloadInput, ok := input.(ParaCloudDownloadInput); ok {
		sourcePath = paraCloudDownloadInput.SourcePath
		targetPath = paraCloudDownloadInput.TargetPath
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	targetPath = strings.TrimSuffix(targetPath, "/")

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Download start.",
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
			"ParaCloud:downloadBatch failed.",
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
		"ParaCloud:Download finish.")
	return nil
}

func (o *ParaCloud) downloadBatch(
	ctx context.Context,
	sourcePath,
	targetPath,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:downloadBatch start.",
		zap.String("sourcePath", sourcePath),
		zap.String("targetPath", targetPath),
		zap.String("downloadFolderRecord", downloadFolderRecord))

	err, fileInfoListTmp := RetryV4(
		ctx,
		ParaCloudAttempts,
		ParaCloudDelay*time.Second,
		func() (error, interface{}) {
			output := make([]os.FileInfo, 0)
			_err, output := o.pcClient.List(ctx, sourcePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"pcClient:List start.",
					zap.String("sourcePath", sourcePath),
					zap.Error(_err))
			}
			return _err, output
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"pcClient:List failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}

	fileInfoList := make([]os.FileInfo, 0)
	isValid := false
	if fileInfoList, isValid = fileInfoListTmp.([]os.FileInfo); !isValid {
		ErrorLogger.WithContext(ctx).Error(
			"response invalid.")
		return errors.New("response invalid")
	}

	objects := make([]*PCObject, 0)
	folders := make([]*PCObject, 0)
	for _, file := range fileInfoList {
		pcObject := new(PCObject)
		pcObject.ObjectPath = sourcePath + file.Name()
		pcObject.ObjectFileInfo = file
		if file.IsDir() {
			pcObject.ObjectPath = pcObject.ObjectPath + "/"
			folders = append(folders, pcObject)
		} else {
			objects = append(objects, pcObject)
		}
	}

	for _, folder := range folders {
		itemPath := targetPath + folder.ObjectPath
		err = os.MkdirAll(itemPath, folder.ObjectFileInfo.Mode())
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
			"ParaCloud:downloadObjects failed.",
			zap.String("sourcePath", sourcePath),
			zap.String("targetPath", targetPath),
			zap.String("downloadFolderRecord", downloadFolderRecord),
			zap.Error(err))
		return err
	}
	for _, folder := range folders {
		var paraCloudDownloadInput ParaCloudDownloadInput
		paraCloudDownloadInput.SourcePath = folder.ObjectPath
		paraCloudDownloadInput.TargetPath = targetPath

		err = o.downloadBatch(
			ctx,
			folder.ObjectPath,
			targetPath,
			downloadFolderRecord)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"ParaCloud:downloadBatch failed.",
				zap.String("sourcePath", folder.ObjectPath),
				zap.String("targetPath", targetPath),
				zap.String("downloadFolderRecord", downloadFolderRecord),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:downloadBatch finish.",
		zap.String("sourcePath", sourcePath))
	return nil
}

func (o *ParaCloud) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	objects []*PCObject,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:downloadObjects start.",
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
	pool, err := ants.NewPool(o.pcDownloadFileTaskNum)
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
			zap.String("path", object.ObjectPath),
			zap.Int64("size", object.ObjectFileInfo.Size()))
		itemObject := object
		if _, exists := fileMap[itemObject.ObjectPath]; exists {
			InfoLogger.WithContext(ctx).Info(
				"file already success.",
				zap.String("objectPath", itemObject.ObjectPath))
			continue
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.pcRateLimiter.Wait(ctxRate)
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
			targetFile := targetPath + itemObject.ObjectPath
			_err := o.downloadPart(
				ctx,
				itemObject,
				targetFile)
			if nil != _err {
				isAllSuccess = false
				ErrorLogger.WithContext(ctx).Error(
					"ParaCloud:downloadPart failed.",
					zap.String("objectPath", itemObject.ObjectPath),
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
			_, _err = f.Write([]byte(itemObject.ObjectPath + "\n"))
			if nil != _err {
				isAllSuccess = false
				ErrorLogger.WithContext(ctx).Error(
					"write file failed.",
					zap.String("downloadFolderRecord",
						downloadFolderRecord),
					zap.String("objectPath", itemObject.ObjectPath),
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
			"ParaCloud:downloadObjects not all success.",
			zap.String("sourcePath", sourcePath))
		return errors.New("downloadObjects not all success")
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:downloadObjects finish.")
	return nil
}

func (o *ParaCloud) downloadPart(
	ctx context.Context,
	object *PCObject,
	targetFile string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:downloadPart start.",
		zap.String("path", object.ObjectPath),
		zap.String("targetFile", targetFile))

	downloadFileInput := new(PCDownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + DownloadFileRecordSuffix
	downloadFileInput.TaskNum = o.pcDownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize

	err = o.resumeDownload(
		ctx,
		object,
		downloadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ParaCloud:resumeDownload failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:downloadPart finish.")
	return
}

func (o *ParaCloud) resumeDownload(
	ctx context.Context,
	object *PCObject,
	input *PCDownloadFileInput) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:resumeDownload start.")

	partSize := input.PartSize
	dfc := &PCDownloadCheckpoint{}

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
				"ParaCloud:getDownloadCheckpointFile failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(err))
			return err
		}
	}

	if needCheckpoint {
		dfc.ObjectPath = object.ObjectPath
		dfc.DownloadFile = input.DownloadFile
		dfc.ObjectInfo = PCObjectInfo{}
		dfc.ObjectInfo.Size = object.ObjectFileInfo.Size()
		dfc.TempFileInfo = PCTempFileInfo{}
		dfc.TempFileInfo.TempFileUrl = input.DownloadFile + ".tmp"
		dfc.TempFileInfo.Size = object.ObjectFileInfo.Size()

		o.sliceObject(ctx, object.ObjectFileInfo.Size(), partSize, dfc)
		err = o.prepareTempFile(ctx,
			dfc.TempFileInfo.TempFileUrl,
			dfc.TempFileInfo.Size)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"ParaCloud:prepareTempFile failed.",
				zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
				zap.Int64("Size", dfc.TempFileInfo.Size),
				zap.Error(err))
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"ParaCloud:updateCheckpointFile failed.",
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
			"ParaCloud:handleDownloadFileResult failed.",
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
		"ParaCloud:resumeDownload finish.")
	return nil
}

func (o *ParaCloud) downloadFileConcurrent(
	ctx context.Context,
	input *PCDownloadFileInput,
	dfc *PCDownloadCheckpoint) error {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:downloadFileConcurrent start.")

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

		task := ParaCloudDownloadPartTask{
			ObjectPath:       dfc.ObjectPath,
			DownloadFile:     dfc.DownloadFile,
			Offset:           downloadPart.Offset,
			Length:           downloadPart.Length,
			Range:            contentRange,
			PCClient:         o.pcClient,
			PC:               o,
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
		err = o.pcRateLimiter.Wait(ctxRate)
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
							"ParaCloud:updateCheckpointFile failed.",
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
						"ParaCloud:handleDownloadTaskResult failed.",
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
		"ParaCloud:downloadFileConcurrent finish.")
	return nil
}

func (o *ParaCloud) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *PCDownloadCheckpoint,
	input *PCDownloadFileInput,
	object *PCObject) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:getDownloadCheckpointFile start.",
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
			"ParaCloud:loadCheckpointFile failed.",
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

func (o *ParaCloud) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:prepareTempFile start.",
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
			"ParaCloud:createFile finish.",
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
		"ParaCloud:prepareTempFile finish.")
	return nil
}

func (o *ParaCloud) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:createFile start.",
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
		"ParaCloud:createFile finish.")
	return nil
}

func (o *ParaCloud) handleDownloadTaskResult(
	ctx context.Context,
	result interface{},
	dfc *PCDownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:handleDownloadTaskResult start.",
		zap.Int64("partNum", partNum),
		zap.String("checkpointFile", checkpointFile))

	if _, ok := result.(*PCDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Warn(
					"ParaCloud:updateCheckpointFile failed.",
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
		"ParaCloud:handleDownloadTaskResult finish.")
	return
}

func (o *ParaCloud) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:handleDownloadFileResult start.",
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
			"ParaCloud.handleDownloadFileResult finish.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(downloadFileError))
		return downloadFileError
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud.handleDownloadFileResult finish.")
	return nil
}

func (o *ParaCloud) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *PCDownloadPartOutput) error {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:UpdateDownloadFile start.",
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
		"ParaCloud:UpdateDownloadFile finish.",
		zap.Int("readTotal", readTotal))
	return nil
}

func (o *ParaCloud) Delete(
	ctx context.Context,
	input interface{}) (err error) {

	var path string
	if paraCloudDeleteInput, ok := input.(ParaCloudDeleteInput); ok {
		path = paraCloudDeleteInput.Path
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Delete start.",
		zap.String("path", path))

	err = RetryV1(
		ctx,
		ParaCloudAttempts,
		ParaCloudDelay*time.Second,
		func() error {
			_err := o.pcClient.Rm(ctx, path)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"pcClient.Rm failed.",
					zap.String("path", path),
					zap.Error(_err))
			}
			return _err
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ParaCloud.Delete failed.",
			zap.String("path", path),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloud:Delete finish.")
	return nil
}

type ParaCloudDownloadPartTask struct {
	ObjectPath       string
	DownloadFile     string
	Offset           int64
	Length           int64
	Range            string
	PCClient         *ParaCloudClient
	PC               *ParaCloud
	PartNumber       int64
	TempFileURL      string
	EnableCheckpoint bool
}

func (task *ParaCloudDownloadPartTask) Run(
	ctx context.Context) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudDownloadPartTask:Run start.",
		zap.String("objectPath", task.ObjectPath),
		zap.Int64("partNumber", task.PartNumber))

	err, downloadPartOutputTmp := RetryV4(
		ctx,
		ParaCloudAttempts,
		ParaCloudDelay*time.Second,
		func() (error, interface{}) {
			output := new(PCDownloadPartOutput)
			_err, output := task.PCClient.Download(
				ctx,
				task.ObjectPath,
				task.Offset,
				task.Length)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"PCClient:Download failed.",
					zap.String("objectPath", task.ObjectPath),
					zap.Int64("partNumber", task.PartNumber),
					zap.Error(_err))
			}
			return _err, output
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ParaCloudDownloadPartTask:Run failed.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int64("partNumber", task.PartNumber),
			zap.Error(err))
		return err
	} else {
		downloadPartOutput := new(PCDownloadPartOutput)
		isValid := false
		if downloadPartOutput, isValid =
			downloadPartOutputTmp.(*PCDownloadPartOutput); !isValid {

			ErrorLogger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		InfoLogger.WithContext(ctx).Debug(
			"ParaCloudDownloadPartTask.Run finish.",
			zap.String("objectPath", task.ObjectPath),
			zap.Int64("partNumber", task.PartNumber))
		defer func() {
			errMsg := downloadPartOutput.Body.Close()
			if errMsg != nil {
				ErrorLogger.WithContext(ctx).Warn(
					"close response body failed.")
			}
		}()
		_err := task.PC.UpdateDownloadFile(
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
				"ParaCloud.updateDownloadFile failed.",
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
