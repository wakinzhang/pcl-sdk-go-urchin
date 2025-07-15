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
}

func (o *StarLight) Init(
	ctx context.Context,
	username,
	password,
	endpoint string,
	reqTimeout,
	maxConnection int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:Init start.",
		" username: ", "***",
		" password: ", "***",
		" endpoint: ", endpoint,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

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

	Logger.WithContext(ctx).Debug(
		"StarLight:Init finish.")
	return nil
}

func (o *StarLight) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:SetConcurrency start.",
		" UploadFileTaskNum: ", config.UploadFileTaskNum,
		" DownloadFileTaskNum: ", config.DownloadFileTaskNum,
		" DownloadMultiTaskNum: ", config.DownloadMultiTaskNum)

	o.slUploadFileTaskNum = int(config.UploadFileTaskNum)
	o.slDownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.slDownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	Logger.WithContext(ctx).Debug(
		"StarLight:SetConcurrency finish.")
	return nil
}

func (o *StarLight) Mkdir(
	ctx context.Context,
	input interface{}) (err error) {

	var target string
	if starLightMkdirInput, ok := input.(StarLightMkdirInput); ok {
		target = starLightMkdirInput.Target
	} else {
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:Mkdir start.",
		" target: ", target)

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_err := o.slClient.Mkdir(ctx, target)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"slClient.Mkdir failed.",
					" target: ", target,
					" err: ", _err)
			}
			return _err
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"StarLight.Mkdir failed.",
			" target: ", target,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:Mkdir finish.")
	return nil
}

func (o *StarLight) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	Logger.WithContext(ctx).Debug(
		"StarLight:loadCheckpointFile start.",
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
		return errors.New("checkpointFile empty")
	}
	Logger.WithContext(ctx).Debug(
		"StarLight:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *StarLight) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *SLDownloadCheckpoint) {

	Logger.WithContext(ctx).Debug(
		"StarLight:sliceObject start.",
		" objectSize: ", objectSize,
		" partSize: ", partSize)

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
	Logger.WithContext(ctx).Debug(
		"StarLight:sliceObject finish.")
}

func (o *StarLight) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	Logger.WithContext(ctx).Debug(
		"StarLight:updateCheckpointFile start.",
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
	file, _ := os.OpenFile(checkpointFilePath, os.O_WRONLY, 0)
	defer func() {
		errMsg := file.Close()
		if errMsg != nil {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" err: ", errMsg)
		}
	}()
	_ = file.Sync()

	Logger.WithContext(ctx).Debug(
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
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:Upload start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
		" needPure: ", needPure)

	stat, err := os.Stat(sourcePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}

	if stat.IsDir() {
		err = o.uploadFolder(ctx, sourcePath, targetPath, needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"StarLight.uploadFolder failed.",
				" sourcePath: ", sourcePath,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"StarLight.uploadFileResume failed.",
				" sourcePath: ", sourcePath,
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"StarLight:Upload finish.")
	return nil
}

func (o *StarLight) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:uploadFolder start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
		" needPure: ", needPure)

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	uploadFolderRecord :=
		strings.TrimSuffix(sourcePath, "/") + UploadFolderRecordSuffix
	Logger.WithContext(ctx).Debug(
		"uploadFolderRecord file info.",
		" uploadFolderRecord: ", uploadFolderRecord)

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

	starLightMkdirInput := StarLightMkdirInput{}
	starLightMkdirInput.Target = targetPath
	err = o.Mkdir(ctx, starLightMkdirInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"StarLight.Mkdir failed.",
			" targetPath: ", targetPath,
			" err: ", err)
		return err
	}

	pool, err := ants.NewPool(o.slUploadFileTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			" err: ", err)
		return err
	}
	defer pool.Release()

	var isAllSuccess = true
	var dirWaitGroup sync.WaitGroup
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

			if sourcePath == filePath {
				Logger.WithContext(ctx).Debug(
					"root dir no need todo.")
				return nil
			}

			dirWaitGroup.Add(1)
			err = pool.Submit(func() {
				defer func() {
					dirWaitGroup.Done()
					if _err := recover(); nil != _err {
						Logger.WithContext(ctx).Error(
							"StarLight:uploadFileResume failed.",
							" err: ", _err)
						isAllSuccess = false
					}
				}()

				if fileInfo.IsDir() {
					relPath, _err := filepath.Rel(
						filepath.Dir(sourcePath),
						filePath)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"filepath.Rel failed.",
							" sourcePath: ", sourcePath,
							" filePath: ", filePath,
							" relPath: ", relPath,
							" err: ", _err)
						return
					}
					objectPath := targetPath + relPath
					if _, exists := fileMap[objectPath]; exists {
						Logger.WithContext(ctx).Info(
							"already finish. objectPath: ", objectPath)
						return
					}

					input := StarLightMkdirInput{}
					input.Target = objectPath
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"StarLight.Mkdir failed.",
							" objectPath: ", objectPath,
							" err: ", _err)
						return
					}
					fileMutex.Lock()
					defer fileMutex.Unlock()
					f, _err := os.OpenFile(
						uploadFolderRecord,
						os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"os.OpenFile failed.",
							" uploadFolderRecord: ", uploadFolderRecord,
							" err: ", _err)
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
					_, _err = f.Write([]byte(objectPath + "\n"))
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"write file failed.",
							" uploadFolderRecord: ", uploadFolderRecord,
							" objectPath: ", objectPath,
							" err: ", _err)
						return
					}
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
	dirWaitGroup.Wait()

	if nil != err {
		Logger.WithContext(ctx).Error(
			"filepath.Walk failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}
	if !isAllSuccess {
		Logger.WithContext(ctx).Error(
			"StarLight:uploadFolder not all success.",
			" sourcePath: ", sourcePath)
		return errors.New("uploadFolder not all success")
	}

	var fileWaitGroup sync.WaitGroup
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

			if sourcePath == filePath {
				Logger.WithContext(ctx).Debug(
					"root dir no need todo.")
				return nil
			}

			fileWaitGroup.Add(1)
			err = pool.Submit(func() {
				defer func() {
					fileWaitGroup.Done()
					if _err := recover(); nil != _err {
						Logger.WithContext(ctx).Error(
							"StarLight:uploadFileResume failed.",
							" err: ", _err)
						isAllSuccess = false
					}
				}()

				if !fileInfo.IsDir() {
					relPath, _err := filepath.Rel(
						filepath.Dir(sourcePath),
						filePath)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"filepath.Rel failed.",
							" sourcePath: ", sourcePath,
							" filePath: ", filePath,
							" relPath: ", relPath,
							" err: ", _err)
						return
					}
					objectPath := targetPath + relPath
					if strings.HasSuffix(objectPath, UploadFileRecordSuffix) {
						Logger.WithContext(ctx).Info(
							"upload record file.",
							" objectPath: ", objectPath)
						return
					}
					if _, exists := fileMap[objectPath]; exists {
						Logger.WithContext(ctx).Info(
							"already finish. objectPath: ", objectPath)
						return
					}
					_err = o.uploadFileResume(
						ctx,
						filePath,
						objectPath,
						needPure)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"StarLight:uploadFileResume failed.",
							" filePath: ", filePath,
							" objectPath: ", objectPath,
							" err: ", _err)
						return
					}
					fileMutex.Lock()
					defer fileMutex.Unlock()
					f, _err := os.OpenFile(
						uploadFolderRecord,
						os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"os.OpenFile failed.",
							" uploadFolderRecord: ", uploadFolderRecord,
							" err: ", _err)
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
					_, _err = f.Write([]byte(objectPath + "\n"))
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"write file failed.",
							" uploadFolderRecord: ", uploadFolderRecord,
							" objectPath: ", objectPath,
							" err: ", _err)
						return
					}
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
	fileWaitGroup.Wait()

	if nil != err {
		Logger.WithContext(ctx).Error(
			"filepath.Walk failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}
	if !isAllSuccess {
		Logger.WithContext(ctx).Error(
			"StarLight:uploadFolder not all success.",
			" sourcePath: ", sourcePath)
		return errors.New("uploadFolder not all success")
	}

	_err := os.Remove(uploadFolderRecord)
	if nil != _err {
		if !os.IsNotExist(_err) {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" uploadFolderRecord: ", uploadFolderRecord,
				" err: ", _err)
		}
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:uploadFolder finish.")
	return nil
}

func (o *StarLight) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:uploadFileResume start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" needPure: ", needPure)

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
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" CheckpointFile: ", uploadFileInput.CheckpointFile,
					" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"StarLight:resumeUpload failed.",
			" sourceFile: ", sourceFile,
			" objectPath: ", objectPath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:uploadFileResume finish.")
	return err
}

func (o *StarLight) resumeUpload(
	ctx context.Context,
	sourceFile,
	objectPath string,
	input *SLUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:resumeUpload start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath)

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
			Logger.WithContext(ctx).Error(
				"StarLight:getUploadCheckpointFile failed.",
				" objectPath: ", objectPath,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"StarLight:prepareUpload failed.",
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"StarLight:updateCheckpointFile failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"StarLight:uploadPartConcurrent failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	err = o.completeParts(
		ctx,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"StarLight:completeParts failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:resumeUpload finish.")
	return err
}

func (o *StarLight) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	ufc *SLUploadCheckpoint,
	input *SLUploadFileInput) error {

	Logger.WithContext(ctx).Debug(
		"StarLight:uploadPartConcurrent start.",
		" sourceFile: ", sourceFile)

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

				Logger.WithContext(ctx).Error(
					"StarLight:handleUploadTaskResult failed.",
					" partNumber: ", task.PartNumber,
					" checkpointFile: ", input.CheckpointFile,
					" err: ", _err)
				uploadPartError.Store(_err)
			}
			Logger.WithContext(ctx).Debug(
				"StarLight:handleUploadTaskResult finish.")
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
		"StarLight:uploadPartConcurrent finish.")
	return nil
}

func (o *StarLight) getUploadCheckpointFile(
	ctx context.Context,
	ufc *SLUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *SLUploadFileInput) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:getUploadCheckpointFile start.")

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
			"StarLight:loadCheckpointFile failed.",
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
			"StarLight:loadCheckpointFile finish.",
			" checkpointFilePath: ", checkpointFilePath)
		return false, nil
	}
	Logger.WithContext(ctx).Debug(
		"StarLight:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *StarLight) prepareUpload(
	ctx context.Context,
	objectPath string,
	ufc *SLUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *SLUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:prepareUpload start.",
		" objectPath: ", objectPath)

	ufc.ObjectPath = objectPath
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = SLFileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()

	err = o.sliceFile(ctx, input.PartSize, ufc)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"StarLight:sliceFile failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"StarLight:prepareUpload finish.")
	return err
}

func (o *StarLight) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *SLUploadCheckpoint) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:sliceFile start.",
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

	if partSize > DefaultSLMaxPartSize {
		Logger.WithContext(ctx).Error(
			"upload file part too large.",
			" partSize: ", partSize,
			" maxPartSize: ", DefaultSLMaxPartSize)
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
	Logger.WithContext(ctx).Debug(
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

	Logger.WithContext(ctx).Debug(
		"StarLight:handleUploadTaskResult start.",
		" checkpointFilePath: ", checkpointFilePath,
		" partNum: ", partNum)

	if _, ok := result.(*SLBaseResponse); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"StarLight:updateCheckpointFile failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" partNum: ", partNum,
					" err: ", _err)
			}
		}
	} else if result != ErrAbort {
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
		"StarLight:handleUploadTaskResult finish.")
	return
}

func (o *StarLight) completeParts(
	ctx context.Context,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:completeParts start.",
		" enableCheckpoint: ", enableCheckpoint,
		" checkpointFilePath: ", checkpointFilePath)

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

	Logger.WithContext(ctx).Debug(
		"SLUploadPartTask:Run start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", task.ObjectPath,
		" partNumber: ", task.PartNumber)

	err, resp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			fd, _err := os.Open(sourceFile)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"os.Open failed.",
					" sourceFile: ", sourceFile,
					" objectPath: ", task.ObjectPath,
					" partNumber: ", task.PartNumber,
					" err: ", _err)
				return _err, nil
			}
			defer func() {
				errMsg := fd.Close()
				if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
					Logger.WithContext(ctx).Warn(
						"close file failed.",
						" sourceFile: ", sourceFile,
						" objectPath: ", task.ObjectPath,
						" partNumber: ", task.PartNumber,
						" err: ", errMsg)
				}
			}()
			readerWrapper := new(ReaderWrapper)
			readerWrapper.Reader = fd

			readerWrapper.TotalCount = task.PartSize
			readerWrapper.Mark = task.Offset
			if _, _err = fd.Seek(task.Offset, io.SeekStart); nil != _err {
				Logger.WithContext(ctx).Error(
					"fd.Seek failed.",
					" sourceFile: ", sourceFile,
					" objectPath: ", task.ObjectPath,
					" partNumber: ", task.PartNumber,
					" err: ", _err)
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
				Logger.WithContext(ctx).Error(
					"SlClient.UploadChunks failed.",
					" objectPath: ", task.ObjectPath,
					" partNumber: ", task.PartNumber,
					" err: ", _err)
				return _err, nil
			}
			return _err, respTmp
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SLUploadPartTask.Run failed.",
			" objectPath: ", task.ObjectPath,
			" partNumber: ", task.PartNumber,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"SLUploadPartTask:Run finish.",
		" objectPath: ", task.ObjectPath,
		" partNumber: ", task.PartNumber)
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
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:Download start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath)

	err = os.MkdirAll(targetPath, os.ModePerm)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.MkdirAll failed.",
			" targetPath: ", targetPath,
			" err: ", err)
		return
	}

	downloadFolderRecord :=
		strings.TrimSuffix(targetPath, "/") + DownloadFolderRecordSuffix
	Logger.WithContext(ctx).Debug(
		"downloadFolderRecord file info.",
		" downloadFolderRecord: ", downloadFolderRecord)

	err = o.downloadBatch(
		ctx,
		sourcePath,
		targetPath,
		downloadFolderRecord)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"StarLight:downloadBatch failed.",
			" sourcePath: ", sourcePath,
			" targetPath: ", targetPath,
			" downloadFolderRecord: ", downloadFolderRecord,
			" err: ", err)
		return err
	}

	err = os.Remove(downloadFolderRecord)
	if nil != err {
		if !os.IsNotExist(err) {
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" downloadFolderRecord: ", downloadFolderRecord,
				" err: ", err)
		}
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:Download finish.")
	return nil
}

func (o *StarLight) downloadBatch(
	ctx context.Context,
	sourcePath,
	targetPath,
	downloadFolderRecord string) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:downloadBatch start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
		" downloadFolderRecord: ", downloadFolderRecord)

	err, listOutputTmp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			output := new(SLListOutput)
			_err, output := o.slClient.List(ctx, sourcePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"slClient:List start.",
					" sourcePath: ", sourcePath,
					" err: ", _err)
			}
			return _err, output
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"slClient:List failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}
	listOutput := new(SLListOutput)
	isValid := false
	if listOutput, isValid = listOutputTmp.(*SLListOutput); !isValid {
		Logger.WithContext(ctx).Error(
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
			Logger.WithContext(ctx).Error(
				"os.MkdirAll failed.",
				" itemPath: ", itemPath,
				" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"StarLight:downloadObjects failed.",
			" sourcePath: ", sourcePath,
			" targetPath: ", targetPath,
			" downloadFolderRecord: ", downloadFolderRecord,
			" err: ", err)
		return err
	}
	for _, folder := range folders {
		err = o.downloadBatch(
			ctx,
			folder.Path,
			targetPath,
			downloadFolderRecord)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"StarLight:downloadBatch failed.",
				" sourcePath: ", folder.Path,
				" targetPath: ", targetPath,
				" downloadFolderRecord: ", downloadFolderRecord,
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:downloadBatch finish.",
		" sourcePath: ", sourcePath)
	return nil
}

func (o *StarLight) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	objects []*SLObject,
	downloadFolderRecord string) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:downloadObjects start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
		" downloadFolderRecord: ", downloadFolderRecord)

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

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
	pool, err := ants.NewPool(o.slDownloadFileTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool for download Object failed.",
			" err: ", err)
		return err
	}
	defer pool.Release()
	for index, object := range objects {
		Logger.WithContext(ctx).Debug(
			"object content.",
			" index: ", index,
			" Path: ", object.Path,
			" size: ", object.Size)
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
				if _err := recover(); nil != _err {
					Logger.WithContext(ctx).Error(
						"downloadFile failed.",
						" err: ", _err)
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
				Logger.WithContext(ctx).Error(
					"StarLight:downloadPart failed.",
					" objectPath: ", itemObject.Path,
					" targetFile: ", targetFile,
					" err: ", _err)
				return
			}
			fileMutex.Lock()
			defer fileMutex.Unlock()
			f, _err := os.OpenFile(
				downloadFolderRecord,
				os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if nil != _err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"os.OpenFile failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" err: ", _err)
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
			_, _err = f.Write([]byte(itemObject.Path + "\n"))
			if nil != _err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"write file failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" objectPath: ", itemObject.Path,
					" err: ", _err)
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
			"StarLight:downloadObjects not all success.",
			" sourcePath: ", sourcePath)
		return errors.New("downloadObjects not all success")
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:downloadObjects finish.")
	return nil
}

func (o *StarLight) downloadPart(
	ctx context.Context,
	object *SLObject,
	targetFile string) (err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:downloadPart start.",
		" Path: ", object.Path,
		" targetFile: ", targetFile)

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
		Logger.WithContext(ctx).Error(
			"StarLight:resumeDownload failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"StarLight:downloadPart finish.")
	return
}

func (o *StarLight) resumeDownload(
	ctx context.Context,
	object *SLObject,
	input *SLDownloadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
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
			Logger.WithContext(ctx).Error(
				"StarLight:getDownloadCheckpointFile failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"StarLight:prepareTempFile failed.",
				" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
				" Size: ", dfc.TempFileInfo.Size,
				" err: ", err)
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"StarLight:updateCheckpointFile failed.",
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
		ctx, input, dfc)
	err = o.handleDownloadFileResult(
		ctx,
		dfc.TempFileInfo.TempFileUrl,
		enableCheckpoint,
		downloadFileError)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"StarLight:handleDownloadFileResult failed.",
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
		"StarLight:resumeDownload finish.")
	return nil
}

func (o *StarLight) downloadFileConcurrent(
	ctx context.Context,
	input *SLDownloadFileInput,
	dfc *SLDownloadCheckpoint) error {

	Logger.WithContext(ctx).Debug(
		"StarLight:downloadFileConcurrent start.")

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
		Logger.WithContext(ctx).Debug(
			"DownloadPartTask params.",
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
					_err := o.updateCheckpointFile(
						ctx,
						dfc,
						input.CheckpointFile)
					if nil != _err {
						Logger.WithContext(ctx).Error(
							"StarLight:updateCheckpointFile failed.",
							" checkpointFile: ", input.CheckpointFile,
							" err: ", _err)
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

					Logger.WithContext(ctx).Error(
						"StarLight:handleDownloadTaskResult failed.",
						" partNumber: ", task.PartNumber,
						" checkpointFile: ", input.CheckpointFile,
						" err: ", _err)
					downloadPartError.Store(_err)
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
		"StarLight:downloadFileConcurrent finish.")
	return nil
}

func (o *StarLight) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *SLDownloadCheckpoint,
	input *SLDownloadFileInput,
	object *SLObject) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"StarLight:getDownloadCheckpointFile start.",
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
			"StarLight:loadCheckpointFile failed.",
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

func (o *StarLight) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"StarLight:prepareTempFile start.",
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
				" parentDir: ", parentDir, " err: ", _err)
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
			"StarLight:createFile finish.",
			" tempFileURL: ", tempFileURL, " fileSize: ", fileSize)
		return nil
	}
	fd, err := os.OpenFile(
		tempFileURL,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0640)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.OpenFile failed.",
			" tempFileURL: ", tempFileURL, " err: ", err)
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" tempFileURL: ", tempFileURL, " err: ", errMsg)
		}
	}()
	if fileSize > 0 {
		_, err = fd.WriteAt([]byte("a"), fileSize-1)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"write file failed.",
				" tempFileURL: ", tempFileURL, " err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"StarLight:prepareTempFile finish.")
	return nil
}

func (o *StarLight) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"StarLight:createFile start.",
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

	Logger.WithContext(ctx).Debug(
		"StarLight:handleDownloadTaskResult start.",
		" partNum: ", partNum,
		" checkpointFile: ", checkpointFile)

	if _, ok := result.(*SLDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				Logger.WithContext(ctx).Warn(
					"StarLight:updateCheckpointFile failed.",
					" checkpointFile: ", checkpointFile,
					" err: ", _err)
			}
		}
	} else if result != ErrAbort {
		if _err, ok := result.(error); ok {
			err = _err
		}
	}
	Logger.WithContext(ctx).Debug(
		"StarLight:handleDownloadTaskResult finish.")
	return
}

func (o *StarLight) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	Logger.WithContext(ctx).Debug(
		"StarLight:handleDownloadFileResult start.",
		" tempFileURL: ", tempFileURL)

	if downloadFileError != nil {
		if !enableCheckpoint {
			_err := os.Remove(tempFileURL)
			if nil != _err {
				if !os.IsNotExist(_err) {
					Logger.WithContext(ctx).Error(
						"os.Remove failed.",
						" tempFileURL: ", tempFileURL, " err: ", _err)
				}
			}
		}
		Logger.WithContext(ctx).Debug(
			"StarLight.handleDownloadFileResult finish.",
			" tempFileURL: ", tempFileURL,
			" downloadFileError: ", downloadFileError)
		return downloadFileError
	}

	Logger.WithContext(ctx).Debug(
		"StarLight.handleDownloadFileResult finish.")
	return nil
}

func (o *StarLight) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *SLDownloadPartOutput) error {

	Logger.WithContext(ctx).Debug(
		"StarLight:UpdateDownloadFile start.",
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
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" filePath: ", filePath, " err: ", errMsg)
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
					" filePath: ", filePath, " err: ", writeError)
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
		"StarLight:UpdateDownloadFile finish. readTotal: ", readTotal)
	return nil
}

func (o *StarLight) Delete(
	ctx context.Context,
	input interface{}) (err error) {

	var path string
	if starLightDeleteInput, ok := input.(StarLightDeleteInput); ok {
		path = starLightDeleteInput.Path
	} else {
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"StarLight:Delete start.",
		" path: ", path)

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_err := o.slClient.Rm(ctx, path)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"slClient.Rm failed.",
					" path: ", path,
					" err: ", _err)
			}
			return _err
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"StarLight.Delete failed.",
			" path: ", path,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
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

	Logger.WithContext(ctx).Debug(
		"SLDownloadPartTask:Run start.",
		" objectPath: ", task.ObjectPath,
		" partNumber: ", task.PartNumber)

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
				Logger.WithContext(ctx).Error(
					"SlClient:DownloadChunks failed.",
					" objectPath: ", task.ObjectPath,
					" partNumber: ", task.PartNumber,
					" err: ", _err)
			}
			return _err, output
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SLDownloadPartTask:Run failed.",
			" objectPath: ", task.ObjectPath,
			" partNumber: ", task.PartNumber,
			" err: ", err)
		return err
	} else {
		downloadPartOutput := new(SLDownloadPartOutput)
		isValid := false
		if downloadPartOutput, isValid =
			downloadPartOutputTmp.(*SLDownloadPartOutput); !isValid {

			Logger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		Logger.WithContext(ctx).Debug(
			"SlClient.DownloadChunks finish.",
			" objectPath: ", task.ObjectPath,
			" partNumber: ", task.PartNumber)
		defer func() {
			errMsg := downloadPartOutput.Body.Close()
			if errMsg != nil {
				Logger.WithContext(ctx).Warn(
					"close response body failed.",
					" objectPath: ", task.ObjectPath,
					" partNumber: ", task.PartNumber)
			}
		}()
		err = task.Sl.UpdateDownloadFile(
			ctx,
			task.TempFileURL,
			task.Offset,
			downloadPartOutput)
		if nil != err {
			if !task.EnableCheckpoint {
				Logger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					" objectPath: ", task.ObjectPath,
					" partNumber: ", task.PartNumber)
			}
			Logger.WithContext(ctx).Error(
				"SL.updateDownloadFile failed.",
				" objectPath: ", task.ObjectPath,
				" partNumber: ", task.PartNumber,
				" err: ", err)
			return err
		}
		Logger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.",
			" objectPath: ", task.ObjectPath,
			" partNumber: ", task.PartNumber)
		return downloadPartOutput
	}
}
