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
	sClient *ScowClient
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

	Logger.WithContext(ctx).Debug(
		"Scow:Init start.",
		" username: ", "***",
		" password: ", "***",
		" endpoint: ", endpoint,
		" url: ", url,
		" clusterId: ", clusterId,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

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

	Logger.WithContext(ctx).Debug(
		"Scow:Init finish.")
	return nil
}

func (o *Scow) Mkdir(
	ctx context.Context,
	input interface{}) (err error) {

	var path string
	if scowMkdirInput, ok := input.(ScowMkdirInput); ok {
		path = scowMkdirInput.Path
	} else {
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"Scow:Mkdir start.",
		" path: ", path)

	err = RetryV1(
		ctx,
		ScowAttempts,
		ScowDelay*time.Second,
		func() error {
			_err := o.sClient.Mkdir(ctx, path)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"sClient.Mkdir failed.",
					" path: ", path,
					" err: ", _err)
			}
			return _err
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Scow.Mkdir failed.",
			" path: ", path,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"Scow:Mkdir finish.")
	return nil
}

func (o *Scow) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	Logger.WithContext(ctx).Debug(
		"Scow:loadCheckpointFile start.",
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
		"Scow:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *Scow) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *ScowDownloadCheckpoint) {

	Logger.WithContext(ctx).Debug(
		"Scow:sliceObject start.",
		" objectSize: ", objectSize,
		" partSize: ", partSize)

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
	Logger.WithContext(ctx).Debug(
		"Scow:sliceObject finish.")
}

func (o *Scow) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	Logger.WithContext(ctx).Debug(
		"Scow:updateCheckpointFile start.",
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
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"Scow:Upload start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
		" needPure: ", needPure)

	err = RetryV1(
		ctx,
		ScowAttempts,
		ScowDelay*time.Second,
		func() error {
			_err := o.sClient.Mkdir(ctx, targetPath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"sClient.Mkdir failed.",
					" targetPath: ", targetPath,
					" err: ", _err)
			}
			return _err
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"sClient.Mkdir failed.",
			" targetPath: ", targetPath,
			" err: ", err)
		return err
	}

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
				"Scow.uploadFolder failed.",
				" sourcePath: ", sourcePath,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"Scow.uploadFile failed.",
				" sourcePath: ", sourcePath,
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"Scow:Upload finish.")
	return nil
}

func (o *Scow) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:uploadFolder start.",
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

	scowMkdirInput := ScowMkdirInput{}
	scowMkdirInput.Path = targetPath
	err = o.Mkdir(ctx, scowMkdirInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Scow.Mkdir failed.",
			" targetPath: ", targetPath,
			" err: ", err)
		return err
	}

	pool, err := ants.NewPool(DefaultScowUploadFileTaskNum)
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
							"Scow:uploadFileResume failed.",
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
					Logger.WithContext(ctx).Debug(
						"file is dir.",
						" fileName: ", fileInfo.Name(),
						" filePath: ", filePath,
						" relPath: ", relPath,
						" objectPath: ", objectPath)

					input := ScowMkdirInput{}
					input.Path = objectPath
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"Scow.Mkdir failed.",
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
			"Scow:uploadFolder not all success.",
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
							"Scow:uploadFileResume failed.",
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
					_err = o.uploadFile(
						ctx,
						filePath,
						objectPath,
						needPure)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"Scow:uploadFile failed.",
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
			"Scow:uploadFolder not all success.",
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
		"Scow:uploadFolder finish.")
	return nil
}

func (o *Scow) uploadFile(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:uploadFile start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" needPure: ", needPure)

	sourceFileStat, err := os.Stat(sourceFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	if DefaultScowUploadMultiSize < sourceFileStat.Size() {
		err, exist := o.sClient.CheckExist(ctx, objectPath)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"sClient.CheckExist failed.",
				" err: ", err)
			return err
		}
		if true == exist {
			Logger.WithContext(ctx).Info(
				"Path already exist, no need to upload.",
				" objectPath: ", objectPath)
			return err
		}
		err = o.uploadFileResume(
			ctx,
			sourceFile,
			objectPath,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"Scow:uploadFileResume failed.",
				" sourceFile: ", sourceFile,
				" objectPath: ", objectPath,
				" needPure: ", needPure,
				" err: ", err)
			return err
		}
	} else {
		err = o.uploadFileStream(
			ctx,
			sourceFile,
			objectPath)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"Scow:uploadFileStream failed.",
				" sourceFile: ", sourceFile,
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"Scow:uploadFile finish.")
	return err
}

func (o *Scow) uploadFileStream(
	ctx context.Context,
	sourceFile,
	objectPath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:uploadFileStream start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath)

	err = RetryV1(
		ctx,
		ScowAttempts,
		ScowDelay*time.Second,
		func() error {
			fd, _err := os.Open(sourceFile)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"os.Open failed.",
					" sourceFile: ", sourceFile,
					" err: ", _err)
				return _err
			}
			defer func() {
				errMsg := fd.Close()
				if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
					Logger.WithContext(ctx).Warn(
						"close file failed.",
						" sourceFile: ", sourceFile,
						" err: ", errMsg)
				}
			}()

			_err = o.sClient.Upload(
				ctx,
				sourceFile,
				objectPath,
				fd)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"sClient.Upload failed.",
					" sourceFile: ", sourceFile,
					" objectPath: ", objectPath,
					" err: ", _err)
			}
			return _err
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Scow.uploadFileStream failed.",
			" sourceFile: ", sourceFile,
			" objectPath: ", objectPath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"Scow:uploadFileStream finish.")
	return err
}

func (o *Scow) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:uploadFileResume start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" needPure: ", needPure)

	uploadFileInput := new(ScowUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.FileName = filepath.Base(sourceFile)
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + UploadFileRecordSuffix
	uploadFileInput.TaskNum = DefaultScowUploadMultiTaskNum
	uploadFileInput.PartSize = DefaultPartSize
	if uploadFileInput.PartSize < DefaultScowMinPartSize {
		uploadFileInput.PartSize = DefaultScowMinPartSize
	} else if uploadFileInput.PartSize > DefaultScowMaxPartSize {
		uploadFileInput.PartSize = DefaultScowMaxPartSize
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
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" sourceFile: ", sourceFile,
				" err: ", errMsg)
		}
	}()
	hash := md5.New()
	if _, err = io.Copy(hash, fd); nil != err {
		Logger.WithContext(ctx).Error(
			"io.Copy failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}
	md5Value := hex.EncodeToString(hash.Sum(nil))
	uploadFileInput.Md5 = md5Value

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
			"Scow:resumeUpload failed.",
			" sourceFile: ", sourceFile,
			" objectPath: ", objectPath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"Scow:uploadFileResume finish.")
	return err
}

func (o *Scow) resumeUpload(
	ctx context.Context,
	sourceFile,
	objectPath string,
	input *ScowUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:resumeUpload start.",
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
			Logger.WithContext(ctx).Error(
				"Scow:getUploadCheckpointFile failed.",
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
				"Scow:prepareUpload failed.",
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"Scow:updateCheckpointFile failed.",
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
			"Scow:uploadPartConcurrent failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	err = o.completeParts(
		ctx,
		input,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Scow:completeParts failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"Scow:resumeUpload finish.")
	return err
}

func (o *Scow) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	ufc *ScowUploadCheckpoint,
	input *ScowUploadFileInput) error {

	Logger.WithContext(ctx).Debug(
		"Scow:uploadPartConcurrent start.",
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
					"Scow:handleUploadTaskResult failed.",
					" partNumber: ", task.PartNumber,
					" checkpointFile: ", input.CheckpointFile,
					" err: ", _err)
				uploadPartError.Store(_err)
			}
			Logger.WithContext(ctx).Debug(
				"Scow:handleUploadTaskResult finish.")
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
		"Scow:uploadPartConcurrent finish.")
	return nil
}

func (o *Scow) getUploadCheckpointFile(
	ctx context.Context,
	ufc *ScowUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *ScowUploadFileInput) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:getUploadCheckpointFile start.")

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
			"Scow:loadCheckpointFile failed.",
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
			"Scow:loadCheckpointFile finish.",
			" checkpointFilePath: ", checkpointFilePath)
		return false, nil
	}
	Logger.WithContext(ctx).Debug(
		"Scow:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *Scow) prepareUpload(
	ctx context.Context,
	objectPath string,
	ufc *ScowUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *ScowUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:prepareUpload start.",
		" objectPath: ", objectPath)

	ufc.ObjectPath = objectPath
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = ScowFileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()

	err = o.sliceFile(ctx, input.PartSize, ufc)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Scow:sliceFile failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"Scow:prepareUpload finish.")
	return err
}

func (o *Scow) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *ScowUploadCheckpoint) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:sliceFile start.",
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

	if partSize > DefaultScowMaxPartSize {
		Logger.WithContext(ctx).Error(
			"upload file part too large.",
			" partSize: ", partSize,
			" maxPartSize: ", DefaultScowMaxPartSize)
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
	Logger.WithContext(ctx).Debug(
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

	Logger.WithContext(ctx).Debug(
		"Scow:handleUploadTaskResult start.",
		" checkpointFilePath: ", checkpointFilePath,
		" partNum: ", partNum)

	if _, ok := result.(*ScowBaseMessageResponse); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"Scow:updateCheckpointFile failed.",
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
		"Scow:handleUploadTaskResult finish.")
	return
}

func (o *Scow) completeParts(
	ctx context.Context,
	input *ScowUploadFileInput,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:completeParts start.",
		" enableCheckpoint: ", enableCheckpoint,
		" checkpointFilePath: ", checkpointFilePath,
		" FileName: ", input.FileName,
		" ObjectPath: ", input.ObjectPath,
		" Md5: ", input.Md5)

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
				Logger.WithContext(ctx).Error(
					"sClient.MergeChunks failed.",
					" FileName: ", input.FileName,
					" Path: ", filepath.Dir(input.ObjectPath),
					" Md5: ", input.Md5,
					" err: ", _err)
			}
			return _err
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Scow.completeParts failed.",
			" FileName: ", input.FileName,
			" Path: ", filepath.Dir(input.ObjectPath),
			" Md5: ", input.Md5,
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

	Logger.WithContext(ctx).Debug(
		"ScowUploadPartTask:Run start.",
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
			_err, respTmp := task.SClient.UploadChunks(
				ctx,
				task.FileName,
				task.ObjectPath,
				task.Md5,
				task.PartNumber,
				readerWrapper)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"SClient.UploadChunks failed.",
					" task.FileName: ", task.FileName,
					" task.ObjectPath: ", task.ObjectPath,
					" task.Md5: ", task.Md5,
					" task.PartNumber: ", task.PartNumber,
					" err: ", _err)
				return _err, nil
			}
			return _err, respTmp
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowUploadPartTask.Run failed.",
			" task.FileName: ", task.FileName,
			" task.ObjectPath: ", task.ObjectPath,
			" task.Md5: ", task.Md5,
			" task.PartNumber: ", task.PartNumber,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"ScowUploadPartTask:Run finish.",
		" objectPath: ", task.ObjectPath,
		" partNumber: ", task.PartNumber)
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
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"Scow:Download start.",
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
			"Scow:downloadBatch failed.",
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
		"Scow:Download finish.")
	return nil
}

func (o *Scow) downloadBatch(
	ctx context.Context,
	sourcePath,
	targetPath,
	downloadFolderRecord string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:downloadBatch start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
		" downloadFolderRecord: ", downloadFolderRecord)

	err, scowListResponseBodyTmp := RetryV4(
		ctx,
		Attempts,
		Delay*time.Second,
		func() (error, interface{}) {
			output := new(ScowListResponseBody)
			_err, output := o.sClient.List(ctx, sourcePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"sClient:List start.",
					" sourcePath: ", sourcePath,
					" err: ", _err)
			}
			return _err, output
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"sClient:List start.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}
	scowListResponseBody := new(ScowListResponseBody)
	isValid := false
	if scowListResponseBody, isValid =
		scowListResponseBodyTmp.(*ScowListResponseBody); !isValid {

		Logger.WithContext(ctx).Error(
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
			"Scow:downloadObjects failed.",
			" sourcePath: ", sourcePath,
			" targetPath: ", targetPath,
			" downloadFolderRecord: ", downloadFolderRecord,
			" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"Scow:downloadBatch failed.",
				" sourcePath: ", folder.PathExt,
				" targetPath: ", targetPath,
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"Scow:downloadBatch finish.",
		" sourcePath: ", sourcePath)
	return nil
}

func (o *Scow) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	objects []*ScowObject,
	downloadFolderRecord string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:downloadObjects start.",
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
	pool, err := ants.NewPool(DefaultScowDownloadFileTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool for download object failed.",
			" err: ", err)
		return err
	}
	defer pool.Release()
	for index, object := range objects {
		Logger.WithContext(ctx).Debug(
			"object content.",
			" index: ", index,
			" Name: ", object.Name,
			" size: ", object.Size)
		itemObject := object
		if _, exists := fileMap[itemObject.PathExt]; exists {
			Logger.WithContext(ctx).Info(
				"file already success.",
				" objectPath: ", itemObject.PathExt)
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
				itemObject.PathExt
			_err := o.downloadPart(
				ctx,
				itemObject,
				targetFile)
			if nil != _err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"Scow:downloadPart failed.",
					" objectName: ", itemObject.Name,
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
			_, _err = f.Write([]byte(itemObject.PathExt + "\n"))
			if nil != _err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"write file failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" objectPath: ", itemObject.PathExt,
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
			"Scow:downloadObjects not all success.",
			" sourcePath: ", sourcePath)
		return errors.New("downloadObjects not all success")
	}

	Logger.WithContext(ctx).Debug(
		"Scow:downloadObjects finish.")
	return nil
}

func (o *Scow) downloadPart(
	ctx context.Context,
	object *ScowObject,
	targetFile string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:downloadPart start.",
		" Name: ", object.Name,
		" targetFile: ", targetFile)

	downloadFileInput := new(ScowDownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + DownloadFileRecordSuffix
	downloadFileInput.TaskNum = DefaultScowDownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize

	err = o.resumeDownload(
		ctx,
		object,
		downloadFileInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Scow:resumeDownload failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"Scow:downloadPart finish.")
	return
}

func (o *Scow) resumeDownload(
	ctx context.Context,
	object *ScowObject,
	input *ScowDownloadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
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
			Logger.WithContext(ctx).Error(
				"Scow:getDownloadCheckpointFile failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"Scow:prepareTempFile failed.",
				" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
				" Size: ", dfc.TempFileInfo.Size,
				" err: ", err)
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"Scow:updateCheckpointFile failed.",
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
			"Scow:handleDownloadFileResult failed.",
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
		"Scow:resumeDownload finish.")
	return nil
}

func (o *Scow) downloadFileConcurrent(
	ctx context.Context,
	input *ScowDownloadFileInput,
	dfc *ScowDownloadCheckpoint) error {

	Logger.WithContext(ctx).Debug(
		"Scow:downloadFileConcurrent start.")

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
							"Scow:updateCheckpointFile failed.",
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
						"Scow:handleDownloadTaskResult failed.",
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
		"Scow:downloadFileConcurrent finish.")
	return nil
}

func (o *Scow) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *ScowDownloadCheckpoint,
	input *ScowDownloadFileInput,
	object *ScowObject) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"Scow:getDownloadCheckpointFile start.",
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
			"Scow:loadCheckpointFile failed.",
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

func (o *Scow) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"Scow:prepareTempFile start.",
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
			"Scow:createFile finish.",
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
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
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
		"Scow:prepareTempFile finish.")
	return nil
}

func (o *Scow) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"Scow:createFile start.",
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

	Logger.WithContext(ctx).Debug(
		"Scow:handleDownloadTaskResult start.",
		" partNum: ", partNum,
		" checkpointFile: ", checkpointFile)

	if _, ok := result.(*ScowDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				Logger.WithContext(ctx).Warn(
					"Scow:updateCheckpointFile failed.",
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
		"Scow:handleDownloadTaskResult finish.")
	return
}

func (o *Scow) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	Logger.WithContext(ctx).Debug(
		"Scow:handleDownloadFileResult start.",
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
			"Scow.handleDownloadFileResult finish.",
			" tempFileURL: ", tempFileURL,
			" downloadFileError: ", downloadFileError)
		return downloadFileError
	}

	Logger.WithContext(ctx).Debug(
		"Scow.handleDownloadFileResult finish.")
	return nil
}

func (o *Scow) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *ScowDownloadPartOutput) error {

	Logger.WithContext(ctx).Debug(
		"Scow:UpdateDownloadFile start.",
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
		"Scow:UpdateDownloadFile finish.",
		" readTotal: ", readTotal)
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
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"Scow:Delete start.",
		" path: ", path,
		" target: ", target)

	err = RetryV1(
		ctx,
		ScowAttempts,
		ScowDelay*time.Second,
		func() error {
			_err := o.sClient.Delete(ctx, path, target)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"sClient.Delete failed.",
					" path: ", path,
					" target: ", target,
					" err: ", _err)
			}
			return _err
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Scow.Delete failed.",
			" path: ", path,
			" target: ", target,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
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

	Logger.WithContext(ctx).Debug(
		"ScowDownloadPartTask:Run start.",
		" objectPath: ", task.ObjectPath,
		" partNumber: ", task.PartNumber)

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
				Logger.WithContext(ctx).Error(
					"SClient:DownloadChunks failed.",
					" objectPath: ", task.ObjectPath,
					" partNumber: ", task.PartNumber,
					" err: ", _err)
			}
			return _err, output
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowDownloadPartTask:Run failed.",
			" objectPath: ", task.ObjectPath,
			" partNumber: ", task.PartNumber,
			" err: ", err)
		return err
	} else {
		downloadPartOutput := new(ScowDownloadPartOutput)
		isValid := false
		if downloadPartOutput, isValid =
			downloadPartOutputTmp.(*ScowDownloadPartOutput); !isValid {

			Logger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		Logger.WithContext(ctx).Debug(
			"ScowDownloadPartTask.Run finish.",
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
		_err := task.S.UpdateDownloadFile(
			ctx,
			task.TempFileURL,
			task.Offset,
			downloadPartOutput)
		if nil != _err {
			if !task.EnableCheckpoint {
				Logger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					" objectPath: ", task.ObjectPath,
					" partNumber: ", task.PartNumber)
			}
			Logger.WithContext(ctx).Error(
				"Scow.updateDownloadFile failed.",
				" objectPath: ", task.ObjectPath,
				" partNumber: ", task.PartNumber,
				" err: ", _err)
			return _err
		}
		Logger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.",
			" objectPath: ", task.ObjectPath,
			" partNumber: ", task.PartNumber)
		return downloadPartOutput
	}
}
