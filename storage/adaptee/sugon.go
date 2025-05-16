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
)

type Sugon struct {
	sugonClient *SugonClient
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

	Logger.WithContext(ctx).Debug(
		"Sugon:Init start.",
		" user: ", "***",
		" password: ", "***",
		" endpoint: ", endpoint,
		" url: ", url,
		" orgId: ", orgId,
		" clusterId: ", clusterId,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

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

	Logger.WithContext(ctx).Debug(
		"Sugon:Init finish.")
	return nil
}

func (o *Sugon) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	Logger.WithContext(ctx).Debug(
		"Sugon:loadCheckpointFile start.",
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
		"Sugon:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *Sugon) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *SugonDownloadCheckpoint) {

	Logger.WithContext(ctx).Debug(
		"Sugon:sliceObject start.",
		" objectSize: ", objectSize,
		" partSize: ", partSize)

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
	Logger.WithContext(ctx).Debug(
		"Sugon:sliceObject finish.")
}

func (o *Sugon) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	Logger.WithContext(ctx).Debug(
		"Sugon:updateCheckpointFile start.",
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
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"Sugon:Upload start.",
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
				"Sugon.uploadFolder failed.",
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
				"Sugon.uploadFile failed.",
				" sourcePath: ", sourcePath,
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"Sugon:Upload finish.")
	return nil
}

func (o *Sugon) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:uploadFolder start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
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

	path := targetPath + filepath.Base(sourcePath)
	err = o.sugonClient.Mkdir(ctx, path)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"sugonClient.Mkdir failed.",
			" path: ", path,
			" err: ", err)
		return err
	}

	pool, err := ants.NewPool(DefaultSugonUploadFileTaskNum)
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

			if sourcePath == filePath {
				Logger.WithContext(ctx).Debug(
					"root dir no need todo.")
				return nil
			}

			wg.Add(1)
			err = pool.Submit(func() {
				defer func() {
					wg.Done()
					if _err := recover(); nil != _err {
						Logger.WithContext(ctx).Error(
							"Sugon:uploadFileResume failed.",
							" err: ", _err)
						isAllSuccess = false
					}
				}()
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
				if fileInfo.IsDir() {
					_err = o.sugonClient.Mkdir(ctx, objectPath)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"sugonClient.Mkdir failed.",
							" objectPath: ", objectPath,
							" err: ", _err)
						return
					}
				} else {
					_err = o.uploadFile(
						ctx,
						filePath,
						objectPath,
						needPure)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"Sugon:uploadFile failed.",
							" filePath: ", filePath,
							" objectPath: ", objectPath,
							" err: ", _err)
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
			"Sugon:uploadFolder not all success.",
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
		"Sugon:uploadFolder finish.")
	return nil
}

func (o *Sugon) uploadFile(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:uploadFile start.",
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

	if DefaultSugonUploadMultiSize < sourceFileStat.Size() {
		err = o.uploadFileResume(
			ctx,
			sourceFile,
			objectPath,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"Sugon:uploadFileResume failed.",
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
				"Sugon:uploadFileStream failed.",
				" sourceFile: ", sourceFile,
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"Sugon:uploadFile finish.")
	return err
}

func (o *Sugon) uploadFileStream(
	ctx context.Context,
	sourceFile,
	objectPath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:uploadFileStream start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath)

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

	fileName := filepath.Base(objectPath)
	path := filepath.Dir(objectPath)
	err = o.sugonClient.Upload(
		ctx,
		fileName,
		path,
		fd)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"sugonClient.Upload failed.",
			" fileName: ", fileName,
			" path: ", path,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"Sugon:uploadFileStream finish.")
	return err
}

func (o *Sugon) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:uploadFileResume start.",
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" needPure: ", needPure)

	uploadFileInput := new(SugonUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.FileName = filepath.Base(sourceFile)
	uploadFileInput.RelativePath = filepath.Base(sourceFile)
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + ".upload_file_record"
	uploadFileInput.TaskNum = DefaultSugonUploadMultiTaskNum
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
			"Sugon:resumeUpload failed.",
			" sourceFile: ", sourceFile,
			" objectPath: ", objectPath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"Sugon:uploadFileResume finish.")
	return err
}

func (o *Sugon) resumeUpload(
	ctx context.Context,
	sourceFile,
	objectPath string,
	input *SugonUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:resumeUpload start.",
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
			Logger.WithContext(ctx).Error(
				"Sugon:getUploadCheckpointFile failed.",
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
				"Sugon:prepareUpload failed.",
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"Sugon:updateCheckpointFile failed.",
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
			"Sugon:uploadPartConcurrent failed.",
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
			"Sugon:completeParts failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"Sugon:resumeUpload finish.")
	return err
}

func (o *Sugon) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	ufc *SugonUploadCheckpoint,
	input *SugonUploadFileInput) error {

	Logger.WithContext(ctx).Debug(
		"Sugon:uploadPartConcurrent start.",
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

				Logger.WithContext(ctx).Error(
					"Sugon:handleUploadTaskResult failed.",
					" ChunkNumber: ", task.ChunkNumber,
					" checkpointFile: ", input.CheckpointFile,
					" err: ", _err)
				uploadPartError.Store(_err)
			}
			Logger.WithContext(ctx).Debug(
				"Sugon:handleUploadTaskResult finish.")
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
		"Sugon:uploadPartConcurrent finish.")
	return nil
}

func (o *Sugon) getUploadCheckpointFile(
	ctx context.Context,
	ufc *SugonUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *SugonUploadFileInput) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:getUploadCheckpointFile start.")

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
			"Sugon:loadCheckpointFile failed.",
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
			"Sugon:loadCheckpointFile finish.",
			" checkpointFilePath: ", checkpointFilePath)
		return false, nil
	}
	Logger.WithContext(ctx).Debug(
		"Sugon:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *Sugon) prepareUpload(
	ctx context.Context,
	objectPath string,
	ufc *SugonUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *SugonUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:prepareUpload start.",
		" objectPath: ", objectPath)

	ufc.ObjectPath = objectPath
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = SugonFileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()

	err = o.sliceFile(ctx, input.PartSize, ufc)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Sugon:sliceFile failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"Sugon:prepareUpload finish.")
	return err
}

func (o *Sugon) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *SugonUploadCheckpoint) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:sliceFile start.",
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

	if partSize > DefaultSugonMaxPartSize {
		Logger.WithContext(ctx).Error(
			"upload file part too large.",
			" partSize: ", partSize,
			" maxPartSize: ", DefaultSugonMaxPartSize)
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

	Logger.WithContext(ctx).Debug(
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

	Logger.WithContext(ctx).Debug(
		"Sugon:handleUploadTaskResult start.",
		" checkpointFilePath: ", checkpointFilePath,
		" partNum: ", partNum)

	if _, ok := result.(*SugonBaseResponse); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"Sugon:updateCheckpointFile failed.",
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
		"Sugon:handleUploadTaskResult finish.")
	return
}

func (o *Sugon) completeParts(
	ctx context.Context,
	input *SugonUploadFileInput,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:completeParts start.",
		" enableCheckpoint: ", enableCheckpoint,
		" checkpointFilePath: ", checkpointFilePath)

	err = o.sugonClient.MergeChunks(
		ctx,
		input.FileName,
		filepath.Dir(input.ObjectPath),
		input.RelativePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"sugonClient.MergeChunks failed.",
			" FileName: ", input.FileName,
			" Path: ", filepath.Dir(input.ObjectPath),
			" RelativePath: ", input.RelativePath,
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

	Logger.WithContext(ctx).Debug(
		"SugonUploadPartTask:Run start.",
		" sourceFile: ", sourceFile)

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

	readerWrapper.TotalCount = task.CurrentChunkSize
	readerWrapper.Mark = task.Offset
	if _, err = fd.Seek(task.Offset, io.SeekStart); nil != err {
		Logger.WithContext(ctx).Error(
			"fd.Seek failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	err = task.SClient.UploadChunks(
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

	if nil != err {
		Logger.WithContext(ctx).Error(
			"SClient.UploadChunks failed.",
			" task.File: ", task.File,
			" task.FileName: ", task.FileName,
			" task.Path: ", task.Path,
			" task.RelativePath: ", task.RelativePath,
			" task.ChunkNumber: ", task.ChunkNumber,
			" task.TotalChunks: ", task.TotalChunks,
			" task.TotalSize: ", task.TotalSize,
			" task.ChunkSize: ", task.ChunkSize,
			" task.CurrentChunkSize: ", task.CurrentChunkSize,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"SugonUploadPartTask:Run finish.")
	return err
}

func (o *Sugon) Download(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	if sugonDownloadInput, ok := input.(SugonDownloadInput); ok {
		sourcePath = sugonDownloadInput.SourcePath
		targetPath = sugonDownloadInput.TargetPath
	} else {
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"Sugon:Download start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath)

	var start int32 = 0
	var limit int32 = DefaultSugonListLimit
	for {
		sugonListResponseData := new(SugonListResponseData)
		err, sugonListResponseData = o.sugonClient.List(
			ctx,
			sourcePath,
			start,
			limit)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"sugonClient:List start.",
				" sourcePath: ", sourcePath,
				" start: ", start,
				" limit: ", limit,
				" err: ", err)
			return err
		}

		for _, folder := range sugonListResponseData.Children {
			itemPath := targetPath + folder.Path
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
			sugonListResponseData.FileList)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"Sugon:downloadObjects failed.",
				" sourcePath: ", sourcePath,
				" targetPath: ", targetPath,
				" err: ", err)
			return err
		}

		for _, folder := range sugonListResponseData.Children {
			var sugonDownloadInput SugonDownloadInput
			sugonDownloadInput.SourcePath = folder.Path
			sugonDownloadInput.TargetPath = targetPath

			err = o.Download(ctx, sugonDownloadInput)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"Sugon:Download failed.",
					" sourcePath: ", folder.Path,
					" targetPath: ", targetPath,
					" err: ", err)
				return err
			}
		}

		start = start + limit
		if start >= sugonListResponseData.Total {
			break
		}
	}

	Logger.WithContext(ctx).Debug(
		"Sugon:Download finish.",
		" sourcePath: ", sourcePath)
	return nil
}

func (o *Sugon) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	objects []*SugonFileInfo) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:downloadObjects start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath)

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	path := strings.TrimSuffix(targetPath, "/") + sourcePath
	downloadFolderRecord :=
		filepath.Dir(path) +
			filepath.Base(path) +
			".download_folder_record"
	Logger.WithContext(ctx).Debug(
		"downloadFolderRecord file info.",
		" downloadFolderRecord: ", downloadFolderRecord)

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
	pool, err := ants.NewPool(DefaultSugonDownloadFileTaskNum)
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
			" Path: ", object.Path,
			" IsDirectory: ", object.IsDirectory)

		itemObject := object

		if itemObject.IsDirectory {
			Logger.WithContext(ctx).Info(
				"dir already process, pass.",
				" Path: ", itemObject.Path)
			continue
		}

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
			_err := o.downloadPart(
				ctx,
				itemObject,
				targetPath+itemObject.Path)
			if nil != _err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"Sugon:downloadPart failed.",
					" objectPath: ", itemObject.Path,
					" targetFile: ", targetPath+itemObject.Path,
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
			"Sugon:downloadObjects not all success.",
			" sourcePath: ", sourcePath)
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
		"Sugon:downloadObjects finish.")
	return nil
}

func (o *Sugon) downloadPart(
	ctx context.Context,
	object *SugonFileInfo,
	targetFile string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:downloadPart start.",
		" ObjectPath: ", object.Path,
		" targetFile: ", targetFile)

	downloadFileInput := new(SugonDownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + ".download_file_record"
	downloadFileInput.TaskNum = DefaultSugonDownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize

	err = o.resumeDownload(
		ctx,
		object,
		downloadFileInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Sugon:resumeDownload failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"Sugon:downloadPart finish.")
	return
}

func (o *Sugon) resumeDownload(
	ctx context.Context,
	object *SugonFileInfo,
	input *SugonDownloadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
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
			Logger.WithContext(ctx).Error(
				"Sugon:getDownloadCheckpointFile failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"Sugon:prepareTempFile failed.",
				" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
				" Size: ", dfc.TempFileInfo.Size,
				" err: ", err)
			return err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"Sugon:updateCheckpointFile failed.",
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
			"Sugon:handleDownloadFileResult failed.",
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
		"Sugon:resumeDownload finish.")
	return nil
}

func (o *Sugon) downloadFileConcurrent(
	ctx context.Context,
	input *SugonDownloadFileInput,
	dfc *SugonDownloadCheckpoint) error {

	Logger.WithContext(ctx).Debug(
		"Sugon:downloadFileConcurrent start.")

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
							"Sugon:updateCheckpointFile failed.",
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
						"Sugon:handleDownloadTaskResult failed.",
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
		"Sugon:downloadFileConcurrent finish.")
	return nil
}

func (o *Sugon) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *SugonDownloadCheckpoint,
	input *SugonDownloadFileInput,
	object *SugonFileInfo) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"Sugon:getDownloadCheckpointFile start.",
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
			"Sugon:loadCheckpointFile failed.",
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

func (o *Sugon) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"Sugon:prepareTempFile start.",
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
			"same file exists.",
			" parentDir: ", parentDir)
		return fmt.Errorf(
			"cannot create folder: %s due to a same file exists",
			parentDir)
	}

	err = o.createFile(ctx, tempFileURL, fileSize)
	if nil == err {
		Logger.WithContext(ctx).Debug(
			"Sugon:createFile finish.",
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
		"Sugon:prepareTempFile finish.")
	return nil
}

func (o *Sugon) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"Sugon:createFile start.",
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

	Logger.WithContext(ctx).Debug(
		"Sugon:handleDownloadTaskResult start.",
		" partNum: ", partNum,
		" checkpointFile: ", checkpointFile)

	if _, ok := result.(*SugonDownloadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				Logger.WithContext(ctx).Warn(
					"Sugon:updateCheckpointFile failed.",
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
		"Sugon:handleDownloadTaskResult finish.")
	return
}

func (o *Sugon) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	Logger.WithContext(ctx).Debug(
		"Sugon:handleDownloadFileResult start.",
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
			"Sugon.handleDownloadFileResult finish.",
			" tempFileURL: ", tempFileURL,
			" downloadFileError: ", downloadFileError)
		return downloadFileError
	}

	Logger.WithContext(ctx).Debug(
		"Sugon.handleDownloadFileResult finish.")
	return nil
}

func (o *Sugon) UpdateDownloadFile(
	ctx context.Context,
	filePath string,
	offset int64,
	downloadPartOutput *SugonDownloadPartOutput) error {

	Logger.WithContext(ctx).Debug(
		"Sugon:UpdateDownloadFile start.",
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
		"Sugon:UpdateDownloadFile finish. readTotal: ", readTotal)
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

	Logger.WithContext(ctx).Debug(
		"SugonDownloadPartTask:Run start.",
		" partNumber: ", task.PartNumber)

	err, downloadPartOutput :=
		task.SClient.DownloadChunks(
			ctx,
			task.ObjectPath,
			task.Range)

	if nil == err {
		Logger.WithContext(ctx).Debug(
			"SClient.DownloadChunks finish.")
		defer func() {
			errMsg := downloadPartOutput.Body.Close()
			if errMsg != nil {
				Logger.WithContext(ctx).Warn(
					"close response body failed.")
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
					" partNumber: ", task.PartNumber)
			}
			Logger.WithContext(ctx).Error(
				"Sugon.updateDownloadFile failed.",
				" err: ", _err)
			return _err
		}
		Logger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.")
		return downloadPartOutput
	}

	Logger.WithContext(ctx).Error(
		"SugonDownloadPartTask:Run failed.",
		" err: ", err)
	return err
}
