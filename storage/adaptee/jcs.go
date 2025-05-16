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

	Logger.WithContext(ctx).Debug(
		"JCS:Init start.",
		" accessKey: ", "***",
		" secretKey: ", "***",
		" endPoint: ", endPoint,
		" authService: ", authService,
		" authRegion: ", authRegion,
		" userID: ", userID,
		" bucketID: ", bucketID,
		" bucketName: ", bucketName,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

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

	Logger.WithContext(ctx).Debug(
		"JCS:Init finish.")
	return nil
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
			dfc.DownloadParts[cnt-1].Length = value
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
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"JCS:Upload start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
		" packageId: ", packageId,
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
		err = o.uploadFolder(
			ctx,
			packageId,
			sourcePath,
			targetPath,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS.uploadFolder failed.",
				" packageId: ", packageId,
				" sourcePath: ", sourcePath,
				" targetPath: ", targetPath,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"JCS.uploadFile failed.",
				" packageId: ", packageId,
				" sourcePath: ", sourcePath,
				" objectPath: ", objectPath,
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
	packageId int32,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFolder start.",
		" packageId: ", packageId,
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
	if 0 < len(path) &&
		'/' != path[len(path)-1] {

		path = path + "/"
	}
	err = o.jcsClient.UploadFile(
		ctx,
		packageId,
		path,
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.UploadFile mkdir failed.",
			" packageId: ", packageId,
			" path: ", path,
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
							"JCS:uploadFileResume failed.",
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
					if 0 < len(objectPath) &&
						'/' != objectPath[len(objectPath)-1] {

						objectPath = objectPath + "/"
					}
					_err = o.jcsClient.UploadFile(
						ctx,
						packageId,
						objectPath,
						nil)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"jcsClient.UploadFile mkdir failed.",
							" packageId: ", packageId,
							" objectPath: ", objectPath,
							" err: ", _err)
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
						Logger.WithContext(ctx).Error(
							"JCS:uploadFile failed.",
							" packageId: ", packageId,
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
	packageId int32,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFile start.",
		" packageId: ", packageId,
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

	if DefaultJCSUploadMultiSize < sourceFileStat.Size() {
		err = o.uploadFileResume(
			ctx,
			packageId,
			sourceFile,
			objectPath,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:uploadFileResume failed.",
				" packageId: ", packageId,
				" sourceFile: ", sourceFile,
				" objectPath: ", objectPath,
				" needPure: ", needPure,
				" err: ", err)
			return err
		}
	} else {
		err = o.uploadFileStream(
			ctx,
			packageId,
			sourceFile,
			objectPath)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:uploadFileStream failed.",
				" packageId: ", packageId,
				" sourceFile: ", sourceFile,
				" objectPath: ", objectPath,
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFile finish.")
	return err
}

func (o *JCS) uploadFileStream(
	ctx context.Context,
	packageId int32,
	sourceFile,
	objectPath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFileStream start.",
		" packageId: ", packageId,
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

	err = o.jcsClient.UploadFile(
		ctx,
		packageId,
		objectPath,
		fd)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.UploadFile failed.",
			" packageId: ", packageId,
			" objectPath: ", objectPath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFileStream finish.")
	return err
}

func (o *JCS) uploadFileResume(
	ctx context.Context,
	packageId int32,
	sourceFile,
	objectPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFileResume start.",
		" packageId: ", packageId,
		" sourceFile: ", sourceFile,
		" objectPath: ", objectPath,
		" needPure: ", needPure)

	uploadFileInput := new(JCSUploadFileInput)
	uploadFileInput.ObjectPath = objectPath
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + ".upload_file_record"
	uploadFileInput.TaskNum = DefaultJCSUploadMultiTaskNum
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
		packageId,
		sourceFile,
		objectPath,
		uploadFileInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:resumeUpload failed.",
			" packageId: ", packageId,
			" sourceFile: ", sourceFile,
			" objectPath: ", objectPath,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCS:uploadFileResume finish.")
	return err
}

func (o *JCS) resumeUpload(
	ctx context.Context,
	packageId int32,
	sourceFile,
	objectPath string,
	input *JCSUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:resumeUpload start.",
		" packageId: ", packageId,
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
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"JCS:prepareUpload failed.",
				" objectPath: ", objectPath,
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
		ufc,
		input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:uploadPartConcurrent failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	err = o.completeParts(
		ctx,
		ufc,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:completeParts failed.",
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
	ufc *JCSUploadCheckpoint,
	input *JCSUploadFileInput) error {

	Logger.WithContext(ctx).Debug(
		"JCS:uploadPartConcurrent start.",
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
					"JCS:handleUploadTaskResult failed.",
					" PartNumber: ", task.PartNumber,
					" checkpointFile: ", input.CheckpointFile,
					" err: ", _err)
				uploadPartError.Store(_err)
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
	packageId int32,
	objectPath string,
	ufc *JCSUploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *JCSUploadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:prepareUpload start.",
		" packageId: ", packageId,
		" objectPath: ", objectPath)

	newMultipartUploadOutput, err := o.jcsClient.NewMultiPartUpload(
		ctx,
		packageId,
		objectPath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSProxy:NewMultipartUploadWithSignedUrl failed.",
			" packageId: ", packageId,
			" objectPath: ", objectPath,
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
		"JCS:handleUploadTaskResult finish.")
	return
}

func (o *JCS) completeParts(
	ctx context.Context,
	ufc *JCSUploadCheckpoint,
	enableCheckpoint bool,
	checkpointFilePath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:completeParts start.",
		" enableCheckpoint: ", enableCheckpoint,
		" checkpointFilePath: ", checkpointFilePath)

	parts := make([]int32, 0, len(ufc.UploadParts))
	for _, uploadPart := range ufc.UploadParts {
		parts = append(parts, uploadPart.PartNumber)
	}

	err = o.jcsClient.CompleteMultiPartUpload(
		ctx,
		ufc.ObjectId,
		parts)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.CompleteMultiPartUpload failed.",
			" ObjectId: ", ufc.ObjectId,
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

	Logger.WithContext(ctx).Debug(
		"JCSUploadPartTask:Run start.",
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

	readerWrapper.TotalCount = task.PartSize
	readerWrapper.Mark = task.Offset
	if _, err = fd.Seek(task.Offset, io.SeekStart); nil != err {
		Logger.WithContext(ctx).Error(
			"fd.Seek failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	err = task.JcsClient.UploadPart(
		ctx,
		task.ObjectId,
		task.PartNumber,
		task.ObjectPath,
		readerWrapper)

	if nil != err {
		Logger.WithContext(ctx).Error(
			"JcsClient.UploadPart failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"JCSUploadPartTask:Run finish.")
	return err
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
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"JCS:Download start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath,
		" packageId: ", packageId)

	err = os.MkdirAll(targetPath, os.ModePerm)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.MkdirAll failed.",
			" targetPath: ", targetPath,
			" err: ", err)
		return
	}

	downloadFolderRecord :=
		strings.TrimSuffix(targetPath, "/") + ".download_folder_record"
	Logger.WithContext(ctx).Debug(
		"downloadFolderRecord file info.",
		" downloadFolderRecord: ", downloadFolderRecord)

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
			Logger.WithContext(ctx).Error(
				"jcsClient:List failed.",
				" packageId: ", packageId,
				" sourcePath: ", sourcePath,
				" continuationToken: ", continuationToken,
				" err: ", err)
			return err
		}

		err = o.downloadObjects(
			ctx,
			sourcePath,
			targetPath,
			listObjectsData,
			downloadFolderRecord)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS:downloadObjects failed.",
				" sourcePath: ", sourcePath,
				" targetPath: ", targetPath,
				" downloadFolderRecord: ", downloadFolderRecord,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"os.Remove failed.",
				" downloadFolderRecord: ", downloadFolderRecord,
				" err: ", err)
		}
	}

	Logger.WithContext(ctx).Debug(
		"JCS:Download finish.",
		" sourcePath: ", sourcePath)
	return nil
}

func (o *JCS) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	listObjectsData *JCSListData,
	downloadFolderRecord string) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:downloadObjects start.",
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
	pool, err := ants.NewPool(DefaultJCSDownloadFileTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool for download object failed.",
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
			if "0" == itemObject.Size &&
				'/' == itemObject.Path[len(itemObject.Path)-1] {

				itemPath := targetPath + itemObject.Path
				_err := os.MkdirAll(itemPath, os.ModePerm)
				if nil != _err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"os.MkdirAll failed.",
						" itemPath: ", itemPath,
						" err: ", _err)
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
					Logger.WithContext(ctx).Error(
						"JCS:downloadPart failed.",
						" objectPath: ", itemObject.Path,
						" targetFile: ", targetFile,
						" err: ", _err)
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
			"JCS:downloadObjects not all success.",
			" sourcePath: ", sourcePath)
		return errors.New("downloadObjects not all success")
	}

	Logger.WithContext(ctx).Debug(
		"JCS:downloadObjects finish.")
	return nil
}

func (o *JCS) downloadPart(
	ctx context.Context,
	object *JCSObject,
	targetFile string) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCS:downloadPart start.",
		" ObjectPath: ", object.Path,
		" targetFile: ", targetFile)

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
		object,
		downloadFileInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCS:resumeDownload failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"JCS:downloadPart finish.")
	return
}

func (o *JCS) resumeDownload(
	ctx context.Context,
	object *JCSObject,
	input *JCSDownloadFileInput) (err error) {

	Logger.WithContext(ctx).Debug(
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

	downloadFileError := o.downloadFileConcurrent(ctx, input, dfc)
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
	input *JCSDownloadFileInput,
	dfc *JCSDownloadCheckpoint) error {

	Logger.WithContext(ctx).Debug(
		"JCS:downloadFileConcurrent start.")

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
							"JCS:updateCheckpointFile failed.",
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
						"JCS:handleDownloadTaskResult failed.",
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
			"same file exists.",
			" parentDir: ", parentDir)
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
	} else if result != ErrAbort {
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
		"JCS:UpdateDownloadFile finish. readTotal: ", readTotal)
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

	Logger.WithContext(ctx).Debug(
		"JCSDownloadPartTask:Run start.",
		" partNumber: ", task.PartNumber)

	err, downloadPartOutput :=
		task.JcsClient.DownloadPart(
			ctx,
			task.ObjectId,
			task.Offset,
			task.Length)

	if nil == err {
		Logger.WithContext(ctx).Debug(
			"JcsClient.DownloadPart finish.")
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
		" err: ", err)
	return err
}
