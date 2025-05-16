package adaptee

import (
	"context"
	"errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/panjf2000/ants/v2"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type S3 struct {
	bucket    string
	obsClient *obs.ObsClient
}

func (o *S3) Init(
	ctx context.Context,
	ak,
	sk,
	endpoint,
	bucket string) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:Init start.",
		" ak: ", "***",
		" sk: ", "***",
		" endpoint: ", endpoint,
		" bucket: ", bucket)

	o.bucket = bucket

	o.obsClient, err = obs.New(ak, sk, endpoint)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"obs.New failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"S3:Init finish.")
	return nil
}

func (o *S3) Upload(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	var needPure bool
	if s3UploadInput, ok := input.(S3UploadInput); ok {
		sourcePath = s3UploadInput.SourcePath
		targetPath = s3UploadInput.TargetPath
		needPure = s3UploadInput.NeedPure
	} else {
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"S3:Upload start.",
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
		err = o.uploadFolder(
			ctx,
			sourcePath,
			targetPath,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3.uploadFolder failed.",
				" sourcePath: ", sourcePath,
				" targetPath: ", targetPath,
				" err: ", err)
			return err
		}
	} else {
		objectKey := targetPath + filepath.Base(sourcePath)
		err = o.uploadFile(
			ctx,
			sourcePath,
			objectKey,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3.uploadFile failed.",
				" sourcePath: ", sourcePath,
				" objectKey: ", objectKey,
				" needPure: ", needPure,
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"S3:Upload finish.")
	return nil
}

func (o *S3) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:uploadFolder start.",
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
	err = o.mkdir(ctx, path)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:mkdir failed.",
			" path: ", path,
			" err: ", err)
		return err
	}

	pool, err := ants.NewPool(DefaultS3UploadFileTaskNum)
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
							"S3:uploadFileResume failed.",
							" err: ", _err)
						isAllSuccess = false
					}
				}()
				relPath, _err := filepath.Rel(sourcePath, filePath)
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
				objectKey := targetPath + relPath
				if _, exists := fileMap[objectKey]; exists {
					Logger.WithContext(ctx).Info(
						"already finish.",
						" objectKey: ", objectKey)
					return
				}
				if fileInfo.IsDir() {
					_err = o.mkdir(ctx, objectKey)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"S3:mkdir failed.",
							" objectKey: ", objectKey,
							" err: ", _err)
						return
					}
				} else {
					_err = o.uploadFile(
						ctx,
						filePath,
						objectKey,
						needPure)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"S3:uploadFile failed.",
							" filePath: ", filePath,
							" objectKey: ", objectKey,
							" needPure: ", needPure,
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
				_, _err = f.Write([]byte(objectKey + "\n"))
				if nil != _err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"write file failed.",
						" uploadFolderRecord: ", uploadFolderRecord,
						" objectKey: ", objectKey,
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
			" sourcePath: ", sourcePath, " err: ", err)
		return err
	}
	if !isAllSuccess {
		Logger.WithContext(ctx).Error(
			"S3:uploadFolder not all success.",
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
		"S3:uploadFolder finish.")
	return nil
}

func (o *S3) mkdir(
	ctx context.Context,
	objectKey string) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:mkdir start.",
		" objectKey: ", objectKey)

	input := new(obs.PutObjectInput)
	input.Bucket = o.bucket
	input.Key = objectKey
	_, err = o.obsClient.PutObject(input)
	if nil != err {
		if obsError, ok := err.(obs.ObsError); ok {
			Logger.WithContext(ctx).Error(
				"obsClient.PutObject failed.",
				" obsCode: ", obsError.Code,
				" obsMessage: ", obsError.Message)
			return err
		} else {
			Logger.WithContext(ctx).Error(
				"obsClient.PutObject failed.",
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"S3:mkdir finish.")
	return nil
}

func (o *S3) uploadFile(
	ctx context.Context,
	sourceFile,
	objectKey string,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:uploadFile start.",
		" sourceFile: ", sourceFile,
		" objectKey: ", objectKey,
		" needPure: ", needPure)

	sourceFileStat, err := os.Stat(sourceFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	if DefaultS3UploadMultiSize < sourceFileStat.Size() {
		input := new(obs.UploadFileInput)
		input.UploadFile = sourceFile
		input.EnableCheckpoint = true
		input.CheckpointFile = input.UploadFile + ".upload_file_record"
		input.TaskNum = DefaultS3UploadMultiTaskNum
		input.PartSize = DefaultPartSize
		input.Key = objectKey
		if input.PartSize < obs.MIN_PART_SIZE {
			input.PartSize = obs.MIN_PART_SIZE
		} else if input.PartSize > obs.MAX_PART_SIZE {
			input.PartSize = obs.MAX_PART_SIZE
		}

		if needPure {
			err = os.Remove(input.CheckpointFile)
			if nil != err {
				if !os.IsNotExist(err) {
					Logger.WithContext(ctx).Error(
						"os.Remove failed.",
						" CheckpointFile: ", input.CheckpointFile,
						" err: ", err)
					return err
				}
			}
		}

		_, err = o.obsClient.UploadFile(input)
		if nil != err {
			if obsError, ok := err.(obs.ObsError); ok {
				Logger.WithContext(ctx).Error(
					"obsClient.UploadFile failed.",
					" obsCode: ", obsError.Code,
					" obsMessage: ", obsError.Message)
				return err
			} else {
				Logger.WithContext(ctx).Error(
					"obsClient.UploadFile failed.",
					" err: ", err)
				return err
			}
		}
	} else {
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

		input := new(obs.PutObjectInput)
		input.Bucket = o.bucket
		input.Key = objectKey
		input.Body = fd
		_, err = o.obsClient.PutObject(input)
		if nil != err {
			if obsError, ok := err.(obs.ObsError); ok {
				Logger.WithContext(ctx).Error(
					"obsClient.PutObject failed.",
					" obsCode: ", obsError.Code,
					" obsMessage: ", obsError.Message)
				return err
			} else {
				Logger.WithContext(ctx).Error(
					"obsClient.PutObject failed.",
					" err: ", err)
				return err
			}
		}
	}

	Logger.WithContext(ctx).Debug(
		"S3:uploadFile finish.")
	return err
}

func (o *S3) Download(
	ctx context.Context,
	input interface{}) (err error) {

	var sourcePath, targetPath string
	if s3DownloadInput, ok := input.(S3DownloadInput); ok {
		sourcePath = s3DownloadInput.SourcePath
		targetPath = s3DownloadInput.TargetPath
	} else {
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"S3:Download start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath)

	marker := ""
	for {
		inputList := new(obs.ListObjectsInput)
		inputList.Bucket = o.bucket
		inputList.Prefix = sourcePath
		if "" != marker {
			inputList.Marker = marker
		}
		listObjectsOutput, err := o.obsClient.ListObjects(inputList)
		if nil != err {
			if obsError, ok := err.(obs.ObsError); ok {
				Logger.WithContext(ctx).Error(
					"obsClient.ListObjects failed.",
					" obsCode: ", obsError.Code,
					" obsMessage: ", obsError.Message)
				return err
			} else {
				Logger.WithContext(ctx).Error(
					"obsClient.ListObjects failed.",
					" err: ", err)
				return err
			}
		}
		err = o.downloadObjects(
			ctx,
			sourcePath,
			targetPath,
			listObjectsOutput)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3:downloadObjects failed.",
				" sourcePath: ", sourcePath,
				" targetPath: ", targetPath,
				" err: ", err)
			return err
		}
		if listObjectsOutput.IsTruncated {
			marker = listObjectsOutput.NextMarker
		} else {
			break
		}
	}
	Logger.WithContext(ctx).Debug(
		"S3:Download finish.")
	return nil
}

func (o *S3) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	listObjectsOutput *obs.ListObjectsOutput) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:downloadObjects start.",
		" sourcePath: ", sourcePath,
		" targetPath: ", targetPath)

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	path := strings.TrimSuffix(
		strings.TrimSuffix(targetPath, "/")+sourcePath, "/")
	downloadFolderRecord := path + ".download_folder_record"
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
	pool, err := ants.NewPool(DefaultS3DownloadFileTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool for download Object  failed.",
			" err: ", err)
		return err
	}
	defer pool.Release()
	for index, object := range listObjectsOutput.Contents {
		Logger.WithContext(ctx).Debug(
			"object content.",
			" index: ", index,
			" eTag: ", object.ETag,
			" key: ", object.Key,
			" size: ", object.Size)
		itemObject := object
		if _, exists := fileMap[itemObject.Key]; exists {
			Logger.WithContext(ctx).Info(
				"file already success.",
				" objectKey: ", itemObject.Key)
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

			if 0 == itemObject.Size &&
				'/' == itemObject.Key[len(itemObject.Key)-1] {

				itemPath := targetPath + itemObject.Key
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
				input := new(obs.DownloadFileInput)
				input.DownloadFile = targetPath + itemObject.Key
				input.EnableCheckpoint = true
				input.CheckpointFile =
					input.DownloadFile + ".download_file_record"
				input.TaskNum = DefaultS3DownloadMultiTaskNum
				input.PartSize = DefaultPartSize
				input.Bucket = o.bucket
				input.Key = itemObject.Key
				_, _err := o.obsClient.DownloadFile(input)
				if nil != _err {
					isAllSuccess = false
					if obsError, ok := _err.(obs.ObsError); ok {
						Logger.WithContext(ctx).Error(
							"obsClient.DownloadFile failed.",
							" obsCode: ", obsError.Code,
							" obsMessage: ", obsError.Message)
						return
					} else {
						Logger.WithContext(ctx).Error(
							"obsClient.DownloadFile failed.",
							" err: ", _err)
						return
					}
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
			_, _err = f.Write([]byte(itemObject.Key + "\n"))
			if nil != _err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"write file failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" objectKey: ", itemObject.Key,
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
			"S3:downloadObjects not all success.",
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
		"S3:downloadObjects finish.")
	return nil
}
