package adaptee

import (
	"context"
	"errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/panjf2000/ants/v2"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"golang.org/x/time/rate"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type S3 struct {
	bucket                 string
	obsClient              *obs.ObsClient
	s3UploadFileTaskNum    int
	s3UploadMultiTaskNum   int
	s3DownloadFileTaskNum  int
	s3DownloadMultiTaskNum int
	s3RateLimiter          *rate.Limiter
}

func (o *S3) Init(
	ctx context.Context,
	ak,
	sk,
	endpoint,
	bucket string,
	nodeType int32,
	reqTimeout,
	maxConnection int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:Init start.",
		" ak: ", "***",
		" sk: ", "***",
		" endpoint: ", endpoint,
		" bucket: ", bucket,
		" nodeType: ", nodeType,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

	o.bucket = bucket

	switch nodeType {
	case StorageCategoryEObs:
		o.obsClient, err = obs.New(
			ak,
			sk,
			endpoint,
			obs.WithSignature(obs.SignatureObs),
			obs.WithConnectTimeout(int(reqTimeout)),
			obs.WithMaxConnections(int(maxConnection)))
	default:
		o.obsClient, err = obs.New(
			ak,
			sk,
			endpoint,
			obs.WithConnectTimeout(int(reqTimeout)),
			obs.WithMaxConnections(int(maxConnection)))
	}
	if nil != err {
		Logger.WithContext(ctx).Error(
			"obs.New failed.",
			" err: ", err)
		return err
	}

	o.s3UploadFileTaskNum = DefaultS3UploadFileTaskNum
	o.s3UploadMultiTaskNum = DefaultS3UploadMultiTaskNum
	o.s3DownloadFileTaskNum = DefaultS3DownloadFileTaskNum
	o.s3DownloadMultiTaskNum = DefaultS3DownloadMultiTaskNum

	o.s3RateLimiter = rate.NewLimiter(
		DefaultS3RateLimit,
		DefaultS3RateBurst)

	Logger.WithContext(ctx).Debug(
		"S3:Init finish.")
	return nil
}

func (o *S3) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:SetConcurrency start.",
		" UploadFileTaskNum: ", config.UploadFileTaskNum,
		" UploadMultiTaskNum: ", config.UploadMultiTaskNum,
		" DownloadFileTaskNum: ", config.DownloadFileTaskNum,
		" DownloadMultiTaskNum: ", config.DownloadMultiTaskNum)

	o.s3UploadFileTaskNum = int(config.UploadFileTaskNum)
	o.s3UploadMultiTaskNum = int(config.UploadMultiTaskNum)
	o.s3DownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.s3DownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	Logger.WithContext(ctx).Debug(
		"S3:SetConcurrency finish.")
	return nil
}

func (o *S3) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:SetRate start.")

	o.s3RateLimiter = rateLimiter

	Logger.WithContext(ctx).Debug(
		"S3:SetRate finish.")
	return nil
}

func (o *S3) Mkdir(
	ctx context.Context,
	input interface{}) (err error) {

	var objectKey string
	if s3MkdirInput, ok := input.(S3MkdirInput); ok {
		objectKey = s3MkdirInput.ObjectKey
	} else {
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}
	if 0 < len(objectKey) && '/' != objectKey[len(objectKey)-1] {
		objectKey = objectKey + "/"
	}

	Logger.WithContext(ctx).Debug(
		"S3:Mkdir start.",
		" objectKey: ", objectKey)

	putObjectInput := new(obs.PutObjectInput)
	putObjectInput.Bucket = o.bucket
	putObjectInput.Key = objectKey

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			_, _err := o.obsClient.PutObject(putObjectInput)
			if nil != _err {
				if obsError, ok := _err.(obs.ObsError); ok {
					Logger.WithContext(ctx).Error(
						"obsClient.PutObject failed.",
						" objectKey: ", putObjectInput.Key,
						" obsCode: ", obsError.Code,
						" obsMessage: ", obsError.Message)
					return _err
				} else {
					Logger.WithContext(ctx).Error(
						"obsClient.PutObject failed.",
						" objectKey: ", putObjectInput.Key,
						" err: ", _err)
					return _err
				}
			}
			return nil
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:Mkdir failed.",
			" objectKey: ", putObjectInput.Key,
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"S3:Mkdir finish.")
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

	s3MkdirInput := S3MkdirInput{}
	s3MkdirInput.ObjectKey = targetPath
	err = o.Mkdir(ctx, s3MkdirInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:Mkdir failed.",
			" targetPath: ", targetPath,
			" err: ", err)
		return err
	}

	pool, err := ants.NewPool(o.s3UploadFileTaskNum)
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

			err = o.s3RateLimiter.Wait(ctx)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"RateLimiter.Wait failed.",
					" err: ", err)
				return err
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
				if strings.HasSuffix(objectKey, UploadFileRecordSuffix) {
					Logger.WithContext(ctx).Info(
						"upload record file.",
						" objectKey: ", objectKey)
					return
				}
				if _, exists := fileMap[objectKey]; exists {
					Logger.WithContext(ctx).Info(
						"already finish. objectKey: ", objectKey)
					return
				}
				if fileInfo.IsDir() {
					input := S3MkdirInput{}
					input.ObjectKey = objectKey
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"S3:Mkdir failed.",
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
		input.Bucket = o.bucket
		input.UploadFile = sourceFile
		input.EnableCheckpoint = true
		input.CheckpointFile = input.UploadFile + UploadFileRecordSuffix
		input.TaskNum = o.s3UploadMultiTaskNum
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

		err = RetryV1(
			ctx,
			Attempts,
			Delay*time.Second,
			func() error {
				_, _err := o.obsClient.UploadFile(input)
				if nil != _err {
					if obsError, ok := _err.(obs.ObsError); ok {
						Logger.WithContext(ctx).Error(
							"obsClient.UploadFile failed.",
							" objectKey: ", input.Key,
							" obsCode: ", obsError.Code,
							" obsMessage: ", obsError.Message)
						return _err
					} else {
						Logger.WithContext(ctx).Error(
							"obsClient.UploadFile failed.",
							" objectKey: ", input.Key,
							" err: ", _err)
						return _err
					}
				}
				return nil
			})
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3:uploadFile failed.",
				" objectKey: ", input.Key,
				" err: ", err)
			return err
		}
	} else {
		err = RetryV1(
			ctx,
			Attempts,
			Delay*time.Second,
			func() error {
				stat, err := os.Stat(sourceFile)
				if nil != err {
					Logger.WithContext(ctx).Error(
						"os.Stat failed.",
						" sourceFile: ", sourceFile,
						" err: ", err)
					return err
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

				input := new(obs.PutObjectInput)
				input.Bucket = o.bucket
				input.Key = objectKey
				input.Body = fd
				input.ContentLength = stat.Size()
				if 0 == input.ContentLength {
					input.Body = nil
				}
				_, _err := o.obsClient.PutObject(input)
				if nil != _err {
					if obsError, ok := _err.(obs.ObsError); ok {
						Logger.WithContext(ctx).Error(
							"obsClient.PutObject failed.",
							" objectKey: ", input.Key,
							" obsCode: ", obsError.Code,
							" obsMessage: ", obsError.Message)
						return _err
					} else {
						Logger.WithContext(ctx).Error(
							"obsClient.PutObject failed.",
							" objectKey: ", input.Key,
							" err: ", _err)
						return _err
					}
				}
				return nil
			})
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3:uploadFile failed.",
				" objectKey: ", objectKey,
				" err: ", err)
			return err
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

	marker := ""
	for {
		inputList := new(obs.ListObjectsInput)
		inputList.Bucket = o.bucket
		inputList.Prefix = sourcePath
		if "" != marker {
			inputList.Marker = marker
		}

		err, listObjectsOutputTmp := RetryV4(
			ctx,
			Attempts,
			Delay*time.Second,
			func() (error, interface{}) {
				output := new(obs.ListObjectsOutput)
				output, _err := o.obsClient.ListObjects(inputList)
				if nil != _err {
					if obsError, ok := _err.(obs.ObsError); ok {
						Logger.WithContext(ctx).Error(
							"obsClient.ListObjects failed.",
							" Prefix: ", inputList.Prefix,
							" Marker: ", inputList.Marker,
							" obsCode: ", obsError.Code,
							" obsMessage: ", obsError.Message)
						return _err, output
					} else {
						Logger.WithContext(ctx).Error(
							"obsClient.ListObjects failed.",
							" Prefix: ", inputList.Prefix,
							" Marker: ", inputList.Marker,
							" err: ", _err)
						return _err, output
					}
				}
				return _err, output
			})
		if nil != err {
			Logger.WithContext(ctx).Error(
				"list objects failed.",
				" Prefix: ", inputList.Prefix,
				" Marker: ", inputList.Marker,
				" err: ", err)
			return err
		}

		listObjectsOutput := new(obs.ListObjectsOutput)
		isValid := false
		if listObjectsOutput, isValid =
			listObjectsOutputTmp.(*obs.ListObjectsOutput); !isValid {

			Logger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		err = o.downloadObjects(
			ctx,
			sourcePath,
			targetPath,
			listObjectsOutput,
			downloadFolderRecord)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3:downloadObjects failed.",
				" sourcePath: ", sourcePath,
				" targetPath: ", targetPath,
				" downloadFolderRecord: ", downloadFolderRecord,
				" err: ", err)
			return err
		}
		if listObjectsOutput.IsTruncated {
			marker = listObjectsOutput.NextMarker
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
		"S3:Download finish.")
	return nil
}

func (o *S3) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	listObjectsOutput *obs.ListObjectsOutput,
	downloadFolderRecord string) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:downloadObjects start.",
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
	pool, err := ants.NewPool(o.s3DownloadFileTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool for download Object failed.",
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

		ctxRate, cancel := context.WithCancel(context.Background())
		func() {
			defer cancel()
		}()
		err = o.s3RateLimiter.Wait(ctxRate)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"RateLimiter.Wait failed.",
				" err: ", err)
			return err
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
					input.DownloadFile + DownloadFileRecordSuffix
				input.TaskNum = o.s3DownloadMultiTaskNum
				input.PartSize = DefaultPartSize
				input.Bucket = o.bucket
				input.Key = itemObject.Key

				_err := RetryV1(
					ctx,
					Attempts,
					Delay*time.Second,
					func() error {
						_, __err := o.obsClient.DownloadFile(input)
						if nil != __err {
							if obsError, ok := __err.(obs.ObsError); ok {
								Logger.WithContext(ctx).Error(
									"obsClient.DownloadFile failed.",
									" objectKey: ", input.Key,
									" obsCode: ", obsError.Code,
									" obsMessage: ", obsError.Message)
								return __err
							} else {
								Logger.WithContext(ctx).Error(
									"obsClient.DownloadFile failed.",
									" objectKey: ", input.Key,
									" err: ", __err)
								return __err
							}
						}
						return __err
					})
				if nil != _err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"S3:downloadFile failed.",
						" objectKey: ", input.Key,
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
	}

	Logger.WithContext(ctx).Debug(
		"S3:downloadObjects finish.")
	return nil
}

func (o *S3) Delete(
	ctx context.Context,
	input interface{}) (err error) {

	var path string
	if s3DeleteInput, ok := input.(S3DeleteInput); ok {
		path = s3DeleteInput.Path
	} else {
		Logger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	Logger.WithContext(ctx).Debug(
		"S3:Delete start.",
		" path: ", path)

	marker := ""
	for {
		inputList := new(obs.ListObjectsInput)
		inputList.Bucket = o.bucket
		inputList.Prefix = path
		if "" != marker {
			inputList.Marker = marker
		}

		err, listObjectsOutputTmp := RetryV4(
			ctx,
			Attempts,
			Delay*time.Second,
			func() (error, interface{}) {
				output := new(obs.ListObjectsOutput)
				output, _err := o.obsClient.ListObjects(inputList)
				if nil != _err {
					if obsError, ok := _err.(obs.ObsError); ok {
						Logger.WithContext(ctx).Error(
							"obsClient.ListObjects failed.",
							" Prefix: ", inputList.Prefix,
							" Marker: ", inputList.Marker,
							" obsCode: ", obsError.Code,
							" obsMessage: ", obsError.Message)
						return _err, output
					} else {
						Logger.WithContext(ctx).Error(
							"obsClient.ListObjects failed.",
							" Prefix: ", inputList.Prefix,
							" Marker: ", inputList.Marker,
							" err: ", _err)
						return _err, output
					}
				}
				return _err, output
			})
		if nil != err {
			Logger.WithContext(ctx).Error(
				"list objects failed.",
				" Prefix: ", inputList.Prefix,
				" Marker: ", inputList.Marker,
				" err: ", err)
			return err
		}

		listObjectsOutput := new(obs.ListObjectsOutput)
		isValid := false
		if listObjectsOutput, isValid =
			listObjectsOutputTmp.(*obs.ListObjectsOutput); !isValid {

			Logger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		objects := make([]obs.ObjectToDelete, 0)
		for index, object := range listObjectsOutput.Contents {
			Logger.WithContext(ctx).Debug(
				"object content.",
				" index: ", index,
				" eTag: ", object.ETag,
				" key: ", object.Key,
				" size: ", object.Size)
			objectToDelete := obs.ObjectToDelete{}
			objectToDelete.Key = object.Key
			objects = append(objects, objectToDelete)
		}

		if 0 == len(objects) {
			Logger.WithContext(ctx).Info(
				"has no objects, return")
			return nil
		}

		deleteObjectsInput := new(obs.DeleteObjectsInput)
		deleteObjectsInput.Bucket = o.bucket
		deleteObjectsInput.Objects = objects

		err = RetryV1(
			ctx,
			Attempts,
			Delay*time.Second,
			func() error {
				_, _err := o.obsClient.DeleteObjects(deleteObjectsInput)
				if nil != _err {
					if obsError, ok := _err.(obs.ObsError); ok {
						Logger.WithContext(ctx).Error(
							"obsClient.DeleteObjects failed.",
							" obsCode: ", obsError.Code,
							" obsMessage: ", obsError.Message)
						return _err
					} else {
						Logger.WithContext(ctx).Error(
							"obsClient.DeleteObjects failed.",
							" err: ", _err)
						return _err
					}
				}
				return _err
			})
		if nil != err {
			Logger.WithContext(ctx).Error(
				"delete objects failed.",
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
		"S3:Delete finish.")
	return nil
}
