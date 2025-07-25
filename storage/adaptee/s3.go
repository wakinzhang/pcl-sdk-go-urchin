package adaptee

import (
	"context"
	"errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/panjf2000/ants/v2"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
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

	InfoLogger.WithContext(ctx).Debug(
		"S3:Init start.",
		zap.String("endpoint", endpoint),
		zap.String("bucket", bucket),
		zap.Int32("nodeType", nodeType),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

	o.bucket = bucket

	switch nodeType {
	case StorageCategoryEObs:
		o.obsClient, err = obs.New(
			ak,
			sk,
			endpoint,
			obs.WithSignature(obs.SignatureObs),
			obs.WithSocketTimeout(int(reqTimeout)),
			obs.WithMaxConnections(int(maxConnection)),
			obs.WithMaxRetryCount(DefaultSeMaxRetryCount))
	default:
		o.obsClient, err = obs.New(
			ak,
			sk,
			endpoint,
			obs.WithSocketTimeout(int(reqTimeout)),
			obs.WithMaxConnections(int(maxConnection)),
			obs.WithMaxRetryCount(DefaultSeMaxRetryCount))
	}
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"obs.New failed.",
			zap.Error(err))
		return err
	}

	o.s3UploadFileTaskNum = DefaultS3UploadFileTaskNum
	o.s3UploadMultiTaskNum = DefaultS3UploadMultiTaskNum
	o.s3DownloadFileTaskNum = DefaultS3DownloadFileTaskNum
	o.s3DownloadMultiTaskNum = DefaultS3DownloadMultiTaskNum

	o.s3RateLimiter = rate.NewLimiter(
		DefaultS3RateLimit,
		DefaultS3RateBurst)

	InfoLogger.WithContext(ctx).Debug(
		"S3:Init finish.")
	return nil
}

func (o *S3) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("UploadMultiTaskNum", config.UploadMultiTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	o.s3UploadFileTaskNum = int(config.UploadFileTaskNum)
	o.s3UploadMultiTaskNum = int(config.UploadMultiTaskNum)
	o.s3DownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.s3DownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	InfoLogger.WithContext(ctx).Debug(
		"S3:SetConcurrency finish.")
	return nil
}

func (o *S3) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3:SetRate start.")

	o.s3RateLimiter = rateLimiter

	InfoLogger.WithContext(ctx).Debug(
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
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}
	if 0 < len(objectKey) && '/' != objectKey[len(objectKey)-1] {
		objectKey = objectKey + "/"
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3:Mkdir start.",
		zap.String("objectKey", objectKey))

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
				var obsError obs.ObsError
				if errors.As(_err, &obsError) {
					ErrorLogger.WithContext(ctx).Error(
						"obsClient.PutObject failed.",
						zap.String("objectKey", putObjectInput.Key),
						zap.String("obsCode", obsError.Code),
						zap.String("obsMessage", obsError.Message))
					return _err
				}
			}
			return nil
		})
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3:Mkdir failed.",
			zap.String("objectKey", putObjectInput.Key),
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
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
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3:Upload start.",
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
		err = o.uploadFolder(
			ctx,
			sourcePath,
			targetPath,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3.uploadFolder failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("targetPath", targetPath),
				zap.Error(err))
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
			ErrorLogger.WithContext(ctx).Error(
				"S3.uploadFile failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("objectKey", objectKey),
				zap.Bool("needPure", needPure),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3:Upload finish.")
	return nil
}

func (o *S3) uploadFolder(
	ctx context.Context,
	sourcePath,
	targetPath string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3:uploadFolder start.",
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

	s3MkdirInput := S3MkdirInput{}
	s3MkdirInput.ObjectKey = targetPath
	err = o.Mkdir(ctx, s3MkdirInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3:Mkdir failed.",
			zap.String("targetPath", targetPath),
			zap.Error(err))
		return err
	}

	pool, err := ants.NewPool(o.s3UploadFileTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool failed.",
			zap.Error(err))
		return err
	}
	defer pool.Release()

	var isAllSuccess = true
	var wg sync.WaitGroup
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

			wg.Add(1)
			err = pool.Submit(func() {
				defer func() {
					wg.Done()
					if _err := recover(); nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"S3:uploadFileResume failed.",
							zap.Any("error", _err))
						isAllSuccess = false
					}
				}()
				relPath, _err := filepath.Rel(sourcePath, filePath)
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
				objectKey := targetPath + relPath
				if strings.HasSuffix(objectKey, UploadFileRecordSuffix) {
					InfoLogger.WithContext(ctx).Info(
						"upload record file.",
						zap.String("objectKey", objectKey))
					return
				}
				if _, exists := fileMap[objectKey]; exists {
					InfoLogger.WithContext(ctx).Info(
						"already finish.",
						zap.String("objectKey", objectKey))
					return
				}

				InfoLogger.WithContext(ctx).Debug(
					"RateLimiter.Wait start.")
				ctxRate, cancel := context.WithCancel(context.Background())
				_err = o.s3RateLimiter.Wait(ctxRate)
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

				if fileInfo.IsDir() {
					input := S3MkdirInput{}
					input.ObjectKey = objectKey
					_err = o.Mkdir(ctx, input)
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"S3:Mkdir failed.",
							zap.String("objectKey", objectKey),
							zap.Error(_err))
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
						ErrorLogger.WithContext(ctx).Error(
							"S3:uploadFile failed.",
							zap.String("filePath", filePath),
							zap.String("objectKey", objectKey),
							zap.Bool("needPure", needPure),
							zap.Error(_err))
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
				_, _err = f.Write([]byte(objectKey + "\n"))
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"write file failed.",
						zap.String("uploadFolderRecord",
							uploadFolderRecord),
						zap.String("objectKey", objectKey),
						zap.Error(_err))
					return
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
	wg.Wait()

	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"filepath.Walk failed.",
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}
	if !isAllSuccess {
		ErrorLogger.WithContext(ctx).Error(
			"S3:uploadFolder not all success.",
			zap.String("sourcePath", sourcePath))

		return errors.New("uploadFolder not all success")
	} else {
		_err := os.Remove(uploadFolderRecord)
		if nil != _err {
			if !os.IsNotExist(_err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Remove failed.",
					zap.String("uploadFolderRecord", uploadFolderRecord),
					zap.Error(_err))
			}
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3:uploadFolder finish.")
	return nil
}

func (o *S3) uploadFile(
	ctx context.Context,
	sourceFile,
	objectKey string,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3:uploadFile start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectKey", objectKey),
		zap.Bool("needPure", needPure))

	sourceFileStat, err := os.Stat(sourceFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
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
					ErrorLogger.WithContext(ctx).Error(
						"os.Remove failed.",
						zap.String("checkpointFile", input.CheckpointFile),
						zap.Error(err))
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
					var obsError obs.ObsError
					if errors.As(_err, &obsError) {
						ErrorLogger.WithContext(ctx).Error(
							"obsClient.UploadFile failed.",
							zap.String("objectKey", input.Key),
							zap.String("obsCode", obsError.Code),
							zap.String("obsMessage", obsError.Message))
						return _err
					}
				}
				return nil
			})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3:uploadFile failed.",
				zap.String("objectKey", input.Key),
				zap.Error(err))
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
					ErrorLogger.WithContext(ctx).Error(
						"os.Stat failed.",
						zap.String("sourceFile", sourceFile),
						zap.Error(err))
					return err
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
					var obsError obs.ObsError
					if errors.As(_err, &obsError) {
						ErrorLogger.WithContext(ctx).Error(
							"obsClient.PutObject failed.",
							zap.String("objectKey", input.Key),
							zap.String("obsCode", obsError.Code),
							zap.String("obsMessage", obsError.Message))
						return _err
					}
				}
				return nil
			})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3:uploadFile failed.",
				zap.String("objectKey", objectKey),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
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
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3:Download start.",
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
					var obsError obs.ObsError
					if errors.As(_err, &obsError) {
						ErrorLogger.WithContext(ctx).Error(
							"obsClient.ListObjects failed.",
							zap.String("prefix", inputList.Prefix),
							zap.String("marker", inputList.Marker),
							zap.String("obsCode", obsError.Code),
							zap.String("obsMessage", obsError.Message))
						return _err, output
					}
				}
				return _err, output
			})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"list objects failed.",
				zap.String("prefix", inputList.Prefix),
				zap.String("marker", inputList.Marker),
				zap.Error(err))
			return err
		}

		listObjectsOutput := new(obs.ListObjectsOutput)
		isValid := false
		if listObjectsOutput, isValid =
			listObjectsOutputTmp.(*obs.ListObjectsOutput); !isValid {

			ErrorLogger.WithContext(ctx).Error(
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
			ErrorLogger.WithContext(ctx).Error(
				"S3:downloadObjects failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("targetPath", targetPath),
				zap.String("downloadFolderRecord", downloadFolderRecord),
				zap.Error(err))
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
			ErrorLogger.WithContext(ctx).Error(
				"os.Remove failed.",
				zap.String("downloadFolderRecord", downloadFolderRecord),
				zap.Error(err))
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3:Download finish.")
	return nil
}

func (o *S3) downloadObjects(
	ctx context.Context,
	sourcePath,
	targetPath string,
	listObjectsOutput *obs.ListObjectsOutput,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3:downloadObjects start.",
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
	pool, err := ants.NewPool(o.s3DownloadFileTaskNum)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ants.NewPool for download Object failed.",
			zap.Error(err))
		return err
	}
	defer pool.Release()
	for index, object := range listObjectsOutput.Contents {
		InfoLogger.WithContext(ctx).Debug(
			"object content.",
			zap.Int("index", index),
			zap.String("eTag", object.ETag),
			zap.String("key", object.Key),
			zap.Int64("size", object.Size))
		itemObject := object
		if _, exists := fileMap[itemObject.Key]; exists {
			InfoLogger.WithContext(ctx).Info(
				"file already success.",
				zap.String("objectKey", itemObject.Key))
			continue
		}

		InfoLogger.WithContext(ctx).Debug(
			"RateLimiter.Wait start.")
		ctxRate, cancel := context.WithCancel(context.Background())
		err = o.s3RateLimiter.Wait(ctxRate)
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

			if 0 == itemObject.Size &&
				'/' == itemObject.Key[len(itemObject.Key)-1] {

				itemPath := targetPath + itemObject.Key
				_err := os.MkdirAll(itemPath, os.ModePerm)
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"os.MkdirAll failed.",
						zap.String("itemPath", itemPath),
						zap.Error(_err))
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
							var obsError obs.ObsError
							if errors.As(__err, &obsError) {
								ErrorLogger.WithContext(ctx).Error(
									"obsClient.DownloadFile failed.",
									zap.String("objectKey", input.Key),
									zap.String("obsCode", obsError.Code),
									zap.String("obsMessage",
										obsError.Message))
								return __err
							}
						}
						return __err
					})
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"S3:downloadFile failed.",
						zap.String("objectKey", input.Key),
						zap.Error(_err))
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
			_, _err = f.Write([]byte(itemObject.Key + "\n"))
			if nil != _err {
				isAllSuccess = false
				ErrorLogger.WithContext(ctx).Error(
					"write file failed.",
					zap.String("downloadFolderRecord",
						downloadFolderRecord),
					zap.String("objectKey", itemObject.Key),
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
			"S3:downloadObjects not all success.",
			zap.String("sourcePath", sourcePath))

		return errors.New("downloadObjects not all success")
	}

	InfoLogger.WithContext(ctx).Debug(
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
		ErrorLogger.WithContext(ctx).Error(
			"input param invalid.")
		return errors.New("input param invalid")
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3:Delete start.",
		zap.String("path", path))

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
					var obsError obs.ObsError
					if errors.As(_err, &obsError) {
						ErrorLogger.WithContext(ctx).Error(
							"obsClient.ListObjects failed.",
							zap.String("prefix", inputList.Prefix),
							zap.String("marker", inputList.Marker),
							zap.String("obsCode", obsError.Code),
							zap.String("obsMessage", obsError.Message))
						return _err, output
					}
				}
				return _err, output
			})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"list objects failed.",
				zap.String("prefix", inputList.Prefix),
				zap.String("marker", inputList.Marker),
				zap.Error(err))
			return err
		}

		listObjectsOutput := new(obs.ListObjectsOutput)
		isValid := false
		if listObjectsOutput, isValid =
			listObjectsOutputTmp.(*obs.ListObjectsOutput); !isValid {

			ErrorLogger.WithContext(ctx).Error(
				"response invalid.")
			return errors.New("response invalid")
		}

		objects := make([]obs.ObjectToDelete, 0)
		for index, object := range listObjectsOutput.Contents {
			InfoLogger.WithContext(ctx).Debug(
				"object content.",
				zap.Int("index", index),
				zap.String("eTag", object.ETag),
				zap.String("key", object.Key),
				zap.Int64("size", object.Size))
			objectToDelete := obs.ObjectToDelete{}
			objectToDelete.Key = object.Key
			objects = append(objects, objectToDelete)
		}

		if 0 == len(objects) {
			InfoLogger.WithContext(ctx).Info(
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
					var obsError obs.ObsError
					if errors.As(_err, &obsError) {
						ErrorLogger.WithContext(ctx).Error(
							"obsClient.DeleteObjects failed.",
							zap.String("obsCode", obsError.Code),
							zap.String("obsMessage", obsError.Message))
						return _err
					}
				}
				return _err
			})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"delete objects failed.",
				zap.Error(err))
			return err
		}

		if listObjectsOutput.IsTruncated {
			marker = listObjectsOutput.NextMarker
		} else {
			break
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3:Delete finish.")
	return nil
}
