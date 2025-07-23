package adaptee

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/panjf2000/ants/v2"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type S3Proxy struct {
	obsClient              *obs.ObsClient
	s3UploadFileTaskNum    int
	s3UploadMultiTaskNum   int
	s3DownloadFileTaskNum  int
	s3DownloadMultiTaskNum int
	s3RateLimiter          *rate.Limiter
}

func (o *S3Proxy) Init(
	ctx context.Context,
	nodeType int32,
	reqTimeout,
	maxConnection int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:Init start.",
		zap.Int32("nodeType", nodeType),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

	switch nodeType {
	case StorageCategoryEObs:
		o.obsClient, err = obs.New(
			"",
			"",
			"magicalParam",
			obs.WithSignature(obs.SignatureObs),
			obs.WithSocketTimeout(int(reqTimeout)),
			obs.WithMaxConnections(int(maxConnection)),
			obs.WithMaxRetryCount(DefaultSeMaxRetryCount))
	default:
		o.obsClient, err = obs.New(
			"",
			"",
			"magicalParam",
			obs.WithSignature(obs.SignatureV4),
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
		"S3Proxy:Init finish.")
	return nil
}

func (o *S3Proxy) SetConcurrency(
	ctx context.Context,
	config *StorageNodeConcurrencyConfig) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:SetConcurrency start.",
		zap.Int32("UploadFileTaskNum", config.UploadFileTaskNum),
		zap.Int32("UploadMultiTaskNum", config.UploadMultiTaskNum),
		zap.Int32("DownloadFileTaskNum", config.DownloadFileTaskNum),
		zap.Int32("DownloadMultiTaskNum", config.DownloadMultiTaskNum))

	o.s3UploadFileTaskNum = int(config.UploadFileTaskNum)
	o.s3UploadMultiTaskNum = int(config.UploadMultiTaskNum)
	o.s3DownloadFileTaskNum = int(config.DownloadFileTaskNum)
	o.s3DownloadMultiTaskNum = int(config.DownloadMultiTaskNum)

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:SetConcurrency finish.")
	return nil
}

func (o *S3Proxy) SetRate(
	ctx context.Context,
	rateLimiter *rate.Limiter) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:SetRate start.")

	o.s3RateLimiter = rateLimiter

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:SetRate finish.")
	return nil
}

func (o *S3Proxy) NewFolderWithSignedUrl(
	ctx context.Context,
	objectKey string,
	taskId int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:NewFolderWithSignedUrl start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

	if 0 < len(objectKey) && '/' != objectKey[len(objectKey)-1] {
		objectKey = objectKey + "/"
	}
	createEmptyFileSignedUrlReq := new(CreatePutObjectSignedUrlReq)
	createEmptyFileSignedUrlReq.TaskId = taskId
	createEmptyFileSignedUrlReq.Source = objectKey

	err, createEmptyFileSignedUrlResp :=
		UClient.CreatePutObjectSignedUrl(
			ctx,
			createEmptyFileSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreatePutObjectSignedUrl failed.",
			zap.String("objectKey", objectKey),
			zap.Error(err))
		return err
	}
	var emptyFileWithSignedUrlHeader = http.Header{}
	for key, item := range createEmptyFileSignedUrlResp.Header {
		for _, value := range item.Values {
			emptyFileWithSignedUrlHeader.Set(key, value)
		}
	}
	// 创建空文件
	_, err = o.obsClient.PutObjectWithSignedUrl(
		createEmptyFileSignedUrlResp.SignedUrl,
		emptyFileWithSignedUrlHeader,
		nil)
	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) {
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.PutObjectWithSignedUrl failed.",
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:NewFolderWithSignedUrl finish.")
	return err
}

func (o *S3Proxy) PutObjectWithSignedUrl(
	ctx context.Context,
	sourceFile,
	objectKey string,
	taskId int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:PutObjectWithSignedUrl start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

	createPutObjectSignedUrlReq := new(CreatePutObjectSignedUrlReq)
	createPutObjectSignedUrlReq.TaskId = taskId
	createPutObjectSignedUrlReq.Source = objectKey

	err, createPutObjectSignedUrlResp :=
		UClient.CreatePutObjectSignedUrl(
			ctx,
			createPutObjectSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreatePutObjectSignedUrl failed.",
			zap.String("objectKey", objectKey),
			zap.Error(err))
		return err
	}

	var putObjectWithSignedUrlHeader = http.Header{}
	for key, item := range createPutObjectSignedUrlResp.Header {
		for _, value := range item.Values {
			putObjectWithSignedUrlHeader.Set(key, value)
		}
	}

	stat, err := os.Stat(sourceFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return err
	}
	putObjectWithSignedUrlHeader.Set(
		HttpHeaderContentLength,
		strconv.FormatInt(stat.Size(), 10))

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

	if 0 == stat.Size() {
		_, err = o.obsClient.PutObjectWithSignedUrl(
			createPutObjectSignedUrlResp.SignedUrl,
			putObjectWithSignedUrlHeader,
			nil)
	} else {
		_, err = o.obsClient.PutObjectWithSignedUrl(
			createPutObjectSignedUrlResp.SignedUrl,
			putObjectWithSignedUrlHeader,
			fd)
	}
	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) {
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.PutObjectWithSignedUrl failed.",
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
			return err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:PutObjectWithSignedUrl finish.")
	return err
}

func (o *S3Proxy) InitiateMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectKey string,
	taskId int32) (output *obs.InitiateMultipartUploadOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:InitiateMultipartUploadWithSignedUrl start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

	createInitiateMultipartUploadSignedUrlReq :=
		new(CreateInitiateMultipartUploadSignedUrlReq)
	createInitiateMultipartUploadSignedUrlReq.TaskId = taskId
	createInitiateMultipartUploadSignedUrlReq.Source = objectKey

	err, createInitiateMultipartUploadSignedUrlResp :=
		UClient.CreateInitiateMultipartUploadSignedUrl(
			ctx,
			createInitiateMultipartUploadSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateInitiateMultipartUploadSignedUrl"+
				" failed.",
			zap.Error(err))
		return output, err
	}
	var initiateMultipartUploadWithSignedUrlHeader = http.Header{}
	for key, item := range createInitiateMultipartUploadSignedUrlResp.Header {
		for _, value := range item.Values {
			initiateMultipartUploadWithSignedUrlHeader.Set(key, value)
		}
	}

	// 初始化分段上传任务
	output, err = o.obsClient.InitiateMultipartUploadWithSignedUrl(
		createInitiateMultipartUploadSignedUrlResp.SignedUrl,
		initiateMultipartUploadWithSignedUrlHeader)
	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) {
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.InitiateMultipartUploadWithSignedUrl failed.",
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
			return output, err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:InitiateMultipartUploadWithSignedUrl finish.")

	return output, err
}

func (o *S3Proxy) UploadPartWithSignedUrl(
	ctx context.Context,
	sourceFile, uploadId string,
	taskId int32,
	objectKey string,
	partNumber int32,
	offset,
	partSize int64) (output *obs.UploadPartOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:UploadPartWithSignedUrl start.",
		zap.String("sourceFile", sourceFile),
		zap.String("uploadId", uploadId),
		zap.Int32("taskId", taskId),
		zap.String("objectKey", objectKey),
		zap.Int32("partNumber", partNumber),
		zap.Int64("offset", offset),
		zap.Int64("partSize", partSize))

	fd, err := os.Open(sourceFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Open failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return output, err
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

	readerWrapper := new(ReaderWrapper)
	readerWrapper.Reader = fd

	readerWrapper.TotalCount = partSize
	readerWrapper.Mark = offset
	if _, err = fd.Seek(offset, io.SeekStart); nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"fd.Seek failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return output, err
	}

	createUploadPartSignedUrlReq := new(CreateUploadPartSignedUrlReq)
	createUploadPartSignedUrlReq.UploadId = uploadId
	createUploadPartSignedUrlReq.PartNumber = partNumber
	createUploadPartSignedUrlReq.TaskId = taskId
	createUploadPartSignedUrlReq.Source = objectKey
	err, createUploadPartSignedUrlResp :=
		UClient.CreateUploadPartSignedUrl(
			ctx,
			createUploadPartSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateUploadPartSignedUrl failed.",
			zap.String("uploadId", uploadId),
			zap.Int32("partNumber", partNumber),
			zap.Int32("taskId", taskId),
			zap.String("source", objectKey),
			zap.Error(err))
		return output, err
	}
	var uploadPartWithSignedUrlHeader = http.Header{}
	for key, item := range createUploadPartSignedUrlResp.Header {
		for _, value := range item.Values {
			uploadPartWithSignedUrlHeader.Set(key, value)
		}
	}

	uploadPartWithSignedUrlHeader.Set("Content-Length",
		strconv.FormatInt(partSize, 10))

	output, err = o.obsClient.UploadPartWithSignedUrl(
		createUploadPartSignedUrlResp.SignedUrl,
		uploadPartWithSignedUrlHeader,
		readerWrapper)

	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) &&
			obsError.StatusCode >= 400 && obsError.StatusCode < 500 {

			ErrorLogger.WithContext(ctx).Error(
				"obsClient.UploadPartWithSignedUrl failed.",
				zap.String("uploadId", uploadId),
				zap.Int32("partNumber", partNumber),
				zap.Int32("taskId", taskId),
				zap.String("source", objectKey),
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
		}
		return output, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:UploadPartWithSignedUrl finish.")
	return output, err
}

func (o *S3Proxy) ListPartsWithSignedUrl(
	ctx context.Context,
	uploadId string,
	taskId int32,
	objectKey string,
	partNumberMarker,
	maxParts int32) (output *obs.ListPartsOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:ListPartsWithSignedUrl start.",
		zap.String("uploadId", uploadId),
		zap.Int32("taskId", taskId),
		zap.String("objectKey", objectKey),
		zap.Int32("partNumberMarker", partNumberMarker),
		zap.Int32("maxParts", maxParts))

	createListPartsSignedUrlReq := new(CreateListPartsSignedUrlReq)
	createListPartsSignedUrlReq.UploadId = uploadId
	createListPartsSignedUrlReq.TaskId = taskId
	createListPartsSignedUrlReq.Source = objectKey
	createListPartsSignedUrlReq.PartNumberMarker = &partNumberMarker
	createListPartsSignedUrlReq.MaxParts = &maxParts

	err, createListPartsSignedUrlResp :=
		UClient.CreateListPartsSignedUrl(
			ctx,
			createListPartsSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateListPartsSignedUrl failed.",
			zap.String("uploadId", uploadId),
			zap.Int32("taskId", taskId),
			zap.String("source", objectKey),
			zap.Int32("partNumberMarker", partNumberMarker),
			zap.Int32("maxParts", maxParts),
			zap.Error(err))
		return output, err
	}
	var listPartsWithSignedUrlHeader = http.Header{}
	for key, item := range createListPartsSignedUrlResp.Header {
		for _, value := range item.Values {
			listPartsWithSignedUrlHeader.Set(key, value)
		}
	}

	output, err = o.obsClient.ListPartsWithSignedUrl(
		createListPartsSignedUrlResp.SignedUrl,
		listPartsWithSignedUrlHeader)

	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) &&
			obsError.StatusCode >= 400 && obsError.StatusCode < 500 {

			ErrorLogger.WithContext(ctx).Error(
				"obsClient.ListPartsWithSignedUrl failed.",
				zap.String("uploadId", uploadId),
				zap.Int32("taskId", taskId),
				zap.String("source", objectKey),
				zap.Int32("partNumberMarker", partNumberMarker),
				zap.Int32("maxParts", maxParts),
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
		}
		return output, err
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:ListPartsWithSignedUrl finish.")
	return output, err
}

func (o *S3Proxy) CompleteMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectKey,
	uploadId string,
	taskId int32,
	partSlice PartSlice) (
	output *obs.CompleteMultipartUploadOutput,
	err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:CompleteMultipartUploadWithSignedUrl start.",
		zap.String("objectKey", objectKey),
		zap.String("uploadId", uploadId),
		zap.Int32("taskId", taskId))

	// 合并段
	createCompleteMultipartUploadSignedUrlReq :=
		new(CreateCompleteMultipartUploadSignedUrlReq)
	createCompleteMultipartUploadSignedUrlReq.UploadId = uploadId
	createCompleteMultipartUploadSignedUrlReq.TaskId = taskId
	createCompleteMultipartUploadSignedUrlReq.Source = objectKey

	err, createCompleteMultipartUploadSignedUrlResp :=
		UClient.CreateCompleteMultipartUploadSignedUrl(
			ctx,
			createCompleteMultipartUploadSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient:CreateCompleteMultipartUploadSignedUrl"+
				" failed.",
			zap.Error(err))
		return output, err
	}

	var completeMultipartUploadPart = new(CompleteMultipartUploadPart)
	completeMultipartUploadPart.PartSlice = make([]XPart, 0)
	completeMultipartUploadPart.PartSlice = partSlice
	completeMultipartUploadPartXML, err := xml.MarshalIndent(
		completeMultipartUploadPart,
		"",
		"  ")
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"xml.MarshalIndent failed.",
			zap.Error(err))
		return output, err
	}
	InfoLogger.WithContext(ctx).Debug(
		"completeMultipartUploadPartXML info.",
		zap.String("content", string(completeMultipartUploadPartXML)))
	var completeMultipartUploadWithSignedUrlHeader = http.Header{}
	for key, item := range createCompleteMultipartUploadSignedUrlResp.Header {
		for _, value := range item.Values {
			completeMultipartUploadWithSignedUrlHeader.Set(key, value)
		}
	}
	output, err = o.obsClient.CompleteMultipartUploadWithSignedUrl(
		createCompleteMultipartUploadSignedUrlResp.SignedUrl,
		completeMultipartUploadWithSignedUrlHeader,
		strings.NewReader(string(completeMultipartUploadPartXML)))
	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) {
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.CompleteMultipartUploadWithSignedUrl failed.",
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
			return output, err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"obsClient.CompleteMultipartUploadWithSignedUrl finish.")
	return output, nil
}

func (o *S3Proxy) AbortMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectKey,
	uploadId string,
	taskId int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:AbortMultipartUploadWithSignedUrl start.",
		zap.String("objectKey", objectKey),
		zap.String("uploadId", uploadId),
		zap.Int32("taskId", taskId))

	createAbortMultipartUploadSignedUrlReq :=
		new(CreateAbortMultipartUploadSignedUrlReq)
	createAbortMultipartUploadSignedUrlReq.UploadId = uploadId
	createAbortMultipartUploadSignedUrlReq.TaskId = taskId
	createAbortMultipartUploadSignedUrlReq.Source = objectKey

	err, createAbortMultipartUploadSignedUrlResp :=
		UClient.CreateAbortMultipartUploadSignedUrl(
			ctx,
			createAbortMultipartUploadSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateAbortMultipartUploadSignedUrl failed.",
			zap.Error(err))
		return err
	}

	var abortMultipartUploadWithSignedUrlHeader = http.Header{}
	for key, item := range createAbortMultipartUploadSignedUrlResp.Header {
		for _, value := range item.Values {
			abortMultipartUploadWithSignedUrlHeader.Set(key, value)
		}
	}
	_, err =
		o.obsClient.AbortMultipartUploadWithSignedUrl(
			createAbortMultipartUploadSignedUrlResp.SignedUrl,
			abortMultipartUploadWithSignedUrlHeader)
	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) {
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.AbortMultipartUploadWithSignedUrl failed.",
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy.AbortMultipartUploadWithSignedUrl finish.")
	return
}

func (o *S3Proxy) GetObjectInfoWithSignedUrl(
	ctx context.Context,
	objectKey string,
	taskId int32) (
	getObjectMetaOutput *obs.GetObjectMetadataOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:GetObjectInfoWithSignedUrl start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

	createGetObjectMetadataSignedUrlReq :=
		new(CreateGetObjectMetadataSignedUrlReq)
	createGetObjectMetadataSignedUrlReq.TaskId = taskId
	createGetObjectMetadataSignedUrlReq.Source = objectKey

	err, createGetObjectMetadataSignedUrlResp :=
		UClient.CreateGetObjectMetadataSignedUrl(
			ctx,
			createGetObjectMetadataSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateGetObjectMetadataSignedUrl failed.",
			zap.Error(err))
		return
	}
	var getObjectMetadataWithSignedUrlHeader = http.Header{}
	for key, item := range createGetObjectMetadataSignedUrlResp.Header {
		for _, value := range item.Values {
			getObjectMetadataWithSignedUrlHeader.Set(key, value)
		}
	}

	getObjectMetaOutput, err = o.obsClient.GetObjectMetadataWithSignedUrl(
		createGetObjectMetadataSignedUrlResp.SignedUrl,
		getObjectMetadataWithSignedUrlHeader)

	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) {
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.GetObjectMetadataWithSignedUrl failed.",
				zap.String("signedUrl",
					createGetObjectMetadataSignedUrlResp.SignedUrl),
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
			return
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy.GetObjectInfoWithSignedUrl success.")
	return
}

func (o *S3Proxy) ListObjectsWithSignedUrl(
	ctx context.Context,
	taskId int32,
	marker string) (listObjectsOutput *obs.ListObjectsOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:ListObjectsWithSignedUrl start.",
		zap.Int32("taskId", taskId),
		zap.String("marker", marker))

	createListObjectsSignedUrlReq := new(CreateListObjectsSignedUrlReq)
	createListObjectsSignedUrlReq.TaskId = taskId
	if "" != marker {
		createListObjectsSignedUrlReq.Marker = &marker
	}

	err, createListObjectsSignedUrlResp :=
		UClient.CreateListObjectsSignedUrl(
			ctx,
			createListObjectsSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateListObjectsSignedUrl failed.",
			zap.Error(err))
		return listObjectsOutput, err
	}

	var listObjectsWithSignedUrlHeader = http.Header{}
	for key, item := range createListObjectsSignedUrlResp.Header {
		for _, value := range item.Values {
			listObjectsWithSignedUrlHeader.Set(key, value)
		}
	}
	listObjectsOutput = new(obs.ListObjectsOutput)
	listObjectsOutput, err =
		o.obsClient.ListObjectsWithSignedUrl(
			createListObjectsSignedUrlResp.SignedUrl,
			listObjectsWithSignedUrlHeader)
	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) {
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.ListObjectsWithSignedUrl failed.",
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
			return listObjectsOutput, err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:ListObjectsWithSignedUrl finish.")
	return listObjectsOutput, nil
}

func (o *S3Proxy) GetObjectWithSignedUrl(
	ctx context.Context,
	objectKey string,
	taskId int32,
	partNumber,
	rangeStart,
	rangeEnd int64) (output *obs.GetObjectOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:GetObjectWithSignedUrl start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId),
		zap.Int64("partNumber", partNumber),
		zap.Int64("rangeStart", rangeStart),
		zap.Int64("rangeEnd", rangeEnd))

	createGetObjectSignedUrlReq := new(CreateGetObjectSignedUrlReq)
	createGetObjectSignedUrlReq.TaskId = taskId
	createGetObjectSignedUrlReq.Source = objectKey
	createGetObjectSignedUrlReq.RangeStart = rangeStart
	createGetObjectSignedUrlReq.RangeEnd = rangeEnd

	err, createGetObjectSignedUrlResp :=
		UClient.CreateGetObjectSignedUrl(
			ctx,
			createGetObjectSignedUrlReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.CreateGetObjectSignedUrl failed.",
			zap.Error(err))
		return output, err
	}
	var getObjectWithSignedUrlHeader = http.Header{}
	for key, item := range createGetObjectSignedUrlResp.Header {
		for _, value := range item.Values {
			getObjectWithSignedUrlHeader.Set(key, value)
		}
	}

	output, err =
		o.obsClient.GetObjectWithSignedUrl(
			createGetObjectSignedUrlResp.SignedUrl,
			getObjectWithSignedUrlHeader)

	if nil != err {
		var obsError obs.ObsError
		if errors.As(err, &obsError) &&
			obsError.StatusCode >= 400 && obsError.StatusCode < 500 {

			ErrorLogger.WithContext(ctx).Error(
				"obsClient.GetObjectWithSignedUrl failed.",
				zap.String("source", objectKey),
				zap.Int32("taskId", taskId),
				zap.Int64("partNumber", partNumber),
				zap.Int64("rangeStart", rangeStart),
				zap.Int64("rangeEnd", rangeEnd),
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
		}
		return output, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:GetObjectWithSignedUrl finish.")
	return output, err
}

func (o *S3Proxy) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:loadCheckpointFile start.",
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
		"S3Proxy:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *S3Proxy) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *DownloadCheckpoint) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:sliceObject start.",
		zap.Int64("objectSize", objectSize),
		zap.Int64("partSize", partSize))

	cnt := objectSize / partSize
	if objectSize%partSize > 0 {
		cnt++
	}

	if cnt == 0 {
		downloadPart := obs.DownloadPartInfo{}
		downloadPart.PartNumber = 1
		dfc.DownloadParts = []obs.DownloadPartInfo{downloadPart}
	} else {
		downloadParts := make([]obs.DownloadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			downloadPart := obs.DownloadPartInfo{}
			downloadPart.PartNumber = i + 1
			downloadPart.Offset = i * partSize
			downloadPart.RangeEnd = (i+1)*partSize - 1
			downloadParts = append(downloadParts, downloadPart)
		}
		dfc.DownloadParts = downloadParts
		if value := objectSize % partSize; value > 0 {
			dfc.DownloadParts[cnt-1].RangeEnd = dfc.ObjectInfo.Size - 1
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:sliceObject finish.")
}

func (o *S3Proxy) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:updateCheckpointFile start.",
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
		"updateCheckpointFile finish.")
	return err
}

func (o *S3Proxy) Upload(
	ctx context.Context,
	userId string,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:Upload start.",
		zap.String("userId", userId),
		zap.String("sourcePath", sourcePath),
		zap.Int32("taskId", taskId),
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
		err = o.uploadFolder(ctx, sourcePath, taskId, needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy.uploadFolder failed.",
				zap.String("sourcePath", sourcePath),
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return err
		}
	} else {
		objectKey := filepath.Base(sourcePath)
		err = o.uploadFile(
			ctx,
			sourcePath,
			objectKey,
			taskId,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy.uploadFile failed.",
				zap.String("sourcePath", sourcePath),
				zap.String("objectKey", objectKey),
				zap.Int32("taskId", taskId),
				zap.Bool("needPure", needPure),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:Upload finish.")
	return nil
}

func (o *S3Proxy) uploadFolder(
	ctx context.Context,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:uploadFolder start.",
		zap.String("sourcePath", sourcePath),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure))

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	uploadFolderRecord :=
		strings.TrimSuffix(sourcePath, "/") +
			"_" +
			strconv.FormatInt(int64(taskId), 10) +
			UploadFolderRecordSuffix
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
							"S3Proxy:uploadFileResume failed.",
							zap.Any("error", _err))
						isAllSuccess = false
					}
				}()
				objectKey, _err := filepath.Rel(sourcePath, filePath)
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"filepath.Rel failed.",
						zap.String("sourcePath", sourcePath),
						zap.String("filePath", filePath),
						zap.String("objectKey", objectKey),
						zap.Error(_err))
					return
				}
				// 统一linux目录风格
				objectKey = filepath.ToSlash(objectKey)
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
				if fileInfo.IsDir() {
					_err = RetryV1(
						ctx,
						Attempts,
						Delay*time.Second,
						func() error {
							__err := o.NewFolderWithSignedUrl(
								ctx,
								objectKey,
								taskId)
							if nil != __err {
								ErrorLogger.WithContext(ctx).Error(
									"S3Proxy:NewFolderWithSignedUrl"+
										" failed.",
									zap.String("objectKey", objectKey),
									zap.Int32("taskId", taskId),
									zap.Error(__err))
								return __err
							}
							return nil
						})
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"new folder failed.",
							zap.String("objectKey", objectKey),
							zap.Int32("taskId", taskId),
							zap.Error(_err))
						return
					}
				} else {
					_err = RetryV1(
						ctx,
						Attempts,
						Delay*time.Second,
						func() error {
							__err := o.uploadFile(
								ctx,
								filePath,
								objectKey,
								taskId,
								needPure)
							if nil != __err {
								ErrorLogger.WithContext(ctx).Error(
									"S3Proxy:uploadFile failed.",
									zap.String("objectKey", objectKey),
									zap.Int32("taskId", taskId),
									zap.Error(__err))
								return __err
							}
							return nil
						})
					if nil != _err {
						isAllSuccess = false
						ErrorLogger.WithContext(ctx).Error(
							"upload file failed.",
							zap.String("objectKey", objectKey),
							zap.Int32("taskId", taskId),
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
						zap.Int32("taskId", taskId),
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
					zap.Int32("taskId", taskId),
					zap.Error(err))
				return err
			}
			return nil
		})
	wg.Wait()

	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"filepath.Walk failed.",
			zap.Int32("taskId", taskId),
			zap.String("sourcePath", sourcePath),
			zap.Error(err))
		return err
	}
	if !isAllSuccess {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:uploadFolder not all success.",
			zap.Int32("taskId", taskId),
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
		"S3Proxy:uploadFolder finish.")
	return nil
}

func (o *S3Proxy) uploadFile(
	ctx context.Context,
	sourceFile,
	objectKey string,
	taskId int32,
	needPure bool) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:uploadFile start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId),
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
		_, err = o.uploadFileResume(
			ctx,
			sourceFile,
			objectKey,
			taskId,
			needPure)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy:uploadFileResume failed.",
				zap.String("sourceFile", sourceFile),
				zap.String("objectKey", objectKey),
				zap.Int32("taskId", taskId),
				zap.Bool("needPure", needPure),
				zap.Error(err))
			return err
		}
	} else {
		err = o.PutObjectWithSignedUrl(
			ctx,
			sourceFile,
			objectKey,
			taskId)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy:PutObjectWithSignedUrl failed.",
				zap.String("sourceFile", sourceFile),
				zap.String("objectKey", objectKey),
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return err
		}
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:uploadFile finish.")
	return err
}

func (o *S3Proxy) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectKey string,
	taskId int32,
	needPure bool) (
	output *obs.CompleteMultipartUploadOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:uploadFileResume start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId),
		zap.Bool("needPure", needPure))

	uploadFileInput := new(obs.UploadFileInput)
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile +
			"_" +
			strconv.FormatInt(int64(taskId), 10) +
			UploadFileRecordSuffix
	uploadFileInput.TaskNum = o.s3UploadMultiTaskNum
	uploadFileInput.PartSize = DefaultPartSize
	uploadFileInput.Key = objectKey
	if uploadFileInput.PartSize < obs.MIN_PART_SIZE {
		uploadFileInput.PartSize = obs.MIN_PART_SIZE
	} else if uploadFileInput.PartSize > obs.MAX_PART_SIZE {
		uploadFileInput.PartSize = obs.MAX_PART_SIZE
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
				return output, err
			}
		}
	}

	output, err = o.resumeUpload(
		ctx,
		sourceFile,
		objectKey,
		taskId,
		uploadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:resumeUpload failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("objectKey", objectKey),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return output, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:uploadFileResume finish.")
	return output, err
}

func (o *S3Proxy) resumeUpload(
	ctx context.Context,
	sourceFile, objectKey string,
	taskId int32,
	input *obs.UploadFileInput) (
	output *obs.CompleteMultipartUploadOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:resumeUpload start.",
		zap.String("sourceFile", sourceFile),
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

	uploadFileStat, err := os.Stat(input.UploadFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("uploadFile", input.UploadFile),
			zap.Error(err))
		return nil, err
	}
	if uploadFileStat.IsDir() {
		ErrorLogger.WithContext(ctx).Error(
			"uploadFile can not be a folder.",
			zap.String("uploadFile", input.UploadFile))
		return nil, errors.New("uploadFile can not be a folder")
	}

	ufc := &UploadCheckpoint{}

	var needCheckpoint = true
	var checkpointFilePath = input.CheckpointFile
	var enableCheckpoint = input.EnableCheckpoint
	if enableCheckpoint {
		needCheckpoint, err = o.getUploadCheckpointFile(
			ctx,
			objectKey,
			taskId,
			ufc,
			uploadFileStat,
			input)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy:getUploadCheckpointFile failed.",
				zap.String("objectKey", objectKey),
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return nil, err
		}
	}
	if needCheckpoint {
		err = o.prepareUpload(
			ctx,
			objectKey,
			taskId,
			ufc,
			uploadFileStat,
			input)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy:prepareUpload failed.",
				zap.String("objectKey", objectKey),
				zap.Int32("taskId", taskId),
				zap.Error(err))
			return nil, err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"S3Proxy:updateCheckpointFile failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Error(err))
				_err := o.AbortMultipartUploadWithSignedUrl(
					ctx,
					objectKey,
					ufc.UploadId,
					taskId)
				if nil != _err {
					ErrorLogger.WithContext(ctx).Error(
						"S3Proxy:AbortMultipartUploadWithSignedUrl"+
							" failed.",
						zap.String("objectKey", objectKey),
						zap.String("uploadId", ufc.UploadId),
						zap.Int32("taskId", taskId),
						zap.Error(_err))
				}
				return nil, err
			}
		}
	}

	uploadPartError := o.uploadPartConcurrent(
		ctx,
		sourceFile,
		taskId,
		ufc,
		input)
	err = o.handleUploadFileResult(
		ctx,
		objectKey,
		taskId,
		uploadPartError,
		ufc,
		enableCheckpoint)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:handleUploadFileResult failed.",
			zap.String("objectKey", objectKey),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return nil, err
	}

	completeOutput, err := o.completeParts(
		ctx,
		objectKey,
		taskId,
		ufc,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:completeParts failed.",
			zap.String("objectKey", objectKey),
			zap.Int32("taskId", taskId),
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return completeOutput, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:resumeUpload finish.")
	return completeOutput, err
}

func (o *S3Proxy) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	taskId int32,
	ufc *UploadCheckpoint,
	input *obs.UploadFileInput) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:uploadPartConcurrent start.",
		zap.String("sourceFile", sourceFile),
		zap.Int32("taskId", taskId))

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
	var abort int32
	lock := new(sync.Mutex)

	for _, uploadPart := range ufc.UploadParts {
		if atomic.LoadInt32(&abort) == 1 {
			break
		}
		if uploadPart.IsCompleted {
			continue
		}
		task := UploadPartTask{
			UploadPartInput: obs.UploadPartInput{
				Bucket:     ufc.Bucket,
				Key:        ufc.Key,
				PartNumber: uploadPart.PartNumber,
				UploadId:   ufc.UploadId,
				SseHeader:  input.SseHeader,
				SourceFile: input.UploadFile,
				Offset:     uploadPart.Offset,
				PartSize:   uploadPart.PartSize,
			},
			s3Proxy:          o,
			abort:            &abort,
			enableCheckpoint: input.EnableCheckpoint,
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
			}()
			result := task.Run(ctx, sourceFile, ufc.UploadId, taskId)
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
					"S3Proxy:handleUploadTaskResult failed.",
					zap.Int("partNumber", task.PartNumber),
					zap.String("checkpointFile", input.CheckpointFile),
					zap.Error(_err))
				uploadPartError.Store(_err)
			}
			InfoLogger.WithContext(ctx).Debug(
				"S3Proxy:handleUploadTaskResult finish.")
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
		"S3Proxy:uploadPartConcurrent finish.")
	return nil
}

func (o *S3Proxy) getUploadCheckpointFile(
	ctx context.Context,
	objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *obs.UploadFileInput) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:getUploadCheckpointFile start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

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
			"S3Proxy:loadCheckpointFile failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return true, nil
	} else if !ufc.isValid(ctx, input.Key, input.UploadFile, uploadFileStat) {
		if ufc.Key != "" && ufc.UploadId != "" {
			_err := o.AbortMultipartUploadWithSignedUrl(
				ctx,
				objectKey,
				ufc.UploadId,
				taskId)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"S3Proxy:AbortMultipartUploadWithSignedUrl failed.",
					zap.String("objectKey", objectKey),
					zap.String("uploadId", ufc.UploadId),
					zap.Int32("taskId", taskId),
					zap.Error(_err))
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
			"S3Proxy:loadCheckpointFile finish.",
			zap.String("checkpointFilePath", checkpointFilePath))
		return false, nil
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *S3Proxy) prepareUpload(
	ctx context.Context,
	objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *obs.UploadFileInput) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:prepareUpload start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

	initiateMultipartUploadOutput, err :=
		o.InitiateMultipartUploadWithSignedUrl(
			ctx,
			objectKey,
			taskId)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:InitiateMultipartUploadWithSignedUrl failed.",
			zap.String("objectKey", objectKey),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return err
	}

	ufc.Key = input.Key
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = obs.FileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()
	ufc.UploadId = initiateMultipartUploadOutput.UploadId

	err = o.sliceFile(ctx, input.PartSize, ufc)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:sliceFile failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:prepareUpload finish.")
	return err
}

func (o *S3Proxy) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *UploadCheckpoint) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:sliceFile start.",
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

	if partSize > obs.MAX_PART_SIZE {
		ErrorLogger.WithContext(ctx).Error(
			"upload file part too large.",
			zap.Int64("partSize", partSize),
			zap.Int("maxPartSize", obs.MAX_PART_SIZE))

		return fmt.Errorf("upload file part too large")
	}

	if cnt == 0 {
		uploadPart := obs.UploadPartInfo{}
		uploadPart.PartNumber = 1
		ufc.UploadParts = []obs.UploadPartInfo{uploadPart}
	} else {
		uploadParts := make([]obs.UploadPartInfo, 0, cnt)
		var i int64
		for i = 0; i < cnt; i++ {
			uploadPart := obs.UploadPartInfo{}
			uploadPart.PartNumber = int(i) + 1
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
		"S3Proxy:sliceFile finish.")
	return nil
}

func (o *S3Proxy) handleUploadTaskResult(
	ctx context.Context,
	result interface{},
	ufc *UploadCheckpoint,
	partNum int,
	enableCheckpoint bool,
	checkpointFilePath string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:handleUploadTaskResult start.",
		zap.String("checkpointFilePath", checkpointFilePath),
		zap.Int("partNum", partNum))

	if uploadPartOutput, ok := result.(*obs.UploadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].Etag = uploadPartOutput.ETag
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Error(
					"S3Proxy:updateCheckpointFile failed.",
					zap.String("checkpointFilePath", checkpointFilePath),
					zap.Int("partNum", partNum),
					zap.Error(_err))
			}
		}
	} else if result != ErrAbort {
		if _err, ok := result.(error); ok {
			ErrorLogger.WithContext(ctx).Error(
				"upload task result failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Int("partNum", partNum),
				zap.Error(_err))
			err = _err
		}
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:handleUploadTaskResult finish.")
	return
}

func (o *S3Proxy) handleUploadFileResult(
	ctx context.Context,
	objectKey string,
	taskId int32,
	uploadPartError error,
	ufc *UploadCheckpoint,
	enableCheckpoint bool) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:handleUploadFileResult start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId),
		zap.String("uploadId", ufc.UploadId))

	if uploadPartError != nil {
		InfoLogger.WithContext(ctx).Debug(
			"uploadPartError not nil.",
			zap.Error(uploadPartError))
		if enableCheckpoint {
			InfoLogger.WithContext(ctx).Debug(
				"enableCheckpoint return uploadPartError.")
			return uploadPartError
		}
		_err := o.AbortMultipartUploadWithSignedUrl(
			ctx,
			objectKey,
			ufc.UploadId,
			taskId)
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy:AbortMultipartUploadWithSignedUrl start.",
				zap.String("objectKey", objectKey),
				zap.Int32("taskId", taskId),
				zap.String("uploadId", ufc.UploadId),
				zap.Error(_err))
		}
		return uploadPartError
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:handleUploadFileResult finish.")
	return nil
}

func (o *S3Proxy) completeParts(
	ctx context.Context,
	objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	enableCheckpoint bool,
	checkpointFilePath string) (
	output *obs.CompleteMultipartUploadOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:completeParts start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId),
		zap.String("uploadId", ufc.UploadId),
		zap.String("checkpointFilePath", checkpointFilePath))

	parts := make([]XPart, 0, len(ufc.UploadParts))
	for _, uploadPart := range ufc.UploadParts {
		part := XPart{}
		part.PartNumber = uploadPart.PartNumber
		part.ETag = uploadPart.Etag
		parts = append(parts, part)
	}
	var partSlice PartSlice = parts
	sort.Sort(partSlice)
	output, err = o.CompleteMultipartUploadWithSignedUrl(
		ctx,
		objectKey,
		ufc.UploadId,
		taskId,
		partSlice)
	if nil == err {
		if enableCheckpoint {
			_err := os.Remove(checkpointFilePath)
			if nil != _err {
				if !os.IsNotExist(_err) {
					ErrorLogger.WithContext(ctx).Error(
						"os.Remove failed.",
						zap.String("checkpointFilePath",
							checkpointFilePath),
						zap.Error(_err))
				}
			}
		}
		InfoLogger.WithContext(ctx).Debug(
			"S3Proxy:completeParts finish.")
		return output, err
	}
	if !enableCheckpoint {
		_err := o.AbortMultipartUploadWithSignedUrl(
			ctx,
			objectKey,
			ufc.UploadId,
			taskId)
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy.AbortMultipartUploadWithSignedUrl failed.",
				zap.String("objectKey", objectKey),
				zap.String("uploadId", ufc.UploadId),
				zap.Int32("taskId", taskId),
				zap.Error(_err))
		}
	}
	ErrorLogger.WithContext(ctx).Error(
		"S3Proxy.CompleteMultipartUploadWithSignedUrl failed.",
		zap.String("objectKey", objectKey),
		zap.String("uploadId", ufc.UploadId),
		zap.Int32("taskId", taskId),
		zap.Error(err))
	return output, err
}

type UploadCheckpoint struct {
	XMLName     xml.Name             `xml:"UploadFileCheckpoint"`
	Bucket      string               `xml:"Bucket"`
	Key         string               `xml:"Key"`
	UploadId    string               `xml:"UploadId,omitempty"`
	UploadFile  string               `xml:"FileUrl"`
	FileInfo    obs.FileStatus       `xml:"FileInfo"`
	UploadParts []obs.UploadPartInfo `xml:"UploadParts>UploadPart"`
}

func (ufc *UploadCheckpoint) isValid(
	ctx context.Context,
	key, uploadFile string,
	fileStat os.FileInfo) bool {

	InfoLogger.WithContext(ctx).Debug(
		"UploadCheckpoint:isValid start.",
		zap.String("key", key),
		zap.String("ufcKey", ufc.Key),
		zap.String("uploadFile", uploadFile),
		zap.String("ufcUploadFile", ufc.UploadFile),
		zap.Int64("fileStatSize", fileStat.Size()),
		zap.Int64("ufcFileInfoSize", ufc.FileInfo.Size),
		zap.String("uploadId", ufc.UploadId))

	if ufc.Key != key ||
		ufc.UploadFile != uploadFile {
		ErrorLogger.WithContext(ctx).Error(
			"Checkpoint file is invalid." +
				" bucketName or objectKey or uploadFile was changed.")
		return false
	}
	if ufc.FileInfo.Size != fileStat.Size() ||
		ufc.FileInfo.LastModified != fileStat.ModTime().Unix() {
		ErrorLogger.WithContext(ctx).Error(
			"Checkpoint file is invalid. uploadFile was changed.")
		return false
	}
	if ufc.UploadId == "" {
		ErrorLogger.WithContext(ctx).Error(
			"UploadId is invalid.")
		return false
	}
	InfoLogger.WithContext(ctx).Debug(
		"UploadCheckpoint:isValid finish.")
	return true
}

type UploadPartTask struct {
	obs.UploadPartInput
	s3Proxy          *S3Proxy
	abort            *int32
	enableCheckpoint bool
}

func (task *UploadPartTask) Run(
	ctx context.Context,
	sourceFile, uploadId string,
	taskId int32) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"UploadPartTask:Run start.",
		zap.String("sourceFile", sourceFile),
		zap.String("uploadId", uploadId),
		zap.Int32("taskId", taskId),
		zap.Int32("partNumber", int32(task.PartNumber)),
		zap.String("source", task.Key))

	if atomic.LoadInt32(task.abort) == 1 {
		ErrorLogger.WithContext(ctx).Error(
			"task abort.")
		return ErrAbort
	}

	uploadPartOutput, err := task.s3Proxy.UploadPartWithSignedUrl(
		ctx,
		sourceFile,
		uploadId,
		taskId,
		task.Key,
		int32(task.PartNumber),
		task.Offset,
		task.PartSize)

	if nil == err {
		if uploadPartOutput.ETag == "" {
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.UploadPartWithSignedUrl failed."+
					" invalid etag value.",
				zap.String("uploadId", uploadId),
				zap.Int32("partNumber", int32(task.PartNumber)),
				zap.Int32("taskId", taskId),
				zap.String("source", task.Key))
			if !task.enableCheckpoint {
				atomic.CompareAndSwapInt32(task.abort, 0, 1)
				ErrorLogger.WithContext(ctx).Error(
					"obsClient.UploadPartWithSignedUrl failed."+
						" invalid etag value. aborted task.",
					zap.String("uploadId", uploadId),
					zap.Int32("partNumber", int32(task.PartNumber)),
					zap.Int32("taskId", taskId),
					zap.String("source", task.Key))
			}
			return errors.New("invalid etag value")
		}
		InfoLogger.WithContext(ctx).Debug(
			"UploadPartTask:Run finish.",
			zap.String("uploadId", uploadId),
			zap.Int32("partNumber", int32(task.PartNumber)),
			zap.Int32("taskId", taskId),
			zap.String("source", task.Key))
		return uploadPartOutput
	} else {
		var obsError obs.ObsError
		if errors.As(err, &obsError) &&
			obsError.StatusCode >= 400 && obsError.StatusCode < 500 {

			atomic.CompareAndSwapInt32(task.abort, 0, 1)
			ErrorLogger.WithContext(ctx).Error(
				"obsClient.UploadPartWithSignedUrl failed.",
				zap.String("uploadId", uploadId),
				zap.Int32("partNumber", int32(task.PartNumber)),
				zap.Int32("taskId", taskId),
				zap.String("source", task.Key),
				zap.String("obsCode", obsError.Code),
				zap.String("obsMessage", obsError.Message))
		}
	}

	ErrorLogger.WithContext(ctx).Error(
		"obsClient.UploadPartWithSignedUrl failed.",
		zap.String("uploadId", uploadId),
		zap.Int32("partNumber", int32(task.PartNumber)),
		zap.Int32("taskId", taskId),
		zap.String("source", task.Key),
		zap.Error(err))
	return err
}

func (o *S3Proxy) Download(
	ctx context.Context,
	userId string,
	targetPath string,
	taskId int32,
	bucketName string) (err error) {

	if '/' != targetPath[len(targetPath)-1] {
		targetPath = targetPath + "/"
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:Download start.",
		zap.String("userId", userId),
		zap.String("targetPath", targetPath),
		zap.Int32("taskId", taskId),
		zap.String("bucketName", bucketName))

	getTaskReq := new(GetTaskReq)
	getTaskReq.UserId = userId
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		ErrorLogger.WithContext(ctx).Error(
			"task not exist.",
			zap.Int32("taskId", taskId))
		return errors.New("task not exist")
	}
	task := getTaskResp.Data.List[0].Task

	var uuid, location string
	if TaskTypeDownload == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(DownloadObjectTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"DownloadObjectTaskParams Unmarshal failed.",
				zap.String("params", getTaskResp.Data.List[0].Task.Params),
				zap.Error(err))
			return err
		}
		uuid = taskParams.Request.ObjUuid
		location = taskParams.Location
	} else if TaskTypeDownloadFile == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(DownloadFileTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"DownloadFileTaskParams Unmarshal failed.",
				zap.String("params", getTaskResp.Data.List[0].Task.Params),
				zap.Error(err))
			return err
		}
		uuid = taskParams.Request.ObjUuid
		location = taskParams.Location
	} else if TaskTypeLoad == getTaskResp.Data.List[0].Task.Type {
		taskParams := new(LoadObjectTaskParams)
		err = json.Unmarshal(
			[]byte(getTaskResp.Data.List[0].Task.Params),
			taskParams)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"DownloadFileTaskParams Unmarshal failed.",
				zap.String("params", getTaskResp.Data.List[0].Task.Params),
				zap.Error(err))
			return err
		}
		uuid = taskParams.Request.ObjUuid
		location = taskParams.SourceLocation
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"task type invalid.",
			zap.Int32("taskId", taskId))
		return errors.New("task type invalid")
	}

	downloadObjectTaskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadObjectTaskParams)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"DownloadObjectTaskParams Unmarshal failed.",
			zap.Int32("taskId", taskId),
			zap.String("params", task.Params),
			zap.Error(err))
		return err
	}

	downloadFolderRecord := targetPath +
		uuid +
		fmt.Sprintf("_%d_tmp", taskId) +
		DownloadFolderRecordSuffix
	tmpTargetPath := targetPath + uuid + fmt.Sprintf("_%d_tmp/", taskId)

	marker := ""
	for {
		err, listObjectsOutputTmp := RetryV4(
			ctx,
			Attempts,
			Delay*time.Second,
			func() (error, interface{}) {
				output := new(obs.ListObjectsOutput)
				output, _err := o.ListObjectsWithSignedUrl(
					ctx,
					taskId,
					marker)
				if nil != _err {
					ErrorLogger.WithContext(ctx).Error(
						"S3Proxy:ListObjectsWithSignedUrl start.",
						zap.Int32("taskId", taskId),
						zap.Error(_err))
					return _err, output
				}
				return _err, output
			})
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"list objects failed.",
				zap.Int32("taskId", taskId),
				zap.String("marker", marker),
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
			userId,
			tmpTargetPath,
			taskId,
			bucketName,
			listObjectsOutput,
			downloadFolderRecord)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy:downloadObjects failed.",
				zap.String("userId", userId),
				zap.String("tmpTargetPath", tmpTargetPath),
				zap.Int32("taskId", taskId),
				zap.String("bucketName", bucketName),
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
	fromPath := tmpTargetPath + strings.TrimSuffix(location, "/")
	toPath := targetPath + uuid + fmt.Sprintf("_%d", taskId)
	err = os.Rename(fromPath, toPath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Rename failed.",
			zap.String("fromPath", fromPath),
			zap.String("toPath", toPath),
			zap.Error(err))
		return err
	}
	tmpPath := targetPath + uuid + fmt.Sprintf("_%d_tmp", taskId)
	err = os.RemoveAll(tmpPath)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.RemoveAll failed.",
			zap.String("tmpPath", tmpPath),
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
		"S3Proxy:Download finish.")
	return nil
}

func (o *S3Proxy) downloadObjects(
	ctx context.Context,
	userId string,
	targetPath string,
	taskId int32,
	bucketName string,
	listObjectsOutput *obs.ListObjectsOutput,
	downloadFolderRecord string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:downloadObjects start.",
		zap.String("userId", userId),
		zap.String("targetPath", targetPath),
		zap.Int32("taskId", taskId),
		zap.String("bucketName", bucketName),
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
				_err := RetryV1(
					ctx,
					Attempts,
					Delay*time.Second,
					func() error {
						_, __err := o.downloadPartWithSignedUrl(
							ctx,
							bucketName,
							itemObject.Key,
							targetPath+itemObject.Key,
							taskId)
						if nil != __err {
							ErrorLogger.WithContext(ctx).Error(
								"S3Proxy:downloadPartWithSignedUrl failed.",
								zap.String("bucketName", bucketName),
								zap.String("objectKey", itemObject.Key),
								zap.String("targetFile",
									targetPath+itemObject.Key),
								zap.Int32("taskId", taskId),
								zap.Error(__err))
							return __err
						}
						return __err
					})
				if nil != _err {
					isAllSuccess = false
					ErrorLogger.WithContext(ctx).Error(
						"S3Proxy:downloadObjects failed.",
						zap.String("bucketName", bucketName),
						zap.String("objectKey", itemObject.Key),
						zap.String("targetFile",
							targetPath+itemObject.Key),
						zap.Int32("taskId", taskId),
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
			"S3Proxy:downloadObjects not all success.")
		return errors.New("downloadObjects not all success")
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:downloadObjects finish.")
	return nil
}

func (o *S3Proxy) downloadPartWithSignedUrl(
	ctx context.Context,
	bucketName, objectKey, targetFile string,
	taskId int32) (output *obs.GetObjectMetadataOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:downloadPartWithSignedUrl start.",
		zap.String("bucketName", bucketName),
		zap.String("objectKey", objectKey),
		zap.String("targetFile", targetFile),
		zap.Int32("taskId", taskId))

	if '/' == objectKey[len(objectKey)-1] {
		parentDir := filepath.Dir(targetFile)
		stat, err := os.Stat(parentDir)
		if nil != err {
			if !os.IsNotExist(err) {
				ErrorLogger.WithContext(ctx).Error(
					"os.Stat failed.",
					zap.String("parentDir", parentDir),
					zap.Error(err))
				return output, err
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
				return output, _err
			}
			return output, nil
		} else if !stat.IsDir() {
			ErrorLogger.WithContext(ctx).Error(
				"same file exists.",
				zap.String("parentDir", parentDir))
			return output,
				fmt.Errorf(
					"cannot create folder: %s same file exists",
					parentDir)
		} else {
			InfoLogger.WithContext(ctx).Debug(
				"no need download.")
			return output, nil
		}
	}

	downloadFileInput := new(obs.DownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + DownloadFileRecordSuffix
	downloadFileInput.TaskNum = o.s3DownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize
	downloadFileInput.Bucket = bucketName
	downloadFileInput.Key = objectKey

	output, err = o.resumeDownload(
		ctx,
		objectKey,
		taskId,
		downloadFileInput)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:resumeDownload failed.",
			zap.String("objectKey", objectKey),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return output, err
	}
	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:downloadPartWithSignedUrl finish.")
	return
}

func (o *S3Proxy) resumeDownload(
	ctx context.Context,
	objectKey string,
	taskId int32,
	input *obs.DownloadFileInput) (
	output *obs.GetObjectMetadataOutput, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:resumeDownload start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

	getObjectMetaOutput, err := o.GetObjectInfoWithSignedUrl(
		ctx,
		objectKey,
		taskId)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:GetObjectInfoWithSignedUrl failed.",
			zap.String("objectKey", objectKey),
			zap.Int32("taskId", taskId),
			zap.Error(err))
		return nil, err
	}

	objectSize := getObjectMetaOutput.ContentLength
	partSize := input.PartSize
	dfc := &DownloadCheckpoint{}

	var needCheckpoint = true
	var checkpointFilePath = input.CheckpointFile
	var enableCheckpoint = input.EnableCheckpoint
	if enableCheckpoint {
		needCheckpoint, err = o.getDownloadCheckpointFile(
			ctx,
			dfc,
			input,
			getObjectMetaOutput)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy:getDownloadCheckpointFile failed.",
				zap.String("checkpointFilePath", checkpointFilePath),
				zap.Error(err))
			return nil, err
		}
	}

	if needCheckpoint {
		dfc.Bucket = input.Bucket
		dfc.Key = input.Key
		dfc.VersionId = input.VersionId
		dfc.DownloadFile = input.DownloadFile
		dfc.ObjectInfo = obs.ObjectInfo{}
		dfc.ObjectInfo.LastModified = getObjectMetaOutput.LastModified.Unix()
		dfc.ObjectInfo.Size = getObjectMetaOutput.ContentLength
		dfc.ObjectInfo.ETag = getObjectMetaOutput.ETag
		dfc.TempFileInfo = obs.TempFileInfo{}
		dfc.TempFileInfo.TempFileUrl = input.DownloadFile + ".tmp"
		dfc.TempFileInfo.Size = getObjectMetaOutput.ContentLength

		o.sliceObject(ctx, objectSize, partSize, dfc)
		err = o.prepareTempFile(ctx,
			dfc.TempFileInfo.TempFileUrl,
			dfc.TempFileInfo.Size)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy:prepareTempFile failed.",
				zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
				zap.Int64("Size", dfc.TempFileInfo.Size),
				zap.Error(err))
			return nil, err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				ErrorLogger.WithContext(ctx).Error(
					"S3Proxy:updateCheckpointFile failed.",
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
				return nil, err
			}
		}
	}

	downloadFileError := o.downloadFileConcurrent(
		ctx, objectKey, taskId, input, dfc)
	err = o.handleDownloadFileResult(
		ctx,
		dfc.TempFileInfo.TempFileUrl,
		enableCheckpoint,
		downloadFileError)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"S3Proxy:handleDownloadFileResult failed.",
			zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
			zap.Error(err))
		return nil, err
	}

	err = os.Rename(dfc.TempFileInfo.TempFileUrl, input.DownloadFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Rename failed.",
			zap.String("TempFileUrl", dfc.TempFileInfo.TempFileUrl),
			zap.String("DownloadFile", input.DownloadFile),
			zap.Error(err))
		return nil, err
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
		"S3Proxy:resumeDownload finish.")
	return getObjectMetaOutput, nil
}

func (o *S3Proxy) downloadFileConcurrent(
	ctx context.Context,
	objectKey string,
	taskId int32,
	input *obs.DownloadFileInput,
	dfc *DownloadCheckpoint) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:downloadFileConcurrent start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId))

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
	var abort int32
	lock := new(sync.Mutex)

	for _, downloadPart := range dfc.DownloadParts {
		if atomic.LoadInt32(&abort) == 1 {
			break
		}
		if downloadPart.IsCompleted {
			continue
		}
		task := DownloadPartTask{
			GetObjectInput: obs.GetObjectInput{
				GetObjectMetadataInput: input.GetObjectMetadataInput,
				IfMatch:                input.IfMatch,
				IfNoneMatch:            input.IfNoneMatch,
				IfUnmodifiedSince:      input.IfUnmodifiedSince,
				IfModifiedSince:        input.IfModifiedSince,
				RangeStart:             downloadPart.Offset,
				RangeEnd:               downloadPart.RangeEnd,
			},
			s3proxy:          o,
			abort:            &abort,
			partNumber:       downloadPart.PartNumber,
			tempFileURL:      dfc.TempFileInfo.TempFileUrl,
			enableCheckpoint: input.EnableCheckpoint,
		}
		InfoLogger.WithContext(ctx).Debug(
			"DownloadPartTask params.",
			zap.Int64("rangeStart", downloadPart.Offset),
			zap.Int64("rangeEnd", downloadPart.RangeEnd),
			zap.Int64("partNumber", downloadPart.PartNumber),
			zap.String("tempFileURL", dfc.TempFileInfo.TempFileUrl),
			zap.Bool("enableCheckpoint", input.EnableCheckpoint))

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
			}()
			if 0 == dfc.ObjectInfo.Size {
				lock.Lock()
				defer lock.Unlock()
				dfc.DownloadParts[task.partNumber-1].IsCompleted = true

				if input.EnableCheckpoint {
					_err := o.updateCheckpointFile(
						ctx,
						dfc,
						input.CheckpointFile)
					if nil != _err {
						ErrorLogger.WithContext(ctx).Error(
							"S3Proxy:updateCheckpointFile failed.",
							zap.String("checkpointFile",
								input.CheckpointFile),
							zap.Error(_err))
						downloadPartError.Store(_err)
					}
				}
				return
			} else {
				result := task.Run(ctx, objectKey, taskId)
				_err := o.handleDownloadTaskResult(
					ctx,
					result,
					dfc,
					task.partNumber,
					input.EnableCheckpoint,
					input.CheckpointFile,
					lock)
				if nil != _err &&
					atomic.CompareAndSwapInt32(&errFlag, 0, 1) {

					ErrorLogger.WithContext(ctx).Error(
						"S3Proxy:handleDownloadTaskResult failed.",
						zap.Int64("partNumber", task.partNumber),
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
		"S3Proxy:downloadFileConcurrent finish.")
	return nil
}

func (o *S3Proxy) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *DownloadCheckpoint,
	input *obs.DownloadFileInput,
	output *obs.GetObjectMetadataOutput) (needCheckpoint bool, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:getDownloadCheckpointFile start.",
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
			"S3Proxy:loadCheckpointFile failed.",
			zap.String("checkpointFilePath", checkpointFilePath),
			zap.Error(err))
		return true, nil
	} else if !dfc.IsValid(ctx, input, output) {
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

func (o *S3Proxy) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:prepareTempFile start.",
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
			"S3Proxy:createFile finish.",
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
		"S3Proxy:prepareTempFile finish.")
	return nil
}

func (o *S3Proxy) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:createFile start.",
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
		"S3Proxy:createFile finish.")
	return nil
}

func (o *S3Proxy) handleDownloadTaskResult(
	ctx context.Context,
	result interface{},
	dfc *DownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:handleDownloadTaskResult start.",
		zap.Int64("partNum", partNum),
		zap.String("checkpointFile", checkpointFile))

	if _, ok := result.(*obs.GetObjectOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				ErrorLogger.WithContext(ctx).Warn(
					"S3Proxy:updateCheckpointFile failed.",
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
		"S3Proxy:handleDownloadTaskResult finish.")
	return
}

func (o *S3Proxy) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:handleDownloadFileResult start.",
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
			"S3Proxy.handleDownloadFileResult finish.",
			zap.String("tempFileURL", tempFileURL),
			zap.Error(downloadFileError))
		return downloadFileError
	}

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy.handleDownloadFileResult finish.")
	return nil
}

func (o *S3Proxy) updateDownloadFile(
	ctx context.Context,
	filePath string,
	rangeStart int64,
	output *obs.GetObjectOutput) error {

	InfoLogger.WithContext(ctx).Debug(
		"S3Proxy:updateDownloadFile start.",
		zap.String("filePath", filePath),
		zap.Int64("rangeStart", rangeStart))

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
	_, err = fd.Seek(rangeStart, 0)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"seek file failed.",
			zap.String("filePath", filePath),
			zap.Int64("rangeStart", rangeStart),
			zap.Error(err))
		return err
	}
	fileWriter := bufio.NewWriterSize(fd, 65536)
	part := make([]byte, 8192)
	var readErr error
	var readCount, readTotal int
	for {
		readCount, readErr = output.Body.Read(part)
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
		"S3Proxy:updateDownloadFile finish.",
		zap.Int("readTotal", readTotal))
	return nil
}

type DownloadCheckpoint struct {
	XMLName       xml.Name               `xml:"DownloadFileCheckpoint"`
	Bucket        string                 `xml:"Bucket"`
	Key           string                 `xml:"Key"`
	VersionId     string                 `xml:"VersionId,omitempty"`
	DownloadFile  string                 `xml:"FileUrl"`
	ObjectInfo    obs.ObjectInfo         `xml:"ObjectInfo"`
	TempFileInfo  obs.TempFileInfo       `xml:"TempFileInfo"`
	DownloadParts []obs.DownloadPartInfo `xml:"DownloadParts>DownloadPart"`
}

func (dfc *DownloadCheckpoint) IsValid(
	ctx context.Context,
	input *obs.DownloadFileInput,
	output *obs.GetObjectMetadataOutput) bool {

	InfoLogger.WithContext(ctx).Debug(
		"DownloadCheckpoint:IsValid start.",
		zap.String("dfcBucket", dfc.Bucket),
		zap.String("inputBucket", input.Bucket),
		zap.String("dfcKey", dfc.Key),
		zap.String("inputKey", input.Key),
		zap.String("dfcVersionId", dfc.VersionId),
		zap.String("inputVersionId", input.VersionId),
		zap.String("dfcDownloadFile", dfc.DownloadFile),
		zap.String("inputDownloadFile", input.DownloadFile),
		zap.Int64("dfcObjectInfoLastModified", dfc.ObjectInfo.LastModified),
		zap.Int64("outputLastModifiedUnix", output.LastModified.Unix()),
		zap.String("dfcObjectInfoETag", dfc.ObjectInfo.ETag),
		zap.String("outputETag", output.ETag),
		zap.Int64("dfcObjectInfoSize", dfc.ObjectInfo.Size),
		zap.Int64("outputContentLength", output.ContentLength),
		zap.Int64("dfcTempFileInfoSize", dfc.TempFileInfo.Size),
		zap.Int64("outputContentLength", output.ContentLength))

	if dfc.Bucket != input.Bucket ||
		dfc.Key != input.Key ||
		dfc.VersionId != input.VersionId ||
		dfc.DownloadFile != input.DownloadFile {

		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid." +
				" the bucketName or objectKey or downloadFile was changed." +
				" clear the record.")
		return false
	}
	if dfc.ObjectInfo.LastModified != output.LastModified.Unix() ||
		dfc.ObjectInfo.ETag != output.ETag ||
		dfc.ObjectInfo.Size != output.ContentLength {

		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid. the object info was changed." +
				" clear the record.")
		return false
	}
	if dfc.TempFileInfo.Size != output.ContentLength {
		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid. size was changed." +
				" clear the record.")
		return false
	}
	stat, err := os.Stat(dfc.TempFileInfo.TempFileUrl)
	if nil != err || stat.Size() != dfc.ObjectInfo.Size {
		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid." +
				" the temp download file was changed. clear the record.")
		return false
	}
	InfoLogger.WithContext(ctx).Debug(
		"DownloadCheckpoint:IsValid finish.")
	return true
}

type DownloadPartTask struct {
	obs.GetObjectInput
	obsClient        *obs.ObsClient
	s3proxy          *S3Proxy
	abort            *int32
	partNumber       int64
	tempFileURL      string
	enableCheckpoint bool
}

func (task *DownloadPartTask) Run(
	ctx context.Context,
	objectKey string,
	taskId int32) interface{} {

	InfoLogger.WithContext(ctx).Debug(
		"DownloadPartTask:Run start.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId),
		zap.Int64("partNumber", task.partNumber))

	if atomic.LoadInt32(task.abort) == 1 {
		InfoLogger.WithContext(ctx).Info(
			"task abort.")
		return ErrAbort
	}

	getObjectWithSignedUrlOutput, err :=
		task.s3proxy.GetObjectWithSignedUrl(
			ctx,
			objectKey,
			taskId,
			task.partNumber,
			task.RangeStart,
			task.RangeEnd)

	if nil == err {
		InfoLogger.WithContext(ctx).Debug(
			"obsClient.GetObjectWithSignedUrl finish.",
			zap.String("objectKey", objectKey),
			zap.Int32("taskId", taskId),
			zap.Int64("partNumber", task.partNumber))
		defer func() {
			errMsg := getObjectWithSignedUrlOutput.Body.Close()
			if errMsg != nil {
				ErrorLogger.WithContext(ctx).Warn(
					"close response body failed.")
			}
		}()
		_err := task.s3proxy.updateDownloadFile(
			ctx,
			task.tempFileURL,
			task.RangeStart,
			getObjectWithSignedUrlOutput)
		if nil != _err {
			if !task.enableCheckpoint {
				atomic.CompareAndSwapInt32(task.abort, 0, 1)
				ErrorLogger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					zap.String("objectKey", objectKey),
					zap.Int32("taskId", taskId),
					zap.Int64("partNumber", task.partNumber))
			}
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy.updateDownloadFile failed.",
				zap.String("objectKey", objectKey),
				zap.Int32("taskId", taskId),
				zap.Int64("partNumber", task.partNumber),
				zap.Error(_err))
			return _err
		}
		InfoLogger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.",
			zap.String("objectKey", objectKey),
			zap.Int32("taskId", taskId),
			zap.Int64("partNumber", task.partNumber))
		return getObjectWithSignedUrlOutput
	} else {
		var obsError obs.ObsError
		if errors.As(err, &obsError) &&
			obsError.StatusCode >= 400 &&
			obsError.StatusCode < 500 {

			atomic.CompareAndSwapInt32(task.abort, 0, 1)
			ErrorLogger.WithContext(ctx).Warn(
				"4** error abort task.",
				zap.String("objectKey", objectKey),
				zap.Int32("taskId", taskId),
				zap.Int64("partNumber", task.partNumber))
		}
	}

	ErrorLogger.WithContext(ctx).Error(
		"DownloadPartTask:Run failed.",
		zap.String("objectKey", objectKey),
		zap.Int32("taskId", taskId),
		zap.Int64("partNumber", task.partNumber),
		zap.Error(err))
	return err
}
