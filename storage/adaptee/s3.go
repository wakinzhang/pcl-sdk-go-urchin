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
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/client"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
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
)

var errAbort = errors.New("AbortError")

type S3 struct {
	obsClient *obs.ObsClient
}

func (o *S3) Init(ctx context.Context) (err error) {
	Logger.WithContext(ctx).Debug(
		"S3:Init start.")

	o.obsClient, err = obs.New("", "", "magicalParam")
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

func (o *S3) NewFolderWithSignedUrl(
	ctx context.Context,
	objectKey string,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:NewFolderWithSignedUrl start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId)

	createEmptyFileSignedUrlReq := new(CreatePutObjectSignedUrlReq)
	createEmptyFileSignedUrlReq.TaskId = taskId
	if 0 < len(objectKey) {
		createEmptyFileSignedUrlReq.Source = &objectKey
	}

	err, createEmptyFileSignedUrlResp :=
		UClient.CreatePutObjectSignedUrl(
			ctx,
			createEmptyFileSignedUrlReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreatePutObjectSignedUrl failed.",
			" objectKey: ", objectKey,
			" err: ", err)
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
		if obsError, ok := err.(obs.ObsError); ok {
			Logger.WithContext(ctx).Error(
				"obsClient.PutObjectWithSignedUrl failed.",
				" obsCode: ", obsError.Code,
				" obsMessage: ", obsError.Message)
			return err
		} else {
			Logger.WithContext(ctx).Error(
				"obsClient.PutObjectWithSignedUrl failed.",
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"S3:NewFolderWithSignedUrl finish.")
	return err
}

func (o *S3) PutObjectWithSignedUrl(
	ctx context.Context,
	sourceFile,
	objectKey string,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:PutObjectWithSignedUrl start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId)

	createEmptyFileSignedUrlReq := new(CreatePutObjectSignedUrlReq)
	createEmptyFileSignedUrlReq.TaskId = taskId
	createEmptyFileSignedUrlReq.Source = &objectKey

	err, createPutObjectSignedUrlResp :=
		UClient.CreatePutObjectSignedUrl(
			ctx,
			createEmptyFileSignedUrlReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreatePutObjectSignedUrl failed.",
			" objectKey: ", objectKey,
			" err: ", err)
		return err
	}
	var putObjectWithSignedUrlHeader = http.Header{}
	for key, item := range createPutObjectSignedUrlResp.Header {
		for _, value := range item.Values {
			putObjectWithSignedUrlHeader.Set(key, value)
		}
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
		if errMsg != nil {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" sourceFile: ", sourceFile,
				" err: ", errMsg)
		}
	}()

	_, err = o.obsClient.PutObjectWithSignedUrl(
		createPutObjectSignedUrlResp.SignedUrl,
		putObjectWithSignedUrlHeader,
		fd)
	if nil != err {
		if obsError, ok := err.(obs.ObsError); ok {
			Logger.WithContext(ctx).Error(
				"obsClient.PutObjectWithSignedUrl failed.",
				" obsCode: ", obsError.Code,
				" obsMessage: ", obsError.Message)
			return err
		} else {
			Logger.WithContext(ctx).Error(
				"obsClient.PutObjectWithSignedUrl failed.",
				" err: ", err)
			return err
		}
	}
	Logger.WithContext(ctx).Debug(
		"S3:PutObjectWithSignedUrl finish.")
	return err
}

func (o *S3) InitiateMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectKey string,
	taskId int32) (output *obs.InitiateMultipartUploadOutput, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:InitiateMultipartUploadWithSignedUrl start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId)

	createInitiateMultipartUploadSignedUrlReq :=
		new(CreateInitiateMultipartUploadSignedUrlReq)
	createInitiateMultipartUploadSignedUrlReq.TaskId = taskId
	createInitiateMultipartUploadSignedUrlReq.Source = objectKey

	err, createInitiateMultipartUploadSignedUrlResp :=
		UClient.CreateInitiateMultipartUploadSignedUrl(
			ctx,
			createInitiateMultipartUploadSignedUrlReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateInitiateMultipartUploadSignedUrl"+
				" failed.",
			" err: ", err)
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
		if obsError, ok := err.(obs.ObsError); ok {
			Logger.WithContext(ctx).Error(
				"obsClient.InitiateMultipartUploadWithSignedUrl failed.",
				" obsCode: ", obsError.Code,
				" obsMessage: ", obsError.Message)
			return output, err
		} else {
			Logger.WithContext(ctx).Error(
				"obsClient.InitiateMultipartUploadWithSignedUrl failed.",
				" err: ", err)
			return output, err
		}
	}
	Logger.WithContext(ctx).Debug(
		"S3:InitiateMultipartUploadWithSignedUrl finish.")

	return output, err
}

func (o *S3) UploadPartWithSignedUrl(
	ctx context.Context,
	sourceFile,
	objectKey,
	uploadId string,
	taskId int32) (partSlice PartSlice, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:UploadPartWithSignedUrl start.",
		" sourceFile: ", sourceFile,
		" objectKey: ", objectKey,
		" uploadId: ", uploadId,
		" taskId: ", taskId)

	var partSize int64 = DefaultPartSize
	stat, err := os.Stat(sourceFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return partSlice, err
	}
	fileSize := stat.Size()

	// 计算需要上传的段数
	partCount := int(fileSize / partSize)

	if fileSize%partSize != 0 {
		partCount++
	}

	// 执行并发上传段
	partChan := make(chan XPart, partCount)

	pool, err := ants.NewPool(DefaultS3UploadMultiTaskNum)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ants.NewPool for upload part failed.",
			" err: ", err)
		return partSlice, err
	}
	defer pool.Release()

	var isGlobalSuccess = true
	for i := 0; i < partCount; i++ {
		partNumber := i + 1
		offset := int64(i) * partSize
		currPartSize := partSize
		if i+1 == partCount {
			currPartSize = fileSize - offset
		}
		err = pool.Submit(func() {
			fd, _err := os.Open(sourceFile)
			if nil != _err {
				err = _err
				panic(err)
			}
			defer func() {
				errMsg := fd.Close()
				if errMsg != nil {
					Logger.WithContext(ctx).Warn(
						"Failed to close file with reason: ", errMsg)
				}
			}()

			readerWrapper := new(ReaderWrapper)
			readerWrapper.Reader = fd

			if offset < 0 || offset > fileSize {
				offset = 0
			}

			if currPartSize <= 0 || currPartSize > (fileSize-offset) {
				currPartSize = fileSize - offset
			}
			readerWrapper.TotalCount = currPartSize
			readerWrapper.Mark = offset
			if _, err = fd.Seek(offset, io.SeekStart); nil != err {
				return
			}

			createUploadPartSignedUrlReq := new(CreateUploadPartSignedUrlReq)
			createUploadPartSignedUrlReq.UploadId = uploadId
			createUploadPartSignedUrlReq.PartNumber = int32(partNumber)
			createUploadPartSignedUrlReq.TaskId = taskId
			createUploadPartSignedUrlReq.Source = objectKey
			err, createUploadPartSignedUrlResp :=
				UClient.CreateUploadPartSignedUrl(
					ctx,
					createUploadPartSignedUrlReq)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"CreateUploadPartSignedUrl failed.",
					" err: ", err)
				isGlobalSuccess = false
				partChan <- XPart{
					PartNumber: partNumber}
			}
			var uploadPartWithSignedUrlHeader = http.Header{}
			for key, item := range createUploadPartSignedUrlResp.Header {
				for _, value := range item.Values {
					uploadPartWithSignedUrlHeader.Set(key, value)
				}
			}

			uploadPartWithSignedUrlHeader.Set("Content-Length",
				strconv.FormatInt(currPartSize, 10))

			uploadPartOutput, err := o.obsClient.UploadPartWithSignedUrl(
				createUploadPartSignedUrlResp.SignedUrl,
				uploadPartWithSignedUrlHeader,
				readerWrapper)

			if nil != err {
				if obsError, ok := err.(obs.ObsError); ok {
					Logger.WithContext(ctx).Error(
						"obsClient.UploadPartWithSignedUrl failed.",
						" signedUrl: ", createUploadPartSignedUrlResp.SignedUrl,
						" sourceFile: ", sourceFile,
						" objectKey: ", objectKey,
						" partNumber: ", partNumber,
						" offset: ", offset,
						" currPartSize: ", currPartSize,
						" obsCode: ", obsError.Code,
						" obsMessage: ", obsError.Message)
				} else {
					Logger.WithContext(ctx).Error(
						"obsClient.UploadPartWithSignedUrl failed.",
						" signedUrl: ", createUploadPartSignedUrlResp.SignedUrl,
						" sourceFile: ", sourceFile,
						" objectKey: ", objectKey,
						" partNumber: ", partNumber,
						" offset: ", offset,
						" currPartSize: ", currPartSize,
						" err: ", err)
				}
				isGlobalSuccess = false
				partChan <- XPart{
					PartNumber: partNumber}
			}
			Logger.WithContext(ctx).Info(
				"obsClient.UploadPartWithSignedUrl success.",
				" signedUrl: ", createUploadPartSignedUrlResp.SignedUrl,
				" sourceFile: ", sourceFile,
				" objectKey: ", objectKey,
				" partNumber: ", partNumber,
				" offset: ", offset,
				" currPartSize: ", currPartSize,
				" ETag: ", strings.Trim(uploadPartOutput.ETag, "\""))
			partChan <- XPart{
				ETag:       strings.Trim(uploadPartOutput.ETag, "\""),
				PartNumber: partNumber}
		})
		if nil != err {
			Logger.WithContext(ctx).Error(
				"ants.Submit for upload part failed.",
				" err: ", err)
			return partSlice, err
		}
	}

	parts := make([]XPart, 0, partCount)
	// 等待上传完成
	for {
		part, ok := <-partChan
		if !ok {
			break
		}
		parts = append(parts, part)

		if len(parts) == partCount {
			close(partChan)
		}
	}
	partSlice = parts
	sort.Sort(partSlice)

	if !isGlobalSuccess {
		Logger.WithContext(ctx).Error(
			"S3:uploadPartWithSignedUrl some part failed.",
			" sourceFile: ", sourceFile,
			" objectKey: ", objectKey,
			" uploadId: ", uploadId)
		return partSlice, errors.New("some part upload failed")
	}

	Logger.WithContext(ctx).Debug(
		"S3:UploadPartWithSignedUrl finish.")
	return partSlice, nil
}

func (o *S3) CompleteMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectKey,
	uploadId string,
	taskId int32,
	partSlice PartSlice) (
	output *obs.CompleteMultipartUploadOutput,
	err error) {

	Logger.WithContext(ctx).Debug(
		"S3:CompleteMultipartUploadWithSignedUrl start.",
		" objectKey: ", objectKey,
		" uploadId: ", uploadId,
		" taskId: ", taskId)

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
		Logger.WithContext(ctx).Error(
			"UrchinClient:CreateCompleteMultipartUploadSignedUrl"+
				" failed.",
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"xml.MarshalIndent failed.",
			" err: ", err)
		return output, err
	}
	Logger.WithContext(ctx).Debug(
		"completeMultipartUploadPartXML content: ",
		string(completeMultipartUploadPartXML))
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
		if obsError, ok := err.(obs.ObsError); ok {
			Logger.WithContext(ctx).Error(
				"obsClient.CompleteMultipartUploadWithSignedUrl failed.",
				" obsCode: ", obsError.Code,
				" obsMessage: ", obsError.Message)
			return output, err
		} else {
			Logger.WithContext(ctx).Error(
				"obsClient.CompleteMultipartUploadWithSignedUrl failed.",
				" err: ", err)
			return output, err
		}
	}

	Logger.WithContext(ctx).Debug(
		"obsClient.CompleteMultipartUploadWithSignedUrl finish.")
	return output, nil
}

func (o *S3) AbortMultipartUploadWithSignedUrl(
	ctx context.Context,
	objectKey,
	uploadId string,
	taskId int32) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:AbortMultipartUploadWithSignedUrl start.",
		" objectKey: ", objectKey,
		" uploadId: ", uploadId,
		" taskId: ", taskId)

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
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateAbortMultipartUploadSignedUrl failed.",
			" err: ", err)
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
		if obsError, ok := err.(obs.ObsError); ok {
			Logger.WithContext(ctx).Error(
				"obsClient.AbortMultipartUploadWithSignedUrl failed.",
				" obsCode: ", obsError.Code,
				" obsMessage: ", obsError.Message)
			return err
		} else {
			Logger.WithContext(ctx).Error(
				"obsClient.AbortMultipartUploadWithSignedUrl failed.",
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"S3.AbortMultipartUploadWithSignedUrl finish.")
	return
}

func (o *S3) GetObjectInfoWithSignedUrl(
	ctx context.Context,
	objectKey string,
	taskId int32) (
	getObjectMetaOutput *obs.GetObjectMetadataOutput, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:GetObjectInfoWithSignedUrl start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId)

	createGetObjectMetadataSignedUrlReq :=
		new(CreateGetObjectMetadataSignedUrlReq)
	createGetObjectMetadataSignedUrlReq.TaskId = taskId
	createGetObjectMetadataSignedUrlReq.Source = objectKey

	err, createGetObjectMetadataSignedUrlResp :=
		UClient.CreateGetObjectMetadataSignedUrl(
			ctx,
			createGetObjectMetadataSignedUrlReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateGetObjectMetadataSignedUrl failed.",
			" err: ", err)
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
		if obsError, ok := err.(obs.ObsError); ok {
			Logger.WithContext(ctx).Error(
				"obsClient.GetObjectMetadataWithSignedUrl failed.",
				" signedUrl: ", createGetObjectMetadataSignedUrlResp.SignedUrl,
				" obsCode: ", obsError.Code,
				" obsMessage: ", obsError.Message)
			return
		} else {
			Logger.WithContext(ctx).Error(
				"obsClient.GetObjectMetadataWithSignedUrl failed.",
				" signedUrl: ", createGetObjectMetadataSignedUrlResp.SignedUrl,
				" err: ", err)
			return
		}
	}
	Logger.WithContext(ctx).Debug(
		"S3.GetObjectInfoWithSignedUrl success.")
	return
}

func (o *S3) ListObjectsWithSignedUrl(
	ctx context.Context,
	taskId int32,
	marker string) (listObjectsOutput *obs.ListObjectsOutput, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:ListObjectsWithSignedUrl start.",
		" taskId: ", taskId,
		" marker: ", marker)

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
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateListObjectsSignedUrl failed.",
			" err: ", err)
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
		if obsError, ok := err.(obs.ObsError); ok {
			Logger.WithContext(ctx).Error(
				"obsClient.ListObjectsWithSignedUrl failed.",
				" obsCode: ", obsError.Code,
				" obsMessage: ", obsError.Message)
			return listObjectsOutput, err
		} else {
			Logger.WithContext(ctx).Error(
				"obsClient.ListObjectsWithSignedUrl failed.",
				" err: ", err)
			return listObjectsOutput, err
		}
	}
	Logger.WithContext(ctx).Debug(
		"S3:ListObjectsWithSignedUrl finish.")
	return listObjectsOutput, nil
}

func (o *S3) loadCheckpointFile(
	ctx context.Context,
	checkpointFile string,
	result interface{}) error {

	Logger.WithContext(ctx).Debug(
		"S3:loadCheckpointFile start.",
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
		"S3:loadCheckpointFile finish.")
	return xml.Unmarshal(ret, result)
}

func (o *S3) sliceObject(
	ctx context.Context,
	objectSize, partSize int64,
	dfc *DownloadCheckpoint) {

	Logger.WithContext(ctx).Debug(
		"S3:sliceObject start.",
		" objectSize: ", objectSize,
		" partSize: ", partSize)

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
	Logger.WithContext(ctx).Debug(
		"S3:sliceObject finish.")
}

func (o *S3) updateCheckpointFile(
	ctx context.Context,
	fc interface{},
	checkpointFilePath string) error {

	Logger.WithContext(ctx).Debug(
		"S3:updateCheckpointFile start.",
		" checkpointFilePath: ", checkpointFilePath)

	result, err := xml.Marshal(fc)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"xml.Marshal failed.",
			" checkpointFilePath: ", checkpointFilePath, " err: ", err)
		return err
	}
	err = os.WriteFile(checkpointFilePath, result, 0640)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.WriteFile failed.",
			" checkpointFilePath: ", checkpointFilePath, " err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"updateCheckpointFile finish.")
	return err
}

func (o *S3) Upload(
	ctx context.Context,
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:Upload start.",
		" sourcePath: ", sourcePath,
		" taskId: ", taskId,
		" needPure: ", needPure)

	stat, err := os.Stat(sourcePath)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourcePath: ", sourcePath,
			" err: ", err)
		return err
	}
	var isDir = false
	if stat.IsDir() {
		isDir = true
		err = o.uploadFolder(ctx, sourcePath, taskId, needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3.uploadFolder failed.",
				" sourcePath: ", sourcePath,
				" taskId: ", taskId,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"S3.uploadFile failed.",
				" sourcePath: ", sourcePath,
				" objectKey: ", objectKey,
				" taskId: ", taskId,
				" needPure: ", needPure,
				" err: ", err)
			return err
		}
	}

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			" err: ", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		Logger.WithContext(ctx).Error(
			"task not exist. taskId: ", taskId)
		return errors.New("task not exist")
	}

	task := getTaskResp.Data.List[0].Task
	if TaskTypeMigrate == task.Type {
		migrateObjectTaskParams := new(MigrateObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), migrateObjectTaskParams)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"MigrateObjectTaskParams Unmarshal failed.",
				" params: ", task.Params,
				" err: ", err)
			return err
		}
		objUuid := migrateObjectTaskParams.Request.ObjUuid
		nodeName := migrateObjectTaskParams.Request.TargetNodeName
		var location string
		if isDir {
			location = objUuid + "/" + filepath.Base(sourcePath) + "/"
		} else {
			location = objUuid + "/" + filepath.Base(sourcePath)
		}

		putObjectDeploymentReq := new(PutObjectDeploymentReq)
		putObjectDeploymentReq.ObjUuid = objUuid
		putObjectDeploymentReq.NodeName = nodeName
		putObjectDeploymentReq.Location = &location

		err, _ = UClient.PutObjectDeployment(ctx, putObjectDeploymentReq)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"UrchinClient.PutObjectDeployment failed.",
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
	sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:uploadFolder start.",
		" sourcePath: ", sourcePath,
		" taskId: ", taskId,
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
				" uploadFolderRecord: ", uploadFolderRecord, " err: ", err)
			return err
		}
	}

	err = o.NewFolderWithSignedUrl(ctx, "", taskId)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:NewFolderWithSignedUrl failed.",
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
			wg.Add(1)
			err = pool.Submit(func() {
				defer func() {
					wg.Done()
					if err := recover(); nil != err {
						Logger.WithContext(ctx).Error(
							"S3:uploadFileResume failed.",
							" err: ", err)
						isAllSuccess = false
					}
				}()
				objectKey, err := filepath.Rel(sourcePath, filePath)
				if nil != err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"filepath.Rel failed.",
						" sourcePath: ", sourcePath,
						" filePath: ", filePath,
						" objectKey: ", objectKey,
						" err: ", err)
					return
				}
				if _, exists := fileMap[objectKey]; exists {
					Logger.WithContext(ctx).Info(
						"already finish. objectKey: ", objectKey)
					return
				}
				if fileInfo.IsDir() {
					err = o.NewFolderWithSignedUrl(
						ctx,
						objectKey,
						taskId)
					if nil != err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"S3:NewFolderWithSignedUrl failed.",
							" objectKey: ", objectKey,
							" err: ", err)
						return
					}
				} else {
					err = o.uploadFile(
						ctx,
						filePath,
						objectKey,
						taskId,
						needPure)
					if nil != err {
						isAllSuccess = false
						Logger.WithContext(ctx).Error(
							"S3:uploadFile failed.",
							" filePath: ", filePath,
							" objectKey: ", objectKey,
							" taskId: ", taskId,
							" needPure: ", needPure,
							" err: ", err)
						return
					}
				}
				fileMutex.Lock()
				defer fileMutex.Unlock()
				f, err := os.OpenFile(
					uploadFolderRecord,
					os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				if nil != err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"os.OpenFile failed.",
						" uploadFolderRecord: ", uploadFolderRecord,
						" err: ", err)
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
				_, err = f.Write([]byte(objectKey + "\n"))
				if nil != err {
					isAllSuccess = false
					Logger.WithContext(ctx).Error(
						"write file failed.",
						" uploadFolderRecord: ", uploadFolderRecord,
						" objectKey: ", objectKey,
						" err: ", err)
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
	taskId int32,
	needPure bool) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:uploadFile start.",
		" sourceFile: ", sourceFile,
		" objectKey: ", objectKey,
		" taskId: ", taskId,
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
		_, err = o.uploadFileResume(
			ctx,
			sourceFile,
			objectKey,
			taskId,
			needPure)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3:uploadFileResume failed.",
				" sourceFile: ", sourceFile,
				" objectKey: ", objectKey,
				" taskId: ", taskId,
				" needPure: ", needPure,
				" err: ", err)
			return err
		}
	} else {
		err = o.PutObjectWithSignedUrl(
			ctx,
			sourceFile,
			objectKey,
			taskId)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3:PutObjectWithSignedUrl failed.",
				" sourceFile: ", sourceFile,
				" objectKey: ", objectKey,
				" taskId: ", taskId,
				" err: ", err)
			return err
		}
	}

	Logger.WithContext(ctx).Debug(
		"S3:uploadFile finish.")
	return err
}

func (o *S3) uploadFileResume(
	ctx context.Context,
	sourceFile,
	objectKey string,
	taskId int32,
	needPure bool) (
	output *obs.CompleteMultipartUploadOutput, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:uploadFileResume start.",
		" sourceFile: ", sourceFile,
		" objectKey: ", objectKey,
		" taskId: ", taskId,
		" needPure: ", needPure)

	uploadFileInput := new(obs.UploadFileInput)
	uploadFileInput.UploadFile = sourceFile
	uploadFileInput.EnableCheckpoint = true
	uploadFileInput.CheckpointFile =
		uploadFileInput.UploadFile + ".upload_file_record"
	uploadFileInput.TaskNum = DefaultS3UploadMultiTaskNum
	uploadFileInput.PartSize = DefaultPartSize
	uploadFileInput.Key = objectKey

	if needPure {
		err = os.Remove(uploadFileInput.CheckpointFile)
		if nil != err {
			if !os.IsNotExist(err) {
				Logger.WithContext(ctx).Error(
					"os.Remove failed.",
					" CheckpointFile: ", uploadFileInput.CheckpointFile,
					" err: ", err)
				return output, err
			}
		}
	}

	if uploadFileInput.PartSize < obs.MIN_PART_SIZE {
		uploadFileInput.PartSize = obs.MIN_PART_SIZE
	} else if uploadFileInput.PartSize > obs.MAX_PART_SIZE {
		uploadFileInput.PartSize = obs.MAX_PART_SIZE
	}

	output, err = o.resumeUpload(
		ctx,
		sourceFile,
		objectKey,
		taskId,
		uploadFileInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:resumeUpload failed.",
			" sourceFile: ", sourceFile,
			" objectKey: ", objectKey,
			" taskId: ", taskId,
			" err: ", err)
		return output, err
	}

	Logger.WithContext(ctx).Debug(
		"S3:uploadFileResume finish.")
	return output, err
}

func (o *S3) resumeUpload(
	ctx context.Context,
	sourceFile, objectKey string,
	taskId int32,
	input *obs.UploadFileInput) (
	output *obs.CompleteMultipartUploadOutput, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:resumeUpload start.",
		" sourceFile: ", sourceFile,
		" objectKey: ", objectKey,
		" taskId: ", taskId)

	uploadFileStat, err := os.Stat(input.UploadFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" uploadFile: ", input.UploadFile,
			" err: ", err)
		return nil, err
	}
	if uploadFileStat.IsDir() {
		Logger.WithContext(ctx).Error(
			"uploadFile can not be a folder.",
			" uploadFile: ", input.UploadFile)
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
			Logger.WithContext(ctx).Error(
				"S3:getUploadCheckpointFile failed.",
				" objectKey: ", objectKey,
				" taskId: ", taskId,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"S3:prepareUpload failed.",
				" objectKey: ", objectKey,
				" taskId: ", taskId,
				" err: ", err)
			return nil, err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"S3:updateCheckpointFile failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" err: ", err)
				_err := o.AbortMultipartUploadWithSignedUrl(
					ctx,
					objectKey,
					ufc.UploadId,
					taskId)
				if nil != _err {
					Logger.WithContext(ctx).Error(
						"S3:AbortMultipartUploadWithSignedUrl failed.",
						" objectKey: ", objectKey,
						" uploadId: ", ufc.UploadId,
						" taskId: ", taskId,
						" err: ", _err)
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
		Logger.WithContext(ctx).Error(
			"S3:handleUploadFileResult failed.",
			" objectKey: ", objectKey,
			" taskId: ", taskId,
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"S3:completeParts failed.",
			" objectKey: ", objectKey,
			" taskId: ", taskId,
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return completeOutput, err
	}

	Logger.WithContext(ctx).Debug(
		"S3:resumeUpload finish.")
	return completeOutput, err
}

func (o *S3) uploadPartConcurrent(
	ctx context.Context,
	sourceFile string,
	taskId int32,
	ufc *UploadCheckpoint,
	input *obs.UploadFileInput) error {

	Logger.WithContext(ctx).Debug(
		"S3:uploadPartConcurrent start.",
		" sourceFile: ", sourceFile,
		" taskId: ", taskId)

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
			obsClient:        o.obsClient,
			abort:            &abort,
			enableCheckpoint: input.EnableCheckpoint,
		}
		wg.Add(1)
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
			}()
			result := task.Run(ctx, sourceFile, ufc.UploadId, taskId)
			err = o.handleUploadTaskResult(
				ctx,
				result,
				ufc,
				task.PartNumber,
				input.EnableCheckpoint,
				input.CheckpointFile,
				lock)
			if nil != err &&
				atomic.CompareAndSwapInt32(&errFlag, 0, 1) {

				Logger.WithContext(ctx).Error(
					"S3:handleUploadTaskResult failed.",
					" partNumber: ", task.PartNumber,
					" checkpointFile: ", input.CheckpointFile,
					" err: ", err)
				uploadPartError.Store(err)
			}
			Logger.WithContext(ctx).Debug(
				"S3:handleUploadTaskResult finish.")
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
		"S3:uploadPartConcurrent finish.")
	return nil
}

func (o *S3) getUploadCheckpointFile(
	ctx context.Context,
	objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *obs.UploadFileInput) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:getUploadCheckpointFile start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId)

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
			"S3:loadCheckpointFile failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return true, nil
	} else if !ufc.isValid(ctx, input.Key, input.UploadFile, uploadFileStat) {
		if ufc.Key != "" && ufc.UploadId != "" {
			_err := o.AbortMultipartUploadWithSignedUrl(
				ctx,
				objectKey,
				ufc.UploadId,
				taskId)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"S3:AbortMultipartUploadWithSignedUrl failed.",
					" objectKey: ", objectKey,
					" uploadId: ", ufc.UploadId,
					" taskId: ", taskId,
					" err: ", _err)
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
			"S3:loadCheckpointFile finish.",
			" checkpointFilePath: ", checkpointFilePath)
		return false, nil
	}
	Logger.WithContext(ctx).Debug(
		"S3:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *S3) prepareUpload(
	ctx context.Context,
	objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *obs.UploadFileInput) error {

	Logger.WithContext(ctx).Debug(
		"S3:prepareUpload start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId)

	initiateMultipartUploadOutput, err :=
		o.InitiateMultipartUploadWithSignedUrl(
			ctx,
			objectKey,
			taskId)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:InitiateMultipartUploadWithSignedUrl failed.",
			" objectKey: ", objectKey,
			" taskId: ", taskId,
			" err: ", err)
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
		Logger.WithContext(ctx).Error(
			"S3:sliceFile failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"S3:prepareUpload finish.")
	return err
}

func (o *S3) sliceFile(
	ctx context.Context,
	partSize int64,
	ufc *UploadCheckpoint) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:sliceFile start.",
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

	if partSize > obs.MAX_PART_SIZE {
		Logger.WithContext(ctx).Error(
			"upload file part too large.",
			" partSize: ", partSize,
			" maxPartSize: ", obs.MAX_PART_SIZE)

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
	Logger.WithContext(ctx).Debug(
		"S3:sliceFile finish.")
	return nil
}

func (o *S3) handleUploadTaskResult(
	ctx context.Context,
	result interface{},
	ufc *UploadCheckpoint,
	partNum int,
	enableCheckpoint bool,
	checkpointFilePath string,
	lock *sync.Mutex) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:handleUploadTaskResult start.",
		" checkpointFilePath: ", checkpointFilePath,
		" partNum: ", partNum)

	if uploadPartOutput, ok := result.(*obs.UploadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].Etag = uploadPartOutput.ETag
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, ufc, checkpointFilePath)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"S3:updateCheckpointFile failed.",
					" checkpointFilePath: ", checkpointFilePath,
					" partNum: ", partNum,
					" err: ", _err)
			}
		}
	} else if result != errAbort {
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
		"S3:handleUploadTaskResult finish.")
	return
}

func (o *S3) handleUploadFileResult(
	ctx context.Context,
	objectKey string,
	taskId int32,
	uploadPartError error,
	ufc *UploadCheckpoint,
	enableCheckpoint bool) error {

	Logger.WithContext(ctx).Debug(
		"S3:handleUploadFileResult start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId,
		" uploadId: ", ufc.UploadId)

	if uploadPartError != nil {
		Logger.WithContext(ctx).Debug(
			"uploadPartError not nil.",
			" uploadPartError: ", uploadPartError)
		if enableCheckpoint {
			Logger.WithContext(ctx).Debug(
				"enableCheckpoint return uploadPartError.")
			return uploadPartError
		}
		_err := o.AbortMultipartUploadWithSignedUrl(
			ctx,
			objectKey,
			ufc.UploadId,
			taskId)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"S3:AbortMultipartUploadWithSignedUrl start.",
				" objectKey: ", objectKey,
				" taskId: ", taskId,
				" uploadId: ", ufc.UploadId,
				" err: ", _err)
		}
		return uploadPartError
	}
	Logger.WithContext(ctx).Debug(
		"S3:handleUploadFileResult finish.")
	return nil
}

func (o *S3) completeParts(
	ctx context.Context,
	objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	enableCheckpoint bool,
	checkpointFilePath string) (
	output *obs.CompleteMultipartUploadOutput, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:completeParts start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId,
		" uploadId: ", ufc.UploadId,
		" checkpointFilePath: ", checkpointFilePath)

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
					Logger.WithContext(ctx).Error(
						"os.Remove failed.",
						" checkpointFilePath: ", checkpointFilePath,
						" err: ", _err)
				}
			}
		}
		Logger.WithContext(ctx).Debug(
			"S3:completeParts finish.")
		return output, err
	}
	if !enableCheckpoint {
		_err := o.AbortMultipartUploadWithSignedUrl(
			ctx,
			objectKey,
			ufc.UploadId,
			taskId)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"S3.AbortMultipartUploadWithSignedUrl failed.",
				" objectKey: ", objectKey,
				" uploadId: ", ufc.UploadId,
				" taskId: ", taskId,
				" err: ", _err)
		}
	}
	Logger.WithContext(ctx).Error(
		"S3.CompleteMultipartUploadWithSignedUrl failed.",
		" objectKey: ", objectKey,
		" uploadId: ", ufc.UploadId,
		" taskId: ", taskId,
		" err: ", err)
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

	Logger.WithContext(ctx).Debug(
		"UploadCheckpoint:isValid start.",
		" key: ", key,
		" ufc.Key: ", ufc.Key,
		" uploadFile: ", uploadFile,
		" ufc.UploadFile: ", ufc.UploadFile,
		" fileStat.Size: ", fileStat.Size(),
		" ufc.FileInfo.Size: ", ufc.FileInfo.Size,
		" uploadId: ", ufc.UploadId)

	if ufc.Key != key ||
		ufc.UploadFile != uploadFile {
		Logger.WithContext(ctx).Error(
			"Checkpoint file is invalid.",
			" bucketName or objectKey or uploadFile was changed.")
		return false
	}
	if ufc.FileInfo.Size != fileStat.Size() ||
		ufc.FileInfo.LastModified != fileStat.ModTime().Unix() {
		Logger.WithContext(ctx).Error(
			"Checkpoint file is invalid.",
			" uploadFile was changed.")
		return false
	}
	if ufc.UploadId == "" {
		Logger.WithContext(ctx).Error(
			"UploadId is invalid.")
		return false
	}
	Logger.WithContext(ctx).Debug(
		"UploadCheckpoint:isValid finish.")
	return true
}

type UploadPartTask struct {
	obs.UploadPartInput
	obsClient        *obs.ObsClient
	abort            *int32
	enableCheckpoint bool
}

func (task *UploadPartTask) Run(
	ctx context.Context,
	sourceFile, uploadId string,
	taskId int32) interface{} {

	Logger.WithContext(ctx).Debug(
		"UploadPartTask:Run start.",
		" sourceFile: ", sourceFile,
		" uploadId: ", uploadId,
		" taskId: ", taskId)

	if atomic.LoadInt32(task.abort) == 1 {
		Logger.WithContext(ctx).Error(
			"task abort.")
		return errAbort
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

	createUploadPartSignedUrlReq := new(CreateUploadPartSignedUrlReq)
	createUploadPartSignedUrlReq.UploadId = uploadId
	createUploadPartSignedUrlReq.PartNumber = int32(task.PartNumber)
	createUploadPartSignedUrlReq.TaskId = taskId
	createUploadPartSignedUrlReq.Source = task.Key
	err, createUploadPartSignedUrlResp :=
		UClient.CreateUploadPartSignedUrl(
			ctx,
			createUploadPartSignedUrlReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateUploadPartSignedUrl failed.",
			" uploadId: ", uploadId,
			" partNumber: ", int32(task.PartNumber),
			" taskId: ", taskId,
			" source: ", task.Key,
			" err: ", err)
		return err
	}
	var uploadPartWithSignedUrlHeader = http.Header{}
	for key, item := range createUploadPartSignedUrlResp.Header {
		for _, value := range item.Values {
			uploadPartWithSignedUrlHeader.Set(key, value)
		}
	}

	uploadPartWithSignedUrlHeader.Set("Content-Length",
		strconv.FormatInt(task.PartSize, 10))

	uploadPartOutput, err := task.obsClient.UploadPartWithSignedUrl(
		createUploadPartSignedUrlResp.SignedUrl,
		uploadPartWithSignedUrlHeader,
		readerWrapper)

	if nil == err {
		if uploadPartOutput.ETag == "" {
			Logger.WithContext(ctx).Error(
				"obsClient.UploadPartWithSignedUrl failed.",
				" invalid etag value.",
				" uploadId: ", uploadId,
				" partNumber: ", int32(task.PartNumber),
				" taskId: ", taskId,
				" source: ", task.Key)
			if !task.enableCheckpoint {
				atomic.CompareAndSwapInt32(task.abort, 0, 1)
				Logger.WithContext(ctx).Error(
					"obsClient.UploadPartWithSignedUrl failed.",
					" invalid etag value.",
					" aborted task.",
					" uploadId: ", uploadId,
					" partNumber: ", int32(task.PartNumber),
					" taskId: ", taskId,
					" source: ", task.Key)
			}
			return errors.New("invalid etag value")
		}
		Logger.WithContext(ctx).Debug(
			"UploadPartTask:Run finish.")
		return uploadPartOutput
	} else if obsError, ok := err.(obs.ObsError); ok &&
		obsError.StatusCode >= 400 && obsError.StatusCode < 500 {

		atomic.CompareAndSwapInt32(task.abort, 0, 1)
		Logger.WithContext(ctx).Error(
			"obsClient.UploadPartWithSignedUrl failed.",
			" uploadId: ", uploadId,
			" partNumber: ", int32(task.PartNumber),
			" taskId: ", taskId,
			" source: ", task.Key,
			" obsCode: ", obsError.Code,
			" obsMessage: ", obsError.Message)
	}

	Logger.WithContext(ctx).Error(
		"obsClient.UploadPartWithSignedUrl failed.",
		" uploadId: ", uploadId,
		" partNumber: ", int32(task.PartNumber),
		" taskId: ", taskId,
		" source: ", task.Key,
		" err: ", err)
	return err
}

func (o *S3) Download(
	ctx context.Context,
	targetPath string,
	taskId int32,
	bucketName string) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:Download start.",
		" targetPath: ", targetPath,
		" taskId: ", taskId,
		" bucketName: ", bucketName)

	marker := ""
	for {
		listObjectsOutput, err := o.ListObjectsWithSignedUrl(
			ctx,
			taskId,
			marker)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3:ListObjectsWithSignedUrl start.",
				" taskId: ", taskId,
				" err: ", err)
			return err
		}
		err = o.downloadObjects(
			ctx,
			targetPath,
			taskId,
			bucketName,
			listObjectsOutput)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3:downloadObjects failed.",
				" targetPath: ", targetPath,
				" taskId: ", taskId,
				" bucketName: ", bucketName,
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
	targetPath string,
	taskId int32,
	bucketName string,
	listObjectsOutput *obs.ListObjectsOutput) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:downloadObjects start.",
		" targetPath: ", targetPath,
		" taskId: ", taskId,
		" bucketName: ", bucketName)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := UClient.GetTask(ctx, getTaskReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.GetTask failed.",
			" taskId: ", taskId,
			" err: ", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		Logger.WithContext(ctx).Error(
			"task not exist. taskId: ", taskId)
		return errors.New("task not exist")
	}
	task := getTaskResp.Data.List[0].Task
	downloadObjectTaskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadObjectTaskParams)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"DownloadObjectTaskParams Unmarshal failed.",
			" taskId: ", taskId,
			" params: ", task.Params,
			" err: ", err)
		return err
	}

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	downloadFolderRecord :=
		targetPath +
			downloadObjectTaskParams.Request.ObjUuid +
			".download_folder_record"
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
		// 处理文件
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
				if err := recover(); nil != err {
					Logger.WithContext(ctx).Error(
						"downloadFile failed.",
						" err: ", err)
					isAllSuccess = false
				}
			}()
			_, err = o.downloadPartWithSignedUrl(
				ctx,
				bucketName,
				itemObject.Key,
				targetPath+itemObject.Key,
				taskId)
			if nil != err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"S3:downloadPartWithSignedUrl failed.",
					" bucketName: ", bucketName,
					" objectKey: ", itemObject.Key,
					" targetFile: ", targetPath+itemObject.Key,
					" taskId: ", taskId,
					" err: ", err)
				return
			}
			fileMutex.Lock()
			defer fileMutex.Unlock()
			f, err := os.OpenFile(
				downloadFolderRecord,
				os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if nil != err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"os.OpenFile failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" err: ", err)
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
			_, err = f.Write([]byte(itemObject.Key + "\n"))
			if nil != err {
				isAllSuccess = false
				Logger.WithContext(ctx).Error(
					"write file failed.",
					" downloadFolderRecord: ", downloadFolderRecord,
					" objectKey: ", itemObject.Key,
					" err: ", err)
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
			" uuid: ", downloadObjectTaskParams.Request.ObjUuid)

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

func (o *S3) downloadPartWithSignedUrl(
	ctx context.Context,
	bucketName, objectKey, targetFile string,
	taskId int32) (output *obs.GetObjectMetadataOutput, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:downloadPartWithSignedUrl start.",
		" bucketName: ", bucketName,
		" objectKey: ", objectKey,
		" targetFile: ", targetFile,
		" taskId: ", taskId)

	if '/' == objectKey[len(objectKey)-1] {
		parentDir := filepath.Dir(targetFile)
		stat, err := os.Stat(parentDir)
		if nil != err {
			if !os.IsNotExist(err) {
				Logger.WithContext(ctx).Error(
					"os.Stat failed.",
					" parentDir: ", parentDir,
					" err: ", err)
				return output, err
			}
			Logger.WithContext(ctx).Debug(
				"parentDir: ", parentDir, " not exist.")

			_err := os.MkdirAll(parentDir, os.ModePerm)
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"os.MkdirAll failed.",
					" parentDir: ", parentDir,
					" err: ", _err)
				return output, _err
			}
			return output, nil
		} else if !stat.IsDir() {
			Logger.WithContext(ctx).Error(
				"same file exists.",
				" parentDir: ", parentDir)
			return output,
				fmt.Errorf(
					"cannot create folder: %s same file exists",
					parentDir)
		} else {
			Logger.WithContext(ctx).Debug(
				"no need download.")
			return output, nil
		}
	}

	downloadFileInput := new(obs.DownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile =
		downloadFileInput.DownloadFile + ".download_file_record"
	downloadFileInput.TaskNum = DefaultS3DownloadMultiTaskNum
	downloadFileInput.PartSize = DefaultPartSize
	downloadFileInput.Bucket = bucketName
	downloadFileInput.Key = objectKey

	output, err = o.resumeDownload(
		ctx,
		objectKey,
		taskId,
		downloadFileInput)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:resumeDownload failed.",
			" objectKey: ", objectKey,
			" taskId: ", taskId,
			" err: ", err)
		return output, err
	}
	Logger.WithContext(ctx).Debug(
		"S3:downloadPartWithSignedUrl finish.")
	return
}

func (o *S3) resumeDownload(
	ctx context.Context,
	objectKey string,
	taskId int32,
	input *obs.DownloadFileInput) (
	output *obs.GetObjectMetadataOutput, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:resumeDownload start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId)

	getObjectMetaOutput, err := o.GetObjectInfoWithSignedUrl(
		ctx,
		objectKey,
		taskId)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"S3:GetObjectInfoWithSignedUrl failed.",
			" objectKey: ", objectKey,
			" taskId: ", taskId,
			" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"S3:getDownloadCheckpointFile failed.",
				" checkpointFilePath: ", checkpointFilePath,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"S3:prepareTempFile failed.",
				" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
				" Size: ", dfc.TempFileInfo.Size,
				" err: ", err)
			return nil, err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ctx, dfc, checkpointFilePath)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"S3:updateCheckpointFile failed.",
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
		Logger.WithContext(ctx).Error(
			"S3:handleDownloadFileResult failed.",
			" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
			" err: ", err)
		return nil, err
	}

	err = os.Rename(dfc.TempFileInfo.TempFileUrl, input.DownloadFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Rename failed.",
			" TempFileUrl: ", dfc.TempFileInfo.TempFileUrl,
			" DownloadFile: ", input.DownloadFile,
			" err: ", err)
		return nil, err
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
		"S3:resumeDownload finish.")
	return getObjectMetaOutput, nil
}

func (o *S3) downloadFileConcurrent(
	ctx context.Context,
	objectKey string,
	taskId int32,
	input *obs.DownloadFileInput,
	dfc *DownloadCheckpoint) error {

	Logger.WithContext(ctx).Debug(
		"S3:downloadFileConcurrent start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId)

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
			obsClient:        o.obsClient,
			s3:               o,
			abort:            &abort,
			partNumber:       downloadPart.PartNumber,
			tempFileURL:      dfc.TempFileInfo.TempFileUrl,
			enableCheckpoint: input.EnableCheckpoint,
		}
		Logger.WithContext(ctx).Debug(
			"DownloadPartTask params.",
			" rangeStart: ", downloadPart.Offset,
			" rangeEnd: ", downloadPart.RangeEnd,
			" partNumber: ", downloadPart.PartNumber,
			" tempFileURL: ", dfc.TempFileInfo.TempFileUrl,
			" enableCheckpoint: ", input.EnableCheckpoint)

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
					err := o.updateCheckpointFile(
						ctx,
						dfc,
						input.CheckpointFile)
					if nil != err {
						Logger.WithContext(ctx).Error(
							"S3:updateCheckpointFile failed.",
							" checkpointFile: ", input.CheckpointFile,
							" err: ", err)
						downloadPartError.Store(err)
					}
				}
				return
			} else {
				result := task.Run(ctx, objectKey, taskId)
				err := o.handleDownloadTaskResult(
					ctx,
					result,
					dfc,
					task.partNumber,
					input.EnableCheckpoint,
					input.CheckpointFile,
					lock)
				if nil != err &&
					atomic.CompareAndSwapInt32(&errFlag, 0, 1) {

					Logger.WithContext(ctx).Error(
						"S3:handleDownloadTaskResult failed.",
						" partNumber: ", task.partNumber,
						" checkpointFile: ", input.CheckpointFile,
						" err: ", err)
					downloadPartError.Store(err)
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
		"S3:downloadFileConcurrent finish.")
	return nil
}

func (o *S3) getDownloadCheckpointFile(
	ctx context.Context,
	dfc *DownloadCheckpoint,
	input *obs.DownloadFileInput,
	output *obs.GetObjectMetadataOutput) (needCheckpoint bool, err error) {

	Logger.WithContext(ctx).Debug(
		"S3:getDownloadCheckpointFile start.",
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
			"S3:loadCheckpointFile failed.",
			" checkpointFilePath: ", checkpointFilePath,
			" err: ", err)
		return true, nil
	} else if !dfc.IsValid(ctx, input, output) {
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

func (o *S3) prepareTempFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"S3:prepareTempFile start.",
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
			"S3:createFile finish.",
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
		"S3:prepareTempFile finish.")
	return nil
}

func (o *S3) createFile(
	ctx context.Context,
	tempFileURL string,
	fileSize int64) error {

	Logger.WithContext(ctx).Debug(
		"S3:createFile start.",
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
		"S3:createFile finish.")
	return nil
}

func (o *S3) handleDownloadTaskResult(
	ctx context.Context,
	result interface{},
	dfc *DownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	Logger.WithContext(ctx).Debug(
		"S3:handleDownloadTaskResult start.",
		" partNum: ", partNum,
		" checkpointFile: ", checkpointFile)

	if _, ok := result.(*obs.GetObjectOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ctx, dfc, checkpointFile)
			if nil != _err {
				Logger.WithContext(ctx).Warn(
					"S3:updateCheckpointFile failed.",
					" checkpointFile: ", checkpointFile,
					" err: ", _err)
			}
		}
	} else if result != errAbort {
		if _err, ok := result.(error); ok {
			err = _err
		}
	}
	Logger.WithContext(ctx).Debug(
		"S3:handleDownloadTaskResult finish.")
	return
}

func (o *S3) handleDownloadFileResult(
	ctx context.Context,
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	Logger.WithContext(ctx).Debug(
		"S3:handleDownloadFileResult start.",
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
			"S3.handleDownloadFileResult finish.",
			" tempFileURL: ", tempFileURL,
			" downloadFileError: ", downloadFileError)
		return downloadFileError
	}

	Logger.WithContext(ctx).Debug(
		"S3.handleDownloadFileResult finish.")
	return nil
}

func (o *S3) updateDownloadFile(
	ctx context.Context,
	filePath string,
	rangeStart int64,
	output *obs.GetObjectOutput) error {

	Logger.WithContext(ctx).Debug(
		"S3:updateDownloadFile start.",
		" filePath: ", filePath,
		" rangeStart: ", rangeStart)

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
	_, err = fd.Seek(rangeStart, 0)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"seek file failed.",
			" filePath: ", filePath,
			" rangeStart: ", rangeStart,
			" err: ", err)
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
		"S3:updateDownloadFile finish.",
		" readTotal: ", readTotal)
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

	Logger.WithContext(ctx).Debug(
		"DownloadCheckpoint:IsValid start.",
		" dfc.Bucket: ", dfc.Bucket,
		" input.Bucket: ", input.Bucket,
		" dfc.Key: ", dfc.Key,
		" input.Key: ", input.Key,
		" dfc.VersionId: ", dfc.VersionId,
		" input.VersionId: ", input.VersionId,
		" dfc.DownloadFile: ", dfc.DownloadFile,
		" input.DownloadFile: ", input.DownloadFile,
		" dfc.ObjectInfo.LastModified: ", dfc.ObjectInfo.LastModified,
		" output.LastModified.Unix(): ", output.LastModified.Unix(),
		" dfc.ObjectInfo.ETag: ", dfc.ObjectInfo.ETag,
		" output.ETag: ", output.ETag,
		" dfc.ObjectInfo.Size: ", dfc.ObjectInfo.Size,
		" output.ContentLength: ", output.ContentLength,
		" dfc.TempFileInfo.Size: ", dfc.TempFileInfo.Size,
		" output.ContentLength: ", output.ContentLength)

	if dfc.Bucket != input.Bucket ||
		dfc.Key != input.Key ||
		dfc.VersionId != input.VersionId ||
		dfc.DownloadFile != input.DownloadFile {

		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" the bucketName or objectKey or downloadFile was changed.",
			" clear the record.")
		return false
	}
	if dfc.ObjectInfo.LastModified != output.LastModified.Unix() ||
		dfc.ObjectInfo.ETag != output.ETag ||
		dfc.ObjectInfo.Size != output.ContentLength {

		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" the object info was changed.",
			" clear the record.")
		return false
	}
	if dfc.TempFileInfo.Size != output.ContentLength {
		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" size was changed.",
			" clear the record.")
		return false
	}
	stat, err := os.Stat(dfc.TempFileInfo.TempFileUrl)
	if nil != err || stat.Size() != dfc.ObjectInfo.Size {
		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" the temp download file was changed.",
			" clear the record.")
		return false
	}
	Logger.WithContext(ctx).Debug(
		"DownloadCheckpoint:IsValid finish.")
	return true
}

type DownloadPartTask struct {
	obs.GetObjectInput
	obsClient        *obs.ObsClient
	s3               *S3
	abort            *int32
	partNumber       int64
	tempFileURL      string
	enableCheckpoint bool
}

func (task *DownloadPartTask) Run(
	ctx context.Context,
	objectKey string,
	taskId int32) interface{} {

	Logger.WithContext(ctx).Debug(
		"DownloadPartTask:Run start.",
		" objectKey: ", objectKey,
		" taskId: ", taskId,
		" partNumber: ", task.partNumber)

	if atomic.LoadInt32(task.abort) == 1 {
		Logger.WithContext(ctx).Info(
			"task abort.")
		return errAbort
	}

	createGetObjectSignedUrlReq := new(CreateGetObjectSignedUrlReq)
	createGetObjectSignedUrlReq.TaskId = taskId
	createGetObjectSignedUrlReq.Source = objectKey
	createGetObjectSignedUrlReq.RangeStart = task.RangeStart
	createGetObjectSignedUrlReq.RangeEnd = task.RangeEnd

	err, createGetObjectSignedUrlResp :=
		UClient.CreateGetObjectSignedUrl(
			ctx,
			createGetObjectSignedUrlReq)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"UrchinClient.CreateGetObjectSignedUrl failed.",
			" err: ", err)
		return err
	}
	var getObjectWithSignedUrlHeader = http.Header{}
	for key, item := range createGetObjectSignedUrlResp.Header {
		for _, value := range item.Values {
			getObjectWithSignedUrlHeader.Set(key, value)
		}
	}

	getObjectWithSignedUrlOutput, err :=
		task.obsClient.GetObjectWithSignedUrl(
			createGetObjectSignedUrlResp.SignedUrl,
			getObjectWithSignedUrlHeader)

	if nil == err {
		Logger.WithContext(ctx).Debug(
			"obsClient.GetObjectWithSignedUrl finish.")
		defer func() {
			errMsg := getObjectWithSignedUrlOutput.Body.Close()
			if errMsg != nil {
				Logger.WithContext(ctx).Warn(
					"close response body failed.")
			}
		}()
		_err := task.s3.updateDownloadFile(
			ctx,
			task.tempFileURL,
			task.RangeStart,
			getObjectWithSignedUrlOutput)
		if nil != _err {
			if !task.enableCheckpoint {
				atomic.CompareAndSwapInt32(task.abort, 0, 1)
				Logger.WithContext(ctx).Warn(
					"not enableCheckpoint abort task.",
					" partNumber: ", task.partNumber)
			}
			Logger.WithContext(ctx).Error(
				"S3.updateDownloadFile failed.",
				" err: ", _err)
			return _err
		}
		Logger.WithContext(ctx).Debug(
			"DownloadPartTask.Run finish.")
		return getObjectWithSignedUrlOutput
	} else if obsError, ok := err.(obs.ObsError); ok &&
		obsError.StatusCode >= 400 &&
		obsError.StatusCode < 500 {

		atomic.CompareAndSwapInt32(task.abort, 0, 1)
		Logger.WithContext(ctx).Warn(
			"4** error abort task.",
			" partNumber: ", task.partNumber)
	}

	Logger.WithContext(ctx).Error(
		"DownloadPartTask:Run failed.",
		" err: ", err)
	return err
}
