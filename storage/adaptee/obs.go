package adaptee

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

var errAbort = errors.New("AbortError")

type ObsAdapteeWithAuth struct {
	obsClient *obs.ObsClient
}

func (o *ObsAdapteeWithAuth) Init(accessKey, secretKey, endPoint string) (err error) {
	obs.DoLog(obs.LEVEL_DEBUG, "Function ObsAdapteeWithAuth:Init start."+
		" accessKey: %s, secretKey: %s, endPoint: %s",
		accessKey, secretKey, endPoint)

	o.obsClient, err = obs.New(accessKey, secretKey, endPoint)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "obs.New failed. err: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Function ObsAdapteeWithAuth:Init finish.")
	return nil
}

func (o *ObsAdapteeWithAuth) CreateInitiateMultipartUploadSignedUrl(
	bucketName, objectKey string, expires int) (
	signedUrl string, header http.Header, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateInitiateMultipartUploadSignedUrl start."+
			" bucketName: %s, objectKey: %s, expires: %d",
		bucketName, objectKey, expires)

	input := &obs.CreateSignedUrlInput{}
	input.Method = obs.HttpMethodPost
	input.Bucket = bucketName
	input.Key = objectKey
	input.Expires = expires
	input.SubResource = obs.SubResourceUploads
	output, err := o.obsClient.CreateSignedUrl(input)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed."+
				" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return signedUrl, header, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
			return signedUrl, header, err
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateInitiateMultipartUploadSignedUrl finish.")
	return output.SignedUrl, output.ActualSignedRequestHeaders, nil
}

func (o *ObsAdapteeWithAuth) CreateUploadPartSignedUrl(
	bucketName, objectKey, uploadId, partNumber string, expires int) (
	signedUrl string, header http.Header, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithAuth:CreateUploadPartSignedUrl start."+
		" bucketName: %s, objectKey: %s, uploadId: %s, partNumber: %s, expires: %d",
		bucketName, objectKey, uploadId, partNumber, expires)

	input := &obs.CreateSignedUrlInput{}
	input.Method = obs.HttpMethodPut
	input.Bucket = bucketName
	input.Key = objectKey
	input.Expires = expires
	input.QueryParams = make(map[string]string)
	input.QueryParams["uploadId"] = uploadId
	input.QueryParams["partNumber"] = partNumber
	output, err := o.obsClient.CreateSignedUrl(input)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed."+
				" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return signedUrl, header, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
			return signedUrl, header, err
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithAuth:CreateUploadPartSignedUrl finish.")
	return output.SignedUrl, output.ActualSignedRequestHeaders, nil
}

func (o *ObsAdapteeWithAuth) CreateCompleteMultipartUploadSignedUrl(
	bucketName, objectKey, uploadId string, expires int) (
	signedUrl string, header http.Header, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateCompleteMultipartUploadSignedUrl start. "+
			" bucketName: %s, objectKey: %s, uploadId: %s, expires: %d",
		bucketName, objectKey, uploadId, expires)

	input := &obs.CreateSignedUrlInput{}
	input.Method = obs.HttpMethodPost
	input.Bucket = bucketName
	input.Key = objectKey
	input.Expires = expires
	input.QueryParams = make(map[string]string)
	input.QueryParams["uploadId"] = uploadId
	output, err := o.obsClient.CreateSignedUrl(input)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed."+
				" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return signedUrl, header, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
			return signedUrl, header, err
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateCompleteMultipartUploadSignedUrl finish.")
	return output.SignedUrl, output.ActualSignedRequestHeaders, nil
}

func (o *ObsAdapteeWithAuth) CreateNewFolderSignedUrl(
	bucketName, objectKey string, expires int) (
	signedUrl string, header http.Header, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateNewFolderSignedUrl start. "+
			" bucketName: %s, objectKey: %s, expires: %d",
		bucketName, objectKey, expires)

	input := &obs.CreateSignedUrlInput{}
	input.Method = obs.HttpMethodPut
	input.Bucket = bucketName
	input.Key = objectKey
	input.Expires = expires
	output, err := o.obsClient.CreateSignedUrl(input)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed."+
				" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return signedUrl, header, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
			return signedUrl, header, err
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateNewFolderSignedUrl finish.")
	return output.SignedUrl, output.ActualSignedRequestHeaders, nil
}

func (o *ObsAdapteeWithAuth) CreateGetObjectMetadataSignedUrl(
	bucketName, objectKey string, expires int) (
	signedUrl string, header http.Header, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateGetObjectMetadataSignedUrl start. "+
			" bucketName: %s, objectKey: %s, expires: %d",
		bucketName, objectKey, expires)

	input := &obs.CreateSignedUrlInput{}
	input.Method = obs.HTTP_HEAD
	input.Bucket = bucketName
	input.Key = objectKey
	output, err := o.obsClient.CreateSignedUrl(input)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed."+
				" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return signedUrl, header, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
			return signedUrl, header, err
		}
	}

	obs.DoLog(
		obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateGetObjectMetadataSignedUrl finish.")
	return output.SignedUrl, output.ActualSignedRequestHeaders, nil
}

func (o *ObsAdapteeWithAuth) CreateGetObjectSignedUrl(
	bucketName, objectKey string,
	expires int) (signedUrl string, header http.Header, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateGetObjectSignedUrl start. "+
			" bucketName: %s, objectKey: %s, expires: %d",
		bucketName, objectKey, expires)

	input := &obs.CreateSignedUrlInput{}
	input.Method = obs.HttpMethodGet
	input.Bucket = bucketName
	input.Key = objectKey
	input.Expires = expires

	output, err := o.obsClient.CreateSignedUrl(input)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed."+
				" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return signedUrl, header, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
			return signedUrl, header, err
		}
	}

	obs.DoLog(
		obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateGetObjectSignedUrl finish.")
	return output.SignedUrl, output.ActualSignedRequestHeaders, nil
}

func (o *ObsAdapteeWithAuth) CreateListObjectsSignedUrl(
	bucketName, prefix string, expires int) (
	signedUrl string, header http.Header, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateListObjectsSignedUrl start. "+
			" bucketName: %s, prefix: %s, expires: %d",
		bucketName, prefix, expires)

	input := &obs.CreateSignedUrlInput{}
	input.Method = obs.HttpMethodGet
	input.Bucket = bucketName
	input.Expires = expires
	input.QueryParams = make(map[string]string)
	input.QueryParams["prefix"] = prefix
	output, err := o.obsClient.CreateSignedUrl(input)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed."+
				" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return signedUrl, header, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
			return signedUrl, header, err
		}
	}

	obs.DoLog(
		obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateListObjectsSignedUrl finish.")
	return output.SignedUrl, output.ActualSignedRequestHeaders, nil
}

type ObsAdapteeWithSignedUrl struct {
	obsClient *obs.ObsClient
}

func (o *ObsAdapteeWithSignedUrl) Init() (err error) {
	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Init start.")

	o.obsClient, err = obs.New("", "", "magicalParam")
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "obs.New failed. err: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Init finish.")
	return nil
}

func (o *ObsAdapteeWithSignedUrl) Upload(
	urchinServiceAddr, sourcePath string, taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Upload start."+
		" urchinServiceAddr: %s, sourcePath: %s, taskId: %d",
		urchinServiceAddr, sourcePath, taskId)

	stat, err := os.Stat(sourcePath)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. urchinServiceAddr: %s sourcePath: %s, err: %v",
			urchinServiceAddr, sourcePath, err)
		return err
	}
	if stat.IsDir() {
		err = o.uploadFolder(urchinServiceAddr, sourcePath, taskId)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"uploadFolder failed. urchinServiceAddr: %s, sourcePath: %s,"+
					" taskId: %d, err: %v",
				urchinServiceAddr, sourcePath, taskId, err)
			return err
		}
	} else {
		objectKey := filepath.Base(sourcePath)
		err = o.uploadFile(urchinServiceAddr, sourcePath, objectKey, taskId)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"uploadFile failed. urchinServiceAddr: %s, sourcePath: %s,"+
					" taskId: %d, err: %v",
				urchinServiceAddr, sourcePath, taskId, err)
			return err
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Upload finish.")
	return nil
}

func (o *ObsAdapteeWithSignedUrl) uploadFile(
	urchinServiceAddr, sourceFile, objectKey string, taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:uploadFile start."+
			" urchinServiceAddr: %s, sourceFile: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, sourceFile, objectKey, taskId)

	stat, err := os.Stat(sourceFile)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. sourceFile: %s, err: %v", sourceFile, err)
		return
	}
	fileSize := stat.Size()

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	if 0 == fileSize {
		createEmptyFileSignedUrlReq := new(CreateNewFolderSignedUrlReq)
		createEmptyFileSignedUrlReq.TaskId = taskId
		createEmptyFileSignedUrlReq.Source = &objectKey

		_err, createEmptyFileSignedUrlResp :=
			urchinService.CreateNewFolderSignedUrl(
				ConfigDefaultUrchinServiceCreateNewFolderSignedUrlInterface,
				createEmptyFileSignedUrlReq)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "CreateEmptyFileSignedUrl failed."+
				" objectKey:%s, err: %v", objectKey, _err)
			return _err
		}
		var emptyFileWithSignedUrlHeader = http.Header{}
		for key, item := range createEmptyFileSignedUrlResp.Header {
			for _, value := range item.Values {
				emptyFileWithSignedUrlHeader.Set(key, value)
			}
		}
		// 创建空文件
		_, _err = o.obsClient.PutObjectWithSignedUrl(
			createEmptyFileSignedUrlResp.SignedUrl,
			emptyFileWithSignedUrlHeader,
			nil)
		if _err != nil {
			if obsError, ok := _err.(obs.ObsError); ok {
				obs.DoLog(obs.LEVEL_ERROR,
					"obsClient.PutObjectWithSignedUrl failed."+
						" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
				return _err
			} else {
				obs.DoLog(obs.LEVEL_ERROR,
					"obsClient.PutObjectWithSignedUrl failed. error: %v", _err)
				return _err
			}
		}
		obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:uploadFile finish.")
		return nil
	}

	createInitiateMultipartUploadSignedUrlReq := new(CreateInitiateMultipartUploadSignedUrlReq)
	createInitiateMultipartUploadSignedUrlReq.TaskId = taskId
	createInitiateMultipartUploadSignedUrlReq.Source = objectKey

	err, createInitiateMultipartUploadSignedUrlResp :=
		urchinService.CreateInitiateMultipartUploadSignedUrl(
			ConfigDefaultUrchinServiceCreateInitiateMultipartUploadSignedUrlInterface,
			createInitiateMultipartUploadSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateInitiateMultipartUploadSignedUrl failed. err: %v", err)
		return err
	}
	var initiateMultipartUploadWithSignedUrlHeader = http.Header{}
	for key, item := range createInitiateMultipartUploadSignedUrlResp.Header {
		for _, value := range item.Values {
			initiateMultipartUploadWithSignedUrlHeader.Set(key, value)
		}
	}
	uploadId, err := o.initiateMultipartUploadWithSignedUrl(
		createInitiateMultipartUploadSignedUrlResp.SignedUrl,
		initiateMultipartUploadWithSignedUrlHeader)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"initiateMultipartUploadWithSignedUrl failed."+
				" signedUrl: %s, err: %v",
			createInitiateMultipartUploadSignedUrlResp.SignedUrl, err)
		return err
	}
	err = o.uploadPartWithSignedUrl(
		urchinServiceAddr,
		sourceFile,
		objectKey,
		uploadId,
		taskId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "uploadPartWithSignedUrl failed."+
			"addr: %s, sourceFile: %s, objectKey: %s, uploadId: %s, err: %v",
			urchinServiceAddr, sourceFile, objectKey, uploadId, err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:uploadFile finish.")
	return nil
}

func (o *ObsAdapteeWithSignedUrl) initiateMultipartUploadWithSignedUrl(
	signedUrl string,
	actualSignedRequestHeaders http.Header) (uploadId string, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:initiateMultipartUploadWithSignedUrl start."+
			" signedUrl: %s", signedUrl)

	// 初始化分段上传任务
	output, err := o.obsClient.InitiateMultipartUploadWithSignedUrl(
		signedUrl,
		actualSignedRequestHeaders)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.InitiateMultipartUploadWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return uploadId, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.InitiateMultipartUploadWithSignedUrl failed."+
					" signedUrl: %s, err: %v", signedUrl, err)
			return uploadId, err
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:initiateMultipartUploadWithSignedUrl finish."+
			" signedUrl: %s, uploadId: %s",
		signedUrl, output.UploadId)

	return output.UploadId, nil
}

func (o *ObsAdapteeWithSignedUrl) uploadPartWithSignedUrl(
	urchinServiceAddr,
	sourceFile,
	objectKey,
	uploadId string,
	taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:uploadPartWithSignedUrl start."+
			" urchinServiceAddr: %s, sourceFile: %s, objectKey: %s, uploadId: %s",
		urchinServiceAddr, sourceFile, objectKey, uploadId)

	var partSize int64 = DefaultPartSize
	stat, err := os.Stat(sourceFile)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. sourceFile: %s, err: %v", sourceFile, err)
		return
	}
	fileSize := stat.Size()

	// 计算需要上传的段数
	partCount := int(fileSize / partSize)

	if fileSize%partSize != 0 {
		partCount++
	}

	// 执行并发上传段
	partChan := make(chan XPart, 5)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	for i := 0; i < partCount; i++ {
		partNumber := i + 1
		offset := int64(i) * partSize
		currPartSize := partSize
		if i+1 == partCount {
			currPartSize = fileSize - offset
		}
		go func() {
			fd, _err := os.Open(sourceFile)
			if _err != nil {
				err = _err
				return
			}
			defer func() {
				errMsg := fd.Close()
				if errMsg != nil {
					obs.DoLog(obs.LEVEL_WARN,
						"Failed to close file with reason: %v", errMsg)
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
			if _, err = fd.Seek(offset, io.SeekStart); err != nil {
				return
			}

			createUploadPartSignedUrlReq := new(CreateUploadPartSignedUrlReq)
			createUploadPartSignedUrlReq.UploadId = uploadId
			createUploadPartSignedUrlReq.PartNumber = int32(partNumber)
			createUploadPartSignedUrlReq.TaskId = taskId
			createUploadPartSignedUrlReq.Source = objectKey
			err, createUploadPartSignedUrlResp :=
				urchinService.CreateUploadPartSignedUrl(
					ConfigDefaultUrchinServiceCreateUploadPartSignedUrlInterface,
					createUploadPartSignedUrlReq)
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"CreateUploadPartSignedUrl failed. err: %v", err)
				return
			}
			var uploadPartWithSignedUrlHeader = http.Header{}
			for key, item := range createUploadPartSignedUrlResp.Header {
				for _, value := range item.Values {
					uploadPartWithSignedUrlHeader.Set(key, value)
				}
			}

			uploadPartInputOutput, err := o.obsClient.UploadPartWithSignedUrl(
				createUploadPartSignedUrlResp.SignedUrl,
				uploadPartWithSignedUrlHeader,
				readerWrapper)

			if err != nil {
				if obsError, ok := err.(obs.ObsError); ok {
					obs.DoLog(obs.LEVEL_ERROR,
						"obsClient.UploadPartWithSignedUrl failed."+
							" signedUrl: %s, sourceFile: %s, objectKey: %s,"+
							" partNumber: %d, offset: %d,"+
							" currPartSize: %d, obsCode: %s, obsMessage: %s",
						createUploadPartSignedUrlResp.SignedUrl,
						sourceFile, objectKey, partNumber, offset, currPartSize,
						obsError.Code, obsError.Message)
					return
				} else {
					obs.DoLog(obs.LEVEL_ERROR,
						"obsClient.UploadPartWithSignedUrl failed."+
							" signedUrl: %s, sourceFile: %s, partNumber: %d,"+
							" offset: %d, currPartSize: %d, err: %v",
						createUploadPartSignedUrlResp.SignedUrl,
						sourceFile, partNumber, offset, currPartSize, err)
					return
				}
			}
			obs.DoLog(obs.LEVEL_INFO, "obsClient.UploadPartWithSignedUrl success."+
				" signedUrl: %s, sourceFile: %s, objectKey: %s, partNumber: %d,"+
				" offset: %d, currPartSize: %d, ETag: %s",
				createUploadPartSignedUrlResp.SignedUrl, sourceFile, objectKey, partNumber,
				offset, currPartSize, strings.Trim(uploadPartInputOutput.ETag, "\""))
			partChan <- XPart{
				ETag:       strings.Trim(uploadPartInputOutput.ETag, "\""),
				PartNumber: partNumber}
		}()
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

	// 合并段
	createCompleteMultipartUploadSignedUrlReq := new(CreateCompleteMultipartUploadSignedUrlReq)
	createCompleteMultipartUploadSignedUrlReq.UploadId = uploadId
	createCompleteMultipartUploadSignedUrlReq.TaskId = taskId
	createCompleteMultipartUploadSignedUrlReq.Source = objectKey

	err, createCompleteMultipartUploadSignedUrlResp :=
		urchinService.CreateCompleteMultipartUploadSignedUrl(
			ConfigDefaultUrchinServiceCreateCompleteMultipartUploadSignedUrlInterface,
			createCompleteMultipartUploadSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateCompleteMultipartUploadSignedUrl failed. err: %v", err)
		return
	}

	var partSlice PartSlice = parts
	sort.Sort(partSlice)

	var completeMultipartUploadPart CompleteMultipartUploadPart
	completeMultipartUploadPart.PartSlice = partSlice
	completeMultipartUploadPartXML, err :=
		xml.MarshalIndent(completeMultipartUploadPart, "", "  ")
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "xml.MarshalIndent failed. err: %v", err)
		return
	}

	var completeMultipartUploadWithSignedUrlHeader = http.Header{}
	for key, item := range createCompleteMultipartUploadSignedUrlResp.Header {
		for _, value := range item.Values {
			completeMultipartUploadWithSignedUrlHeader.Set(key, value)
		}
	}
	completeMultipartUploadOutput, err :=
		o.obsClient.CompleteMultipartUploadWithSignedUrl(
			createCompleteMultipartUploadSignedUrlResp.SignedUrl,
			completeMultipartUploadWithSignedUrlHeader,
			strings.NewReader(string(completeMultipartUploadPartXML)))
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.CompleteMultipartUploadWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.CompleteMultipartUploadWithSignedUrl failed. err: %v", err)
			return
		}
	}
	obs.DoLog(obs.LEVEL_INFO,
		"obsClient.CompleteMultipartUploadWithSignedUrl success. requestId: %s",
		completeMultipartUploadOutput.RequestId)

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:uploadPartWithSignedUrl finish.")
	return
}

func (o *ObsAdapteeWithSignedUrl) uploadFolder(
	urchinServiceAddr, dirPath string, taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:uploadFolder start."+
			" urchinServiceAddr: %s, dirPath: %s, taskId: %d",
		urchinServiceAddr, dirPath, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createNewFolderSignedUrlReq := new(CreateNewFolderSignedUrlReq)
	createNewFolderSignedUrlReq.TaskId = taskId

	err, createNewFolderSignedUrlResp :=
		urchinService.CreateNewFolderSignedUrl(
			ConfigDefaultUrchinServiceCreateNewFolderSignedUrlInterface,
			createNewFolderSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "CreateNewFolderSignedUrl failed. err: %v", err)
		return err
	}
	var newFolderWithSignedUrlHeader = http.Header{}
	for key, item := range createNewFolderSignedUrlResp.Header {
		for _, value := range item.Values {
			newFolderWithSignedUrlHeader.Set(key, value)
		}
	}
	// 创建文件夹
	_, err = o.obsClient.PutObjectWithSignedUrl(
		createNewFolderSignedUrlResp.SignedUrl,
		newFolderWithSignedUrlHeader,
		nil)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.PutObjectWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.PutObjectWithSignedUrl failed. err: %v", err)
			return err
		}
	}

	var wg sync.WaitGroup
	err = filepath.Walk(dirPath, func(filePath string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"filepath.Walk failed. urchinServiceAddr: %s, dirPath: %s, err: %v",
				urchinServiceAddr, dirPath, err)
			return err
		}
		if !fileInfo.IsDir() {
			wg.Add(1)
			// 处理文件
			go func() {
				defer func() {
					wg.Done()
					if err := recover(); err != nil {
						obs.DoLog(obs.LEVEL_ERROR, "uploadFile failed. err: %v", err)
					}
				}()
				objectKey, err := filepath.Rel(dirPath, filePath)
				if err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"filepath.Rel failed. urchinServiceAddr: %s, dirPath: %s,"+
							" filePath: %s, objectKey: %s, err: %v",
						urchinServiceAddr, dirPath, filePath, objectKey, err)
					return
				}
				err = o.uploadFile(urchinServiceAddr, filePath, objectKey, taskId)
				if err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"uploadFile failed."+
							" urchinServiceAddr: %s, filePath: %s, objectKey: %s, err: %v",
						urchinServiceAddr, filePath, objectKey, err)
				}
			}()
		}
		return nil
	})
	wg.Wait()

	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"UploadFolder failed. urchinServiceAddr: %s, dirPath: %s, err: %v",
			urchinServiceAddr, dirPath, err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:uploadFolder finish.")
	return nil
}

func (o *ObsAdapteeWithSignedUrl) Download(
	urchinServiceAddr, targetPath string, taskId int32, bucketName string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Download start."+
		" urchinServiceAddr: %s, targetPath: %s, taskId: %d",
		urchinServiceAddr, targetPath, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createListObjectsSignedUrlReq := new(CreateListObjectsSignedUrlReq)
	createListObjectsSignedUrlReq.TaskId = taskId

	err, createListObjectsSignedUrlResp :=
		urchinService.CreateListObjectsSignedUrl(
			ConfigDefaultUrchinServiceCreateListObjectsSignedUrlInterface,
			createListObjectsSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateListObjectSignedUrl failed. err: %v", err)
		return err
	}

	var listObjectsWithSignedUrlHeader = http.Header{}
	for key, item := range createListObjectsSignedUrlResp.Header {
		for _, value := range item.Values {
			listObjectsWithSignedUrlHeader.Set(key, value)
		}
	}
	listObjectsOutput, err :=
		o.obsClient.ListObjectsWithSignedUrl(
			createListObjectsSignedUrlResp.SignedUrl,
			listObjectsWithSignedUrlHeader)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.ListObjectsWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.CompleteMultipartUploadWithSignedUrl failed. err: %v", err)
			return err
		}
	}

	var wg sync.WaitGroup
	for index, object := range listObjectsOutput.Contents {
		obs.DoLog(obs.LEVEL_DEBUG,
			"Object: Content[%d]-ETag:%s, Key:%s, Size:%d",
			index, object.ETag, object.Key, object.Size)
		wg.Add(1)
		// 处理文件
		itemObject := object
		go func() {
			defer func() {
				wg.Done()
			}()
			_, err = o.downloadPartWithSignedUrl(
				urchinServiceAddr,
				bucketName,
				itemObject.Key,
				targetPath+itemObject.Key,
				taskId)
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"downloadPartWithSignedUrl failed."+
						" urchinServiceAddr: %s, objectKey: %s, taskId: %d, err: %v",
					urchinServiceAddr, itemObject.Key, taskId, err)
			}
		}()
	}
	wg.Wait()

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Download finish.")
	return nil
}

func (o *ObsAdapteeWithSignedUrl) downloadPartWithSignedUrl(
	urchinServiceAddr, bucketName, objectKey, targetFile string,
	taskId int32) (output *obs.GetObjectMetadataOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:downloadPartWithSignedUrl start."+
		" urchinServiceAddr: %s, objectKey: %s, targetFile: %s, taskId: %d",
		urchinServiceAddr, objectKey, targetFile, taskId)

	if '/' == objectKey[len(objectKey)-1] {
		parentDir := filepath.Dir(targetFile)
		stat, err := os.Stat(parentDir)
		if err != nil {
			obs.DoLog(obs.LEVEL_DEBUG, "Failed to stat path. error: %v", err)
			_err := os.MkdirAll(parentDir, os.ModePerm)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR, "Failed to make dir. error: %v", _err)
				return output, _err
			}
			return output, nil
		} else if !stat.IsDir() {
			obs.DoLog(obs.LEVEL_ERROR,
				"Cannot create folder: %s due to a same file exists.", parentDir)
			return output,
				fmt.Errorf("cannot create folder: %s due to a same file exists", parentDir)
		} else {
			return output, nil
		}
	}

	downloadFileInput := new(obs.DownloadFileInput)
	downloadFileInput.DownloadFile = targetFile
	downloadFileInput.EnableCheckpoint = true
	downloadFileInput.CheckpointFile = downloadFileInput.DownloadFile + ".downloadfile_record"
	downloadFileInput.TaskNum = DefaultDownloadFileTaskNum
	downloadFileInput.PartSize = DefaultPartSize
	downloadFileInput.Bucket = bucketName
	downloadFileInput.Key = objectKey

	output, err = o.resumeDownload(urchinServiceAddr, objectKey, taskId, downloadFileInput)

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:downloadPartWithSignedUrl success.")

	return
}

func (o *ObsAdapteeWithSignedUrl) resumeDownload(
	urchinServiceAddr, objectKey string,
	taskId int32,
	input *obs.DownloadFileInput) (
	output *obs.GetObjectMetadataOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:resumeDownload start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	getObjectmetaOutput, err := o.getObjectInfoWithSignedUrl(
		urchinServiceAddr, objectKey, taskId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"getObjectInfoWithSignedUrl failed."+
				" urchinServiceAddr: %s, objectKey: %s, taskId: %d, err: %v",
			urchinServiceAddr, objectKey, taskId, err)
		return nil, err
	}

	objectSize := getObjectmetaOutput.ContentLength
	partSize := input.PartSize
	dfc := &DownloadCheckpoint{}

	var needCheckpoint = true
	var checkpointFilePath = input.CheckpointFile
	var enableCheckpoint = input.EnableCheckpoint
	if enableCheckpoint {
		needCheckpoint, err = getDownloadCheckpointFile(dfc, input, getObjectmetaOutput)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"getDownloadCheckpointFile failed."+
					" checkpointFilePath: %s, enableCheckpoint: %t, err: %v",
				checkpointFilePath, enableCheckpoint, err)
			return nil, err
		}
	}

	if needCheckpoint {
		dfc.Bucket = input.Bucket
		dfc.Key = input.Key
		dfc.VersionId = input.VersionId
		dfc.DownloadFile = input.DownloadFile
		dfc.ObjectInfo = obs.ObjectInfo{}
		dfc.ObjectInfo.LastModified = getObjectmetaOutput.LastModified.Unix()
		dfc.ObjectInfo.Size = getObjectmetaOutput.ContentLength
		dfc.ObjectInfo.ETag = getObjectmetaOutput.ETag
		dfc.TempFileInfo = obs.TempFileInfo{}
		dfc.TempFileInfo.TempFileUrl = input.DownloadFile + ".tmp"
		dfc.TempFileInfo.Size = getObjectmetaOutput.ContentLength

		sliceObject(objectSize, partSize, dfc)
		_err := prepareTempFile(dfc.TempFileInfo.TempFileUrl, dfc.TempFileInfo.Size)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"prepareTempFile failed. TempFileUrl: %s, Size: %d, err: %v",
				dfc.TempFileInfo.TempFileUrl, dfc.TempFileInfo.Size, _err)
			return nil, _err
		}

		if enableCheckpoint {
			_err := updateCheckpointFile(dfc, checkpointFilePath)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"Failed to update checkpoint file. error: %v", _err)
				_errMsg := os.Remove(dfc.TempFileInfo.TempFileUrl)
				if _errMsg != nil {
					obs.DoLog(obs.LEVEL_WARN,
						"Failed to remove temp download file. error: %v", _errMsg)
				}
				return nil, _err
			}
		}
	}

	downloadFileError := o.downloadFileConcurrent(
		urchinServiceAddr, objectKey, taskId, input, dfc)
	err = handleDownloadFileResult(
		dfc.TempFileInfo.TempFileUrl,
		enableCheckpoint,
		downloadFileError)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"handleDownloadFileResult failed. TempFileUrl: %s, err: %v",
			dfc.TempFileInfo.TempFileUrl, err)
		return nil, err
	}

	err = os.Rename(dfc.TempFileInfo.TempFileUrl, input.DownloadFile)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Failed to rename temp download file."+
				" TempFileUrl: %s, DownloadFile: %s, error: %v",
			dfc.TempFileInfo.TempFileUrl, input.DownloadFile, err)
		return nil, err
	}
	if enableCheckpoint {
		err = os.Remove(checkpointFilePath)
		if err != nil {
			obs.DoLog(obs.LEVEL_WARN,
				"Download file successfully,"+
					" but remove checkpoint file failed. error: %v", err)
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:resumeDownload success.")
	return getObjectmetaOutput, nil
}

func (o *ObsAdapteeWithSignedUrl) getObjectInfoWithSignedUrl(
	urchinServiceAddr, objectKey string, taskId int32) (
	getObjectmetaOutput *obs.GetObjectMetadataOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:getObjectInfoWithSignedUrl start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createGetObjectMetadataSignedUrlReq := new(CreateGetObjectMetadataSignedUrlReq)
	createGetObjectMetadataSignedUrlReq.TaskId = taskId
	createGetObjectMetadataSignedUrlReq.Source = objectKey

	err, createGetObjectMetadataSignedUrlResp :=
		urchinService.CreateGetObjectMetadataSignedUrl(
			ConfigDefaultUrchinServiceCreateGetObjectMetadataSignedUrlInterface,
			createGetObjectMetadataSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateGetObjectMetadataSignedUrl failed. err: %v", err)
		return
	}
	var getObjectMetadataWithSignedUrlHeader = http.Header{}
	for key, item := range createGetObjectMetadataSignedUrlResp.Header {
		for _, value := range item.Values {
			getObjectMetadataWithSignedUrlHeader.Set(key, value)
		}
	}

	getObjectmetaOutput, err = o.obsClient.GetObjectMetadataWithSignedUrl(
		createGetObjectMetadataSignedUrlResp.SignedUrl,
		getObjectMetadataWithSignedUrlHeader)

	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.GetObjectMetadataWithSignedUrl failed."+
					" signedUrl: %s, obsCode: %s, obsMessage: %s",
				createGetObjectMetadataSignedUrlResp.SignedUrl,
				obsError.Code, obsError.Message)
			return
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.GetObjectMetadataWithSignedUrl failed."+
					" signedUrl: %s, err: %v",
				createGetObjectMetadataSignedUrlResp.SignedUrl, err)
			return
		}
	}
	obs.DoLog(obs.LEVEL_INFO, "obsClient.GetObjectMetadataWithSignedUrl success.")
	return
}

func (o *ObsAdapteeWithSignedUrl) downloadFileConcurrent(
	urchinServiceAddr, objectKey string,
	taskId int32,
	input *obs.DownloadFileInput,
	dfc *DownloadCheckpoint) error {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:downloadFileConcurrent start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	pool := obs.NewRoutinePool(input.TaskNum, obs.MAX_PART_NUM)
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
			abort:            &abort,
			partNumber:       downloadPart.PartNumber,
			tempFileURL:      dfc.TempFileInfo.TempFileUrl,
			enableCheckpoint: input.EnableCheckpoint,
		}
		obs.DoLog(obs.LEVEL_DEBUG,
			"DownloadPartTask params."+
				" rangeStart: %d, rangeEnd: %d, partNumber: %d,"+
				" tempFileURL: %s, enableCheckpoint: %t",
			downloadPart.Offset,
			downloadPart.RangeEnd,
			downloadPart.PartNumber,
			dfc.TempFileInfo.TempFileUrl,
			input.EnableCheckpoint)

		pool.ExecuteFunc(func() interface{} {
			if 0 == dfc.ObjectInfo.Size {
				lock.Lock()
				defer lock.Unlock()
				dfc.DownloadParts[task.partNumber-1].IsCompleted = true

				if input.EnableCheckpoint {
					err := updateCheckpointFile(dfc, input.CheckpointFile)
					if err != nil {
						obs.DoLog(obs.LEVEL_WARN,
							"Failed to update checkpoint file. error: %v", err)
						downloadPartError.Store(err)
					}
				}
				return nil
			} else {
				result := task.Run(urchinServiceAddr, objectKey, taskId)
				err := handleDownloadTaskResult(
					result,
					dfc,
					task.partNumber,
					input.EnableCheckpoint,
					input.CheckpointFile,
					lock)
				if err != nil && atomic.CompareAndSwapInt32(&errFlag, 0, 1) {
					obs.DoLog(obs.LEVEL_ERROR,
						"handleDownloadTaskResult failed. err: %v", err)
					downloadPartError.Store(err)
				}
				return nil
			}
		})
	}
	pool.ShutDown()
	if err, ok := downloadPartError.Load().(error); ok {
		obs.DoLog(obs.LEVEL_ERROR, "downloadPartError. err: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:downloadFileConcurrent success.")
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
	input *obs.DownloadFileInput,
	output *obs.GetObjectMetadataOutput) bool {

	if dfc.Bucket != input.Bucket ||
		dfc.Key != input.Key ||
		dfc.VersionId != input.VersionId ||
		dfc.DownloadFile != input.DownloadFile {

		obs.DoLog(obs.LEVEL_INFO,
			"Checkpoint file is invalid, "+
				"the bucketName or objectKey or downloadFile was changed. clear the record.")
		return false
	}
	if dfc.ObjectInfo.LastModified != output.LastModified.Unix() ||
		dfc.ObjectInfo.ETag != output.ETag ||
		dfc.ObjectInfo.Size != output.ContentLength {

		obs.DoLog(obs.LEVEL_INFO,
			"Checkpoint file is invalid, the object info was changed. clear the record.")
		return false
	}
	if dfc.TempFileInfo.Size != output.ContentLength {
		obs.DoLog(obs.LEVEL_INFO,
			"Checkpoint file is invalid, size was changed. clear the record.")
		return false
	}
	stat, err := os.Stat(dfc.TempFileInfo.TempFileUrl)
	if err != nil || stat.Size() != dfc.ObjectInfo.Size {
		obs.DoLog(obs.LEVEL_INFO,
			"Checkpoint file is invalid, the temp download file was changed. "+
				"clear the record.")
		return false
	}
	return true
}

type DownloadPartTask struct {
	obs.GetObjectInput
	obsClient        *obs.ObsClient
	abort            *int32
	partNumber       int64
	tempFileURL      string
	enableCheckpoint bool
}

func (task *DownloadPartTask) Run(
	urchinServiceAddr, objectKey string, taskId int32) interface{} {

	obs.DoLog(obs.LEVEL_DEBUG, "DownloadPartTask:Run start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	if atomic.LoadInt32(task.abort) == 1 {
		return errAbort
	}

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createGetObjectSignedUrlReq := new(CreateGetObjectSignedUrlReq)
	createGetObjectSignedUrlReq.TaskId = taskId
	createGetObjectSignedUrlReq.Source = objectKey
	createGetObjectSignedUrlReq.RangeStart = task.RangeStart
	createGetObjectSignedUrlReq.RangeEnd = task.RangeEnd

	err, createGetObjectSignedUrlResp :=
		urchinService.CreateGetObjectSignedUrl(
			ConfigDefaultUrchinServiceCreateGetObjectSignedUrlInterface,
			createGetObjectSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateGetObjectSignedUrl failed. err: %v", err)
		return err
	}
	var getObjectWithSignedUrlHeader = http.Header{}
	for key, item := range createGetObjectSignedUrlResp.Header {
		for _, value := range item.Values {
			getObjectWithSignedUrlHeader.Set(key, value)
		}
	}

	getObjectWithSignedUrlOutput, err := task.obsClient.GetObjectWithSignedUrl(
		createGetObjectSignedUrlResp.SignedUrl,
		getObjectWithSignedUrlHeader)

	if err == nil {
		defer func() {
			errMsg := getObjectWithSignedUrlOutput.Body.Close()
			if errMsg != nil {
				obs.DoLog(obs.LEVEL_WARN, "Failed to close response body.")
			}
		}()
		_err := updateDownloadFile(
			task.tempFileURL,
			task.RangeStart,
			getObjectWithSignedUrlOutput)
		if _err != nil {
			if !task.enableCheckpoint {
				atomic.CompareAndSwapInt32(task.abort, 0, 1)
				obs.DoLog(obs.LEVEL_WARN,
					"Task is aborted, part number: %d", task.partNumber)
			}
			return _err
		}
		return getObjectWithSignedUrlOutput
	} else if obsError, ok := err.(obs.ObsError); ok &&
		obsError.StatusCode >= 400 &&
		obsError.StatusCode < 500 {

		atomic.CompareAndSwapInt32(task.abort, 0, 1)
		obs.DoLog(obs.LEVEL_WARN, "Task is aborted, part number: %d", task.partNumber)
	}

	obs.DoLog(obs.LEVEL_DEBUG, "DownloadPartTask:Run success.")
	return err
}

func getDownloadCheckpointFile(
	dfc *DownloadCheckpoint,
	input *obs.DownloadFileInput,
	output *obs.GetObjectMetadataOutput) (needCheckpoint bool, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"getDownloadCheckpointFile start. CheckpointFile: %s", input.CheckpointFile)

	checkpointFilePath := input.CheckpointFile
	checkpointFileStat, err := os.Stat(checkpointFilePath)
	if err != nil {
		obs.DoLog(obs.LEVEL_DEBUG,
			fmt.Sprintf("Stat checkpoint file failed. error: %v", err))
		return true, nil
	}
	if checkpointFileStat.IsDir() {
		obs.DoLog(obs.LEVEL_ERROR, "Checkpoint file can not be a folder.")
		return false, errors.New("checkpoint file can not be a folder")
	}
	err = loadCheckpointFile(checkpointFilePath, dfc)
	if err != nil {
		obs.DoLog(obs.LEVEL_WARN,
			fmt.Sprintf("Load checkpoint file failed. error: %v", err))
		return true, nil
	} else if !dfc.IsValid(input, output) {
		if dfc.TempFileInfo.TempFileUrl != "" {
			_err := os.Remove(dfc.TempFileInfo.TempFileUrl)
			if _err != nil {
				obs.DoLog(obs.LEVEL_WARN,
					"Failed to remove temp download file. error: %v", _err)
			}
		}
		_err := os.Remove(checkpointFilePath)
		if _err != nil {
			obs.DoLog(obs.LEVEL_WARN,
				"Failed to remove checkpoint file. error: %v", _err)
		}
	} else {
		obs.DoLog(obs.LEVEL_DEBUG, "no need to check point.")
		return false, nil
	}
	obs.DoLog(obs.LEVEL_DEBUG, "need to check point.")
	return true, nil
}

func loadCheckpointFile(checkpointFile string, result interface{}) error {
	obs.DoLog(obs.LEVEL_DEBUG, "loadCheckpointFile start.")
	ret, err := ioutil.ReadFile(checkpointFile)
	if err != nil {
		return err
	}
	if len(ret) == 0 {
		obs.DoLog(obs.LEVEL_DEBUG, "loadCheckpointFile nil.")
		return nil
	}
	obs.DoLog(obs.LEVEL_DEBUG, "loadCheckpointFile success.")
	return xml.Unmarshal(ret, result)
}

func sliceObject(objectSize, partSize int64, dfc *DownloadCheckpoint) {
	obs.DoLog(obs.LEVEL_DEBUG,
		"sliceObject start. objectSize: %d, partSize: %d", objectSize, partSize)

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
	obs.DoLog(obs.LEVEL_DEBUG, "sliceObject success.")
}

func prepareTempFile(tempFileURL string, fileSize int64) error {
	obs.DoLog(obs.LEVEL_DEBUG,
		"prepareTempFile start. tempFileURL: %s, fileSize: %d", tempFileURL, fileSize)

	parentDir := filepath.Dir(tempFileURL)
	stat, err := os.Stat(parentDir)
	if err != nil {
		obs.DoLog(obs.LEVEL_DEBUG, "Failed to stat path. error: %v", err)
		_err := os.MkdirAll(parentDir, os.ModePerm)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "Failed to make dir. error: %v", _err)
			return _err
		}
	} else if !stat.IsDir() {
		obs.DoLog(obs.LEVEL_ERROR,
			"Cannot create folder: %s due to a same file exists.", parentDir)
		return fmt.Errorf("cannot create folder: %s due to a same file exists", parentDir)
	}

	err = createFile(tempFileURL, fileSize)
	if err == nil {
		return nil
	}
	fd, err := os.OpenFile(tempFileURL, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Failed to open temp download file. tempFileURL: %s, error: %v",
			tempFileURL, err)
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil {
			obs.DoLog(obs.LEVEL_WARN, "Failed to close file. error: %v", errMsg)
		}
	}()
	if fileSize > 0 {
		_, err = fd.WriteAt([]byte("a"), fileSize-1)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "Failed to WriteAt file. error: %v", err)
			return err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "prepareTempFile success.")
	return nil
}

func updateCheckpointFile(fc interface{}, checkpointFilePath string) error {
	obs.DoLog(obs.LEVEL_DEBUG, "updateCheckpointFile start.")
	result, err := xml.Marshal(fc)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "xml.Marshal failed. error: %v", err)
		return err
	}
	err = ioutil.WriteFile(checkpointFilePath, result, 0640)
	obs.DoLog(obs.LEVEL_DEBUG, "updateCheckpointFile finish.")
	return err
}

func createFile(tempFileURL string, fileSize int64) error {
	obs.DoLog(obs.LEVEL_DEBUG,
		"createFile start. tempFileURL: %s, fileSize: %d", tempFileURL, fileSize)

	fd, err := syscall.Open(tempFileURL, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Failed to open temp download file. tempFileURL: %s, error: %v",
			tempFileURL, err)
		return err
	}
	defer func() {
		errMsg := syscall.Close(fd)
		if errMsg != nil {
			obs.DoLog(obs.LEVEL_WARN,
				"Failed to close file. error: %v", errMsg)
		}
	}()
	err = syscall.Ftruncate(fd, fileSize)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Failed to Ftruncate file. error: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "createFile success.")
	return nil
}

func handleDownloadTaskResult(
	result interface{},
	dfc *DownloadCheckpoint,
	partNum int64,
	enableCheckpoint bool,
	checkpointFile string,
	lock *sync.Mutex) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"handleDownloadTaskResult start. partNum: %d, checkpointFile: %s",
		partNum, checkpointFile)

	if _, ok := result.(*obs.GetObjectOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := updateCheckpointFile(dfc, checkpointFile)
			if _err != nil {
				obs.DoLog(obs.LEVEL_WARN,
					"Failed to update checkpoint file. error: %v", _err)
			}
		}
	} else if result != errAbort {
		if _err, ok := result.(error); ok {
			err = _err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "handleDownloadTaskResult finish.")
	return
}

func handleDownloadFileResult(
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	obs.DoLog(obs.LEVEL_DEBUG,
		"handleDownloadFileResult start. tempFileURL: %s", tempFileURL)

	if downloadFileError != nil {
		if !enableCheckpoint {
			_err := os.Remove(tempFileURL)
			if _err != nil {
				obs.DoLog(obs.LEVEL_WARN,
					"Failed to remove temp download file. error: %v", _err)
			}
		}
		obs.DoLog(obs.LEVEL_DEBUG, "handleDownloadFileResult finish.")
		return downloadFileError
	}

	obs.DoLog(obs.LEVEL_DEBUG, "handleDownloadFileResult success.")
	return nil
}

func updateDownloadFile(
	filePath string,
	rangeStart int64,
	output *obs.GetObjectOutput) error {

	obs.DoLog(obs.LEVEL_DEBUG,
		"updateDownloadFile start. filePath: %s, rangeStart: %d", filePath, rangeStart)

	fd, err := os.OpenFile(filePath, os.O_WRONLY, 0640)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Failed to open file. filePath: %s, error: %v", filePath, err)
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil {
			obs.DoLog(obs.LEVEL_WARN, "Failed to close file. error: %v", errMsg)
		}
	}()
	_, err = fd.Seek(rangeStart, 0)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Failed to seek file. error: %v", err)
		return err
	}
	fileWriter := bufio.NewWriterSize(fd, 65536)
	part := make([]byte, 8192)
	var readErr error
	var readCount, readTotal int
	for {
		readCount, readErr = output.Body.Read(part)
		if readCount > 0 {
			wcnt, werr := fileWriter.Write(part[0:readCount])
			if werr != nil {
				obs.DoLog(obs.LEVEL_ERROR, "Failed to write to file. error: %v", werr)
				return werr
			}
			if wcnt != readCount {
				obs.DoLog(obs.LEVEL_ERROR,
					"Failed to write to file. filePath: %s, expect: %d, actual: %d",
					filePath, readCount, wcnt)
				return fmt.Errorf(
					"failed to write to file. filePath: %s, expect: %d, actual: %d",
					filePath, readCount, wcnt)
			}
			readTotal = readTotal + readCount
		}
		if readErr != nil {
			if readErr != io.EOF {
				obs.DoLog(obs.LEVEL_ERROR,
					"Failed to read response body. error: %v", readErr)
				return readErr
			}
			break
		}
	}
	err = fileWriter.Flush()
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Failed to flush file. error: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "updateDownloadFile success. readTotal: %d", readTotal)
	return nil
}
