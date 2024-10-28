package adaptee

import (
	"bufio"
	"encoding/xml"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

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
	input.Key = objectKey + "/"
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

func (o *ObsAdapteeWithAuth) CreateGetObjectSignedUrl(
	bucketName, objectKey string, expires int) (
	signedUrl string, header http.Header, err error) {

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

func (o *ObsAdapteeWithAuth) CreateListObjectSignedUrl(
	bucketName, prefix string, expires int) (
	signedUrl string, header http.Header, err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateListObjectSignedUrl start. "+
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
		"ObsAdapteeWithAuth:CreateListObjectSignedUrl finish.")
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

	defer func() {
		if err := recover(); err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "Upload failed. err: %v", err)
		}
	}()

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
		err = o.uploadFile(urchinServiceAddr, sourcePath, taskId)
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
	urchinServiceAddr, sourceFile string, taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:uploadFile start."+
			" urchinServiceAddr: %s, sourceFile: %s, taskId: %d",
		urchinServiceAddr, sourceFile, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createInitiateMultipartUploadSignedUrlReq := new(CreateInitiateMultipartUploadSignedUrlReq)
	createInitiateMultipartUploadSignedUrlReq.TaskId = taskId

	err, createInitiateMultipartUploadSignedUrlResp :=
		urchinService.CreateInitiateMultipartUploadSignedUrl(
			ConfigDefaultUrchinServiceCreateInitiateMultipartUploadSignedUrlInterface,
			createInitiateMultipartUploadSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateInitiateMultipartUploadSignedUrl failed. err: %v", err)
		return err
	}
	var initiateMultipartUploadWithSignedUrlHeader http.Header
	for key, item := range createInitiateMultipartUploadSignedUrlResp.Header {
		for _, value := range item.Values {
			initiateMultipartUploadWithSignedUrlHeader.Add(key, value)
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
	err = o.uploadPartWithSignedUrl(urchinServiceAddr, sourceFile, uploadId, taskId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "uploadPartWithSignedUrl failed."+
			"addr: %s, sourceFile: %s, uploadId: %s, err: %v",
			urchinServiceAddr, sourceFile, uploadId, err)
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
	addr, sourceFile, uploadId string, taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:uploadPartWithSignedUrl start."+
			" addr: %s, sourceFile: %s, uploadId: %s", addr, sourceFile, uploadId)

	var partSize int64 = 100 * 1024 * 1024
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
	urchinService.Init(addr, 10, 10)

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
			err, createUploadPartSignedUrlResp :=
				urchinService.CreateUploadPartSignedUrl(
					ConfigDefaultUrchinServiceCreateUploadPartSignedUrlInterface,
					createUploadPartSignedUrlReq)
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"CreateUploadPartSignedUrl failed. err: %v", err)
				return
			}
			var uploadPartWithSignedUrlHeader http.Header
			for key, item := range createUploadPartSignedUrlResp.Header {
				for _, value := range item.Values {
					uploadPartWithSignedUrlHeader.Add(key, value)
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
							" signedUrl: %s, sourceFile: %s, partNumber: %d, offset: %d,"+
							" currPartSize: %d, obsCode: %s, obsMessage: %s",
						createUploadPartSignedUrlResp.SignedUrl,
						sourceFile, partNumber, offset, currPartSize,
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
				" signedUrl: %s, sourceFile: %s, partNumber: %d,"+
				" offset: %d, currPartSize: %d, ETag: %s",
				createUploadPartSignedUrlResp.SignedUrl, sourceFile, partNumber,
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

	var completeMultipartUploadWithSignedUrlHeader http.Header
	for key, item := range createCompleteMultipartUploadSignedUrlResp.Header {
		for _, value := range item.Values {
			completeMultipartUploadWithSignedUrlHeader.Add(key, value)
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

	err, createNewFolderSignedUrlResp :=
		urchinService.CreateNewFolderSignedUrl(
			ConfigDefaultUrchinServiceCreateNewFolderSignedUrlInterface)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "CreateNewFolderSignedUrl failed. err: %v", err)
		return err
	}
	var newFolderWithSignedUrlHeader http.Header
	for key, item := range createNewFolderSignedUrlResp.Header {
		for _, value := range item.Values {
			newFolderWithSignedUrlHeader.Add(key, value)
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
				err = o.uploadFile(urchinServiceAddr, filePath, taskId)
				if err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"uploadFile failed. urchinServiceAddr: %s, path: %s, err: %v",
						urchinServiceAddr, filePath, err)
				}
			}()
			obs.DoLog(obs.LEVEL_INFO,
				"uploadFile success. urchinServiceAddr: %s, filePath: %s",
				urchinServiceAddr, filePath)
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

func (o *ObsAdapteeWithSignedUrl) Download() {
	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Download start.")
	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Download finish.")
}

func (o *ObsAdapteeWithSignedUrl) downloadFile(addr, targetFile string) (err error) {
	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:downloadFile start."+
			" addr: %s, targetFile: %s", addr, targetFile)

	urchinService := new(UrchinService)
	urchinService.Init(addr, 10, 10)

	err, createGetObjectSignedUrlResp :=
		urchinService.CreateGetObjectSignedUrl(
			ConfigDefaultUrchinServiceCreateGetObjectSignedUrlInterface)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "CreateGetObjectSignedUrl failed. err: %v", err)
		return err
	}
	var getObjectWithSignedUrlHeader http.Header
	for key, item := range createGetObjectSignedUrlResp.Header {
		for _, value := range item.Values {
			getObjectWithSignedUrlHeader.Add(key, value)
		}
	}

	getObjectOutput, err := o.obsClient.GetObjectWithSignedUrl(
		createGetObjectSignedUrlResp.SignedUrl,
		getObjectWithSignedUrlHeader)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.GetObjectWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.GetObjectWithSignedUrl failed. err: %v", err)
			return err
		}
	} else {
		defer func() {
			if err := getObjectOutput.Body.Close(); err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"GetObjectOutput.Body.Close failed. err: %v", err)
			}
		}()
		obs.DoLog(obs.LEVEL_DEBUG, "obsClient.GetObjectWithSignedUrl success. "+
			" SignedUrl: %s StorageClass:%s, ETag:%s, ContentType:%s,"+
			" ContentLength:%d, LastModified:%s",
			createGetObjectSignedUrlResp.SignedUrl,
			getObjectOutput.StorageClass, getObjectOutput.ETag, getObjectOutput.ContentType,
			getObjectOutput.ContentLength, getObjectOutput.LastModified)

		fl, err := os.Create(targetFile)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"Create target file failed. targetFile: %s, err: %v", targetFile, err)
			return err
		}
		defer func() {
			if err := fl.Close(); err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"File.Close failed. err: %v", err)
			}
		}()

		writer := bufio.NewWriter(fl)

		// 读取对象内容
		p := make([]byte, 1024)
		var readCount int
		for {
			readCount, err = getObjectOutput.Body.Read(p)
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"Read failed. targetFile: %s, err: %v", targetFile, err)
				return err
			}
			if readCount > 0 {
				_, err = writer.WriteString(string(p[:readCount]))
				if err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"writer.WriteString failed. targetFile: %s, err: %v",
						targetFile, err)
					return err
				}
				err = writer.Flush()
				if err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"writer.Flush failed. targetFile: %s, err: %v", targetFile, err)
					return err
				}
			} else {
				break
			}
		}
		obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:downloadFile finish.")
		return nil
	}
}

func (o *ObsAdapteeWithSignedUrl) downloadFolder(addr, prefix string) (err error) {
	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:downloadFolder start. addr: %s, prefix: %s",
		addr, prefix)

	urchinService := new(UrchinService)
	urchinService.Init(addr, 10, 10)

	createListObjectsSignedUrlReq := new(CreateListObjectsSignedUrlReq)
	createListObjectsSignedUrlReq.Prefix = prefix
	err, createListObjectsSignedUrlResp :=
		urchinService.CreateListObjectsSignedUrl(
			ConfigDefaultUrchinServiceCreateListObjectsSignedUrlInterface,
			createListObjectsSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateListObjectSignedUrl failed. err: %v", err)
		return err
	}

	var listObjectsWithSignedUrlHeader http.Header
	for key, item := range createListObjectsSignedUrlResp.Header {
		for _, value := range item.Values {
			listObjectsWithSignedUrlHeader.Add(key, value)
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
		go func() {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					obs.DoLog(obs.LEVEL_ERROR, "downloadFile failed. err: %v", err)
				}
			}()
			err = o.downloadFile(addr, "targetFile")
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"downloadFile failed."+
						" addr: %s, objectKey: %s, targetFile: %s, err: %v",
					addr, object.Key, "targetFile", err)
			}
		}()
		obs.DoLog(obs.LEVEL_INFO,
			"downloadFile success. addr: %s, objectKey: %s, targetFile: %s",
			addr, object.Key, "targetFile")
	}
	wg.Wait()

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:downloadFolder finish.")
	return nil
}

func (o *ObsAdapteeWithSignedUrl) Migrate() {
	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Migrate start.")
	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Migrate finish.")
}
