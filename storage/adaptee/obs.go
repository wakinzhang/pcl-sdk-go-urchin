package adaptee

import (
	"encoding/xml"
	"fmt"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"io"
	"net/http"
	"os"
	"path/filepath"
	. "pcl-sdk-go-urchin/storage/common"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ObsAdapteeWithAuth struct {
	obsClient *obs.ObsClient
}

func (o *ObsAdapteeWithAuth) Init(accessKey, secretKey, endPoint string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithAuth:Init start."+
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

	obs.DoLog(
		obs.LEVEL_DEBUG,
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
		obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
		return signedUrl, nil, nil
	}

	obs.DoLog(
		obs.LEVEL_DEBUG,
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
		obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
		return signedUrl, nil, nil
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithAuth:CreateUploadPartSignedUrl finish.")
	return output.SignedUrl, output.ActualSignedRequestHeaders, nil
}

func (o *ObsAdapteeWithAuth) CreateCompleteMultipartUploadSignedUrl(
	bucketName, objectKey, uploadId string, expires int) (
	signedUrl string, header http.Header, err error) {

	obs.DoLog(
		obs.LEVEL_DEBUG,
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
		obs.DoLog(obs.LEVEL_ERROR, "obsClient.CreateSignedUrl failed. err: %v", err)
		return signedUrl, nil, nil
	}

	obs.DoLog(
		obs.LEVEL_DEBUG,
		"ObsAdapteeWithAuth:CreateCompleteMultipartUploadSignedUrl finish.")
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

func (o *ObsAdapteeWithSignedUrl) Upload(path string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Upload start. path: %s", path)

	defer func() {
		if err := recover(); err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "Upload failed. err: %v", err)
		}
	}()

	stat, err := os.Stat(path)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "os.Stat failed. path: %s, err: %v", path, err)
		return err
	}
	if stat.IsDir() {
		err = o.uploadFolder(path)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"uploadFolder failed. path: %s, err: %v", path, err)
			return err
		}
	} else {
		err = o.uploadFile(path)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"uploadFile failed. path: %s, err: %v", path, err)
			return err
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:Upload finish. path: %s", path)
	return nil
}

func (o *ObsAdapteeWithSignedUrl) uploadFile(sourceFile string) (err error) {

	obs.DoLog(
		obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:uploadFile start. sourceFile: %s", sourceFile)

	initiateMultipartUploadSignedUrl, initiateMultipartUploadHeader, err :=
		o.createInitiateMultipartUploadSignedUrl()
	if err != nil {
		obs.DoLog(
			obs.LEVEL_ERROR,
			"createInitiateMultipartUploadSignedUrl failed. err: %v", err)
		return err
	}
	uploadId, err := o.initiateMultipartUploadWithSignedUrl(
		initiateMultipartUploadSignedUrl,
		initiateMultipartUploadHeader)
	if err != nil {
		obs.DoLog(
			obs.LEVEL_ERROR,
			"initiateMultipartUploadWithSignedUrl failed. err: %v", err)
		return err
	}
	err = o.uploadPartWithSignedUrl(sourceFile, uploadId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "uploadPartWithSignedUrl failed. err: %v", err)
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
		obs.DoLog(obs.LEVEL_ERROR,
			"obsClient.InitiateMultipartUploadWithSignedUrl failed."+
				" signedUrl: %s, err: %v", signedUrl, err)
		return uploadId, err
	}

	obs.DoLog(obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:initiateMultipartUploadWithSignedUrl finish."+
			" signedUrl: %s, uploadId: %s",
		signedUrl, output.UploadId)

	return output.UploadId, nil
}

func (o *ObsAdapteeWithSignedUrl) uploadPartWithSignedUrl(
	sourceFile, uploadId string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:uploadPartWithSignedUrl start."+
		" sourceFile: %s, uploadId: %s",
		sourceFile, uploadId)

	var partSize int64 = 100 * 1024 * 1024
	stat, err := os.Stat(sourceFile)
	if err != nil {
		obs.DoLog(
			obs.LEVEL_ERROR,
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
					obs.DoLog(
						obs.LEVEL_WARN,
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

			uploadPartSignedUrl, uploadPartHeader, err :=
				o.createUploadPartSignedUrl(uploadId, strconv.Itoa(partNumber))
			if err != nil {
				obs.DoLog(
					obs.LEVEL_ERROR,
					"createUploadPartSignedUrl failed. err: %v", err)
				return
			}

			uploadPartInputOutput, err := o.obsClient.UploadPartWithSignedUrl(
				uploadPartSignedUrl,
				uploadPartHeader,
				readerWrapper)

			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR, "obsClient.UploadPartWithSignedUrl failed."+
					" uploadPartSignedUrl: %s, sourceFile: %s, partNumber: %d,"+
					" offset: %d, currPartSize: %d, err: %v",
					uploadPartSignedUrl, sourceFile, partNumber, offset, currPartSize, err)
				return
			}
			obs.DoLog(obs.LEVEL_INFO, "obsClient.UploadPartWithSignedUrl success."+
				" uploadPartSignedUrl: %s, sourceFile: %s, partNumber: %d,"+
				" offset: %d, currPartSize: %d, ETag: %s",
				uploadPartSignedUrl, sourceFile, partNumber,
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
	completeMultipartUploadSignedUrl, completeMultipartUploadHeader, err :=
		o.createCompleteMultipartUploadSignedUrl(uploadId)
	if err != nil {
		obs.DoLog(
			obs.LEVEL_ERROR,
			"createCompleteMultipartUploadSignedUrl failed. err: %v", err)
		return
	}

	var partSlice PartSlice = parts
	sort.Sort(partSlice)

	var completeMultipartUploadPart CompleteMultipartUploadPart
	completeMultipartUploadPart.PartSlice = partSlice
	completeMultipartUploadPartXML, err :=
		xml.MarshalIndent(completeMultipartUploadPart, "", "  ")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	completeMultipartUploadOutput, err :=
		o.obsClient.CompleteMultipartUploadWithSignedUrl(
			completeMultipartUploadSignedUrl,
			completeMultipartUploadHeader,
			strings.NewReader(string(completeMultipartUploadPartXML)))
	if err != nil {
		obs.DoLog(
			obs.LEVEL_ERROR,
			"obsClient.CompleteMultipartUploadWithSignedUrl failed. err: %v", err)
		return
	}
	obs.DoLog(
		obs.LEVEL_INFO,
		"obsClient.CompleteMultipartUploadWithSignedUrl success. requestId: %s",
		completeMultipartUploadOutput.RequestId)

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:uploadPartWithSignedUrl finish.")
	return
}

func (o *ObsAdapteeWithSignedUrl) uploadFolder(dirPath string) (err error) {

	obs.DoLog(
		obs.LEVEL_DEBUG,
		"ObsAdapteeWithSignedUrl:uploadFolder start. dirPath: %s", dirPath)

	input := &obs.PutObjectInput{}
	// 创建文件夹
	output, err := o.obsClient.PutObject(input)
	if err == nil {
		obs.DoLog(
			obs.LEVEL_INFO,
			"obsClient.PutObject success. requestId: %s",
			output.RequestId)
	} else if obsError, ok := err.(obs.ObsError); ok {
		obs.DoLog(
			obs.LEVEL_ERROR,
			"obsClient.PutObject failed. code: %s, message: %s, err: %v",
			obsError.Code, obsError.Message, err)
		return err
	}

	var wg sync.WaitGroup
	err = filepath.Walk(dirPath, func(filePath string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			obs.DoLog(
				obs.LEVEL_ERROR,
				"filepath.Walk failed. dirPath: %s, err: %v",
				dirPath, err)
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
				err = o.uploadFile(filePath)
				if err != nil {
					obs.DoLog(
						obs.LEVEL_ERROR,
						"uploadFile failed. path: %s, err: %v", filePath, err)
				}
			}()
			obs.DoLog(obs.LEVEL_INFO, "uploadFile success. filePath: %s", filePath)
		}
		return nil
	})
	wg.Wait()

	if err != nil {
		obs.DoLog(
			obs.LEVEL_ERROR,
			"UploadFolder failed. dirPath: %s, err: %v", dirPath, err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "ObsAdapteeWithSignedUrl:uploadFolder finish.")
	return nil
}

func (o *ObsAdapteeWithSignedUrl) Download() {
}

func (o *ObsAdapteeWithSignedUrl) Migrate() {
}
