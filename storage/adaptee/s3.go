package adaptee

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/panjf2000/ants/v2"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
	"io"
	"io/ioutil"
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

func (o *S3) Init() (err error) {
	obs.DoLog(obs.LEVEL_DEBUG, "S3:Init start.")

	o.obsClient, err = obs.New("", "", "magicalParam")
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "obs.New failed. err: %v", err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "S3:Init finish.")
	return nil
}

func (o *S3) NewFolder(
	urchinServiceAddr, objectKey string,
	taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"S3:NewFolder start. urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createEmptyFileSignedUrlReq := new(CreateNewFolderSignedUrlReq)
	createEmptyFileSignedUrlReq.TaskId = taskId
	createEmptyFileSignedUrlReq.Source = &objectKey

	err, createEmptyFileSignedUrlResp :=
		urchinService.CreateNewFolderSignedUrl(
			createEmptyFileSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "CreateEmptyFileSignedUrl failed."+
			" objectKey:%s, err: %v", objectKey, err)
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
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.PutObjectWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.PutObjectWithSignedUrl failed. error: %v", err)
			return err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:NewFolder finish.")
	return err
}

func (o *S3) InitiateMultipartUpload(
	urchinServiceAddr, objectKey string,
	taskId int32) (output *obs.InitiateMultipartUploadOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:InitiateMultipartUpload start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createInitiateMultipartUploadSignedUrlReq :=
		new(CreateInitiateMultipartUploadSignedUrlReq)
	createInitiateMultipartUploadSignedUrlReq.TaskId = taskId
	createInitiateMultipartUploadSignedUrlReq.Source = objectKey

	err, createInitiateMultipartUploadSignedUrlResp :=
		urchinService.CreateInitiateMultipartUploadSignedUrl(
			createInitiateMultipartUploadSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateInitiateMultipartUploadSignedUrl failed. err: %v", err)
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
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.InitiateMultipartUploadWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return output, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.InitiateMultipartUploadWithSignedUrl failed."+
					" signedUrl: %s, err: %v",
				createInitiateMultipartUploadSignedUrlResp.SignedUrl, err)
			return output, err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG,
		"S3:InitiateMultipartUpload finish.")

	return output, err
}

func (o *S3) UploadPartWithSignedUrl(
	urchinServiceAddr,
	sourceFile,
	objectKey,
	uploadId string,
	taskId int32) (partSlice PartSlice, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:UploadPartWithSignedUrl start."+
		" urchinServiceAddr: %s, sourceFile: %s, objectKey: %s, uploadId: %s",
		urchinServiceAddr, sourceFile, objectKey, uploadId)

	var partSize int64 = DefaultPartSize
	stat, err := os.Stat(sourceFile)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. sourceFile: %s, err: %v", sourceFile, err)
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

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	pool, err := ants.NewPool(DefaultS3UploadMultiTaskNum)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"ants.NewPool for upload part failed. err: %v", err)
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
			if _err != nil {
				err = _err
				panic(err)
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
					createUploadPartSignedUrlReq)
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"CreateUploadPartSignedUrl failed. err: %v", err)
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
				} else {
					obs.DoLog(obs.LEVEL_ERROR,
						"obsClient.UploadPartWithSignedUrl failed."+
							" signedUrl: %s, sourceFile: %s, partNumber: %d,"+
							" offset: %d, currPartSize: %d, err: %v",
						createUploadPartSignedUrlResp.SignedUrl,
						sourceFile, partNumber, offset, currPartSize, err)
				}
				isGlobalSuccess = false
				partChan <- XPart{
					PartNumber: partNumber}
			}
			obs.DoLog(obs.LEVEL_INFO,
				"obsClient.UploadPartWithSignedUrl success."+
					" signedUrl: %s, sourceFile: %s, objectKey: %s, partNumber: %d,"+
					" offset: %d, currPartSize: %d, ETag: %s",
				createUploadPartSignedUrlResp.SignedUrl,
				sourceFile, objectKey, partNumber,
				offset, currPartSize,
				strings.Trim(uploadPartOutput.ETag, "\""))
			partChan <- XPart{
				ETag:       strings.Trim(uploadPartOutput.ETag, "\""),
				PartNumber: partNumber}
		})
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"ants.Submit for upload part failed. err: %v", err)
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
		obs.DoLog(obs.LEVEL_ERROR,
			"S3:uploadPartWithSignedUrl some part failed."+
				" urchinServiceAddr: %s, sourceFile: %s,"+
				" objectKey: %s, uploadId: %s",
			urchinServiceAddr, sourceFile, objectKey, uploadId)
		return partSlice, errors.New("some part upload failed")
	}

	obs.DoLog(obs.LEVEL_DEBUG,
		"S3:UploadPartWithSignedUrl finish.")
	return partSlice, nil
}

func (o *S3) CompleteMultipartUpload(
	urchinServiceAddr,
	objectKey,
	uploadId string,
	taskId int32,
	partSlice PartSlice) (output *obs.CompleteMultipartUploadOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:CompleteMultipartUpload start."+
		" urchinServiceAddr: %s, objectKey: %s, uploadId: %s",
		urchinServiceAddr, objectKey, uploadId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	// 合并段
	createCompleteMultipartUploadSignedUrlReq :=
		new(CreateCompleteMultipartUploadSignedUrlReq)
	createCompleteMultipartUploadSignedUrlReq.UploadId = uploadId
	createCompleteMultipartUploadSignedUrlReq.TaskId = taskId
	createCompleteMultipartUploadSignedUrlReq.Source = objectKey

	err, createCompleteMultipartUploadSignedUrlResp :=
		urchinService.CreateCompleteMultipartUploadSignedUrl(
			createCompleteMultipartUploadSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateCompleteMultipartUploadSignedUrl failed. err: %v", err)
		return output, err
	}

	var completeMultipartUploadPart = new(CompleteMultipartUploadPart)
	completeMultipartUploadPart.PartSlice = make([]XPart, 0)
	completeMultipartUploadPart.PartSlice = partSlice
	completeMultipartUploadPartXML, err := xml.MarshalIndent(
		completeMultipartUploadPart,
		"",
		"  ")
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "xml.MarshalIndent failed. err: %v", err)
		return output, err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "completeMultipartUploadPartXML content: \n%s",
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
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.CompleteMultipartUploadWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return output, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.CompleteMultipartUploadWithSignedUrl failed."+
					" err: %v", err)
			return output, err
		}
	}
	obs.DoLog(obs.LEVEL_INFO,
		"obsClient.CompleteMultipartUpload success. requestId: %s",
		output.RequestId)

	obs.DoLog(obs.LEVEL_DEBUG,
		"S3:CompleteMultipartUpload finish.")
	return output, nil
}

func (o *S3) AbortMultipartUpload(
	urchinServiceAddr,
	objectKey,
	uploadId string,
	taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:AbortMultipartUpload start."+
		" urchinServiceAddr: %s, objectKey: %s, uploadId: %s",
		urchinServiceAddr, objectKey, uploadId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createAbortMultipartUploadSignedUrlReq :=
		new(CreateAbortMultipartUploadSignedUrlReq)
	createAbortMultipartUploadSignedUrlReq.UploadId = uploadId
	createAbortMultipartUploadSignedUrlReq.TaskId = taskId
	createAbortMultipartUploadSignedUrlReq.Source = objectKey

	err, createAbortMultipartUploadSignedUrlResp :=
		urchinService.CreateAbortMultipartUploadSignedUrl(
			createAbortMultipartUploadSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateAbortMultipartUploadSignedUrl failed. err: %v", err)
		return err
	}

	var abortMultipartUploadWithSignedUrlHeader = http.Header{}
	for key, item := range createAbortMultipartUploadSignedUrlResp.Header {
		for _, value := range item.Values {
			abortMultipartUploadWithSignedUrlHeader.Set(key, value)
		}
	}
	abortMultipartUploadOutput, err :=
		o.obsClient.AbortMultipartUploadWithSignedUrl(
			createAbortMultipartUploadSignedUrlResp.SignedUrl,
			abortMultipartUploadWithSignedUrlHeader)
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.AbortMultipartUploadWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
			return err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.AbortMultipartUploadWithSignedUrl failed."+
					" err: %v", err)
			return err
		}
	}
	obs.DoLog(obs.LEVEL_INFO,
		"obsClient.AbortMultipartUpload success. requestId: %s",
		abortMultipartUploadOutput.RequestId)

	obs.DoLog(obs.LEVEL_DEBUG,
		"S3:AbortMultipartUpload finish.")
	return
}

func (o *S3) GetObjectInfoWithSignedUrl(
	urchinServiceAddr, objectKey string, taskId int32) (
	getObjectmetaOutput *obs.GetObjectMetadataOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:GetObjectInfoWithSignedUrl start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createGetObjectMetadataSignedUrlReq := new(CreateGetObjectMetadataSignedUrlReq)
	createGetObjectMetadataSignedUrlReq.TaskId = taskId
	createGetObjectMetadataSignedUrlReq.Source = objectKey

	err, createGetObjectMetadataSignedUrlResp :=
		urchinService.CreateGetObjectMetadataSignedUrl(
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
	obs.DoLog(obs.LEVEL_INFO, "S3.GetObjectInfoWithSignedUrl success.")
	return
}

func (o *S3) ListObjectsWithSignedUrl(
	urchinServiceAddr string,
	taskId int32) (listObjectsOutput *obs.ListObjectsOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:ListObjectsWithSignedUrl start."+
		" urchinServiceAddr: %s taskId: %d", urchinServiceAddr, taskId)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createListObjectsSignedUrlReq := new(CreateListObjectsSignedUrlReq)
	createListObjectsSignedUrlReq.TaskId = taskId

	err, createListObjectsSignedUrlResp :=
		urchinService.CreateListObjectsSignedUrl(
			createListObjectsSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateListObjectSignedUrl failed. err: %v", err)
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
	if err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.ListObjectsWithSignedUrl failed."+
					" obsCode: %s, obsMessage: %s",
				obsError.Code, obsError.Message)
			return listObjectsOutput, err
		} else {
			obs.DoLog(obs.LEVEL_ERROR,
				"obsClient.CompleteMultipartUploadWithSignedUrl failed."+
					" err: %v", err)
			return listObjectsOutput, err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:ListObjectsWithSignedUrl finish.")
	return listObjectsOutput, nil
}

func (o *S3) loadCheckpointFile(checkpointFile string, result interface{}) error {
	obs.DoLog(obs.LEVEL_DEBUG, "loadCheckpointFile start.")
	ret, err := ioutil.ReadFile(checkpointFile)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"io util.ReadFile failed."+
				" checkpointFile: %s, err: %v", checkpointFile, err)
		return err
	}
	if len(ret) == 0 {
		obs.DoLog(obs.LEVEL_DEBUG, "loadCheckpointFile nil.")
		return nil
	}
	obs.DoLog(obs.LEVEL_DEBUG, "loadCheckpointFile success.")
	return xml.Unmarshal(ret, result)
}

func (o *S3) sliceObject(objectSize, partSize int64, dfc *DownloadCheckpoint) {
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

func (o *S3) updateCheckpointFile(fc interface{}, checkpointFilePath string) error {
	obs.DoLog(obs.LEVEL_DEBUG, "updateCheckpointFile start.")
	result, err := xml.Marshal(fc)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "xml.Marshal failed. error: %v", err)
		return err
	}
	err = ioutil.WriteFile(checkpointFilePath, result, 0640)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"io util.WriteFile failed."+
				" checkpointFilePath: %s err: %v", checkpointFilePath, err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "updateCheckpointFile finish.")
	return err
}

func (o *S3) Upload(
	urchinServiceAddr, sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"S3:Upload start."+
			" urchinServiceAddr: %s, sourcePath: %s, taskId: %d, needPure: %t",
		urchinServiceAddr, sourcePath, taskId, needPure)

	stat, err := os.Stat(sourcePath)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. urchinServiceAddr: %s sourcePath: %s, err: %v",
			urchinServiceAddr, sourcePath, err)
		return err
	}
	var isDir = false
	if stat.IsDir() {
		isDir = true
		err = o.uploadFolder(urchinServiceAddr, sourcePath, taskId, needPure)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"uploadFolder failed. urchinServiceAddr: %s, sourcePath: %s,"+
					" taskId: %d, err: %v",
				urchinServiceAddr, sourcePath, taskId, err)
			return err
		}
	} else {
		objectKey := filepath.Base(sourcePath)
		//err = o.uploadFile(urchinServiceAddr, sourcePath, objectKey, taskId)
		_, err = o.uploadFileResume(
			urchinServiceAddr,
			sourcePath,
			objectKey,
			taskId,
			needPure)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"uploadFileResume failed."+
					" urchinServiceAddr: %s, sourcePath: %s, taskId: %d, err: %v",
				urchinServiceAddr, sourcePath, taskId, err)
			return err
		}
	}

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := urchinService.GetTask(getTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. err: %v", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		obs.DoLog(obs.LEVEL_ERROR, "task not exist. taskId: %d", taskId)
		return errors.New("task not exist")
	}

	task := getTaskResp.Data.List[0].Task
	if TaskTypeMigrate == task.Type {
		migrateObjectTaskParams := new(MigrateObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), migrateObjectTaskParams)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"MigrateObjectTaskParams Unmarshal failed."+
					" params: %s, error: %v", task.Params, err)
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

		err, _ = urchinService.PutObjectDeployment(putObjectDeploymentReq)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"PutObjectDeployment failed. err: %v", err)
			return err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:Upload finish.")
	return nil
}

func (o *S3) uploadFolder(
	urchinServiceAddr, dirPath string,
	taskId int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadFolder start."+
		" urchinServiceAddr: %s, dirPath: %s, taskId: %d, needPure: %t",
		urchinServiceAddr, dirPath, taskId, needPure)

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	obs.DoLog(obs.LEVEL_DEBUG, "uploadFolderRecord info."+
		" filepath.Dir(dirPath): %s, filepath.Base(dirPath): %s",
		filepath.Dir(dirPath), filepath.Base(dirPath))

	uploadFolderRecord :=
		filepath.Dir(dirPath) + "/" + filepath.Base(dirPath) + ".upload_folder_record"

	if needPure {
		err = os.Remove(uploadFolderRecord)
		if nil != err {
			if !os.IsNotExist(err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. uploadFolderRecord: %s, err: %v",
					uploadFolderRecord, err)
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
			obs.DoLog(obs.LEVEL_ERROR,
				"os.ReadFile failed. uploadFolderRecord: %s, err: %v",
				uploadFolderRecord, err)
			return err
		}
	}

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createNewFolderSignedUrlReq := new(CreateNewFolderSignedUrlReq)
	createNewFolderSignedUrlReq.TaskId = taskId

	err, createNewFolderSignedUrlResp :=
		urchinService.CreateNewFolderSignedUrl(
			createNewFolderSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateNewFolderSignedUrl failed. err: %v", err)
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

	pool, err := ants.NewPool(DefaultS3UploadFileTaskNum)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"ants.NewPool for upload file failed. err: %v", err)
		return err
	}
	defer pool.Release()

	var isAllSuccess = true
	var wg sync.WaitGroup
	err = filepath.Walk(
		dirPath, func(filePath string, fileInfo os.FileInfo, err error) error {
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"filepath.Walk failed."+
						" urchinServiceAddr: %s, dirPath: %s, err: %v",
					urchinServiceAddr, dirPath, err)
				return err
			}
			if !fileInfo.IsDir() {
				wg.Add(1)
				// 处理文件
				err = pool.Submit(func() {
					defer func() {
						wg.Done()
						if err := recover(); err != nil {
							obs.DoLog(obs.LEVEL_ERROR,
								"uploadFile failed. err: %v", err)
							isAllSuccess = false
						}
					}()
					objectKey, err := filepath.Rel(dirPath, filePath)
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"filepath.Rel failed."+
								" urchinServiceAddr: %s, dirPath: %s,"+
								" filePath: %s, objectKey: %s, err: %v",
							urchinServiceAddr, dirPath, filePath, objectKey, err)
						return
					}
					if _, exists := fileMap[objectKey]; exists {
						obs.DoLog(obs.LEVEL_INFO,
							"file already success. objectKey: %s", objectKey)
						return
					}
					//err = o.uploadFile(urchinServiceAddr, filePath, objectKey, taskId)
					_, err = o.uploadFileResume(
						urchinServiceAddr,
						filePath,
						objectKey,
						taskId,
						needPure)
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"uploadFileResume failed."+
								" urchinServiceAddr: %s, filePath: %s,"+
								" objectKey: %s, err: %v",
							urchinServiceAddr, filePath, objectKey, err)
					}
					fileMutex.Lock()
					defer fileMutex.Unlock()
					f, err := os.OpenFile(
						uploadFolderRecord,
						os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"os.OpenFile failed."+
								" uploadFolderRecord: %s, err: %v",
							uploadFolderRecord, err)
						return
					}
					defer func() {
						errMsg := f.Close()
						if errMsg != nil {
							obs.DoLog(obs.LEVEL_WARN,
								"Failed to close file. error: %v", errMsg)
						}
					}()
					_, err = f.Write([]byte(objectKey + "\n"))
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"Write file failed."+
								" uploadFolderRecord: %s, file item: %s, err: %v",
							uploadFolderRecord, objectKey, err)
						return
					}
					return
				})
				if err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"ants.Submit for upload file failed. err: %v", err)
					return err
				}
			}
			return nil
		})
	wg.Wait()

	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"filepath.Walk failed. dirPath: %s, err: %v", dirPath, err)
		return err
	}
	if !isAllSuccess {
		obs.DoLog(obs.LEVEL_ERROR,
			"S3:uploadFolder not all success. dirPath: %s", dirPath)
		return errors.New("uploadFolder not all success")
	} else {
		_err := os.Remove(uploadFolderRecord)
		if nil != _err {
			if !os.IsNotExist(_err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. uploadFolderRecord: %s, err: %v",
					uploadFolderRecord, _err)
			}
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadFolder finish.")
	return nil
}

func (o *S3) uploadFile(
	urchinServiceAddr, sourceFile, objectKey string, taskId int32) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadFile start."+
		" urchinServiceAddr: %s, sourceFile: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, sourceFile, objectKey, taskId)

	stat, err := os.Stat(sourceFile)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. sourceFile: %s, err: %v", sourceFile, err)
		return
	}
	fileSize := stat.Size()

	if 0 == fileSize {
		err = o.NewFolder(urchinServiceAddr, objectKey, taskId)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "NewFolder failed. err: %v", err)
			return err
		}
		obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadFile finish.")
		return nil
	}

	initiateMultipartUploadOutput, err := o.InitiateMultipartUpload(
		urchinServiceAddr,
		objectKey,
		taskId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"InitiateMultipartUpload failed. err: %v", err)
		return err
	}
	partSlice, err := o.UploadPartWithSignedUrl(
		urchinServiceAddr,
		sourceFile,
		objectKey,
		initiateMultipartUploadOutput.UploadId,
		taskId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "UploadPartWithSignedUrl failed."+
			" urchinServiceAddr: %s, sourceFile: %s,"+
			" objectKey: %s, uploadId: %s, err: %v",
			urchinServiceAddr, sourceFile, objectKey,
			initiateMultipartUploadOutput.UploadId, err)
		return err
	}

	_, err = o.CompleteMultipartUpload(
		urchinServiceAddr,
		objectKey,
		initiateMultipartUploadOutput.UploadId,
		taskId,
		partSlice)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "CompleteMultipartUpload failed."+
			" urchinServiceAddr: %s, sourceFile: %s,"+
			" objectKey: %s, uploadId: %s, err: %v",
			urchinServiceAddr, sourceFile, objectKey,
			initiateMultipartUploadOutput.UploadId, err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadFile finish.")
	return nil
}

func (o *S3) uploadFileResume(
	urchinServiceAddr, sourceFile, objectKey string,
	taskId int32,
	needPure bool) (
	output *obs.CompleteMultipartUploadOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadFileResume start."+
		" urchinServiceAddr: %s, sourceFile: %s, objectKey: %s, taskId: %d, needPure: %t",
		urchinServiceAddr, sourceFile, objectKey, taskId, needPure)

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
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. CheckpointFile: %s, err: %v",
					uploadFileInput.CheckpointFile, err)
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
		urchinServiceAddr,
		sourceFile,
		objectKey,
		taskId,
		uploadFileInput)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR, "S3:resumeUpload failed. err: %v", err)
		return output, err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadFileResume finish.")
	return output, err
}

func (o *S3) resumeUpload(
	urchinServiceAddr, sourceFile, objectKey string,
	taskId int32,
	input *obs.UploadFileInput) (
	output *obs.CompleteMultipartUploadOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:resumeUpload start."+
		" urchinServiceAddr: %s, sourceFile: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, sourceFile, objectKey, taskId)

	uploadFileStat, err := os.Stat(input.UploadFile)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "os.Stat failed."+
			" UploadFile: %s, err: %v", input.UploadFile, err)
		return nil, err
	}
	if uploadFileStat.IsDir() {
		obs.DoLog(obs.LEVEL_ERROR, "uploadFile can not be a folder."+
			" UploadFile: %s", input.UploadFile)
		return nil, errors.New("uploadFile can not be a folder")
	}

	ufc := &UploadCheckpoint{}

	var needCheckpoint = true
	var checkpointFilePath = input.CheckpointFile
	var enableCheckpoint = input.EnableCheckpoint
	if enableCheckpoint {
		needCheckpoint, err = o.getUploadCheckpointFile(
			urchinServiceAddr,
			objectKey,
			taskId,
			ufc,
			uploadFileStat,
			input)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"S3:getUploadCheckpointFile failed. err: %v", err)
			return nil, err
		}
	}
	if needCheckpoint {
		err = o.prepareUpload(
			urchinServiceAddr,
			objectKey,
			taskId,
			ufc,
			uploadFileStat,
			input)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "S3:prepareUpload failed. err: %v", err)
			return nil, err
		}

		if enableCheckpoint {
			err = o.updateCheckpointFile(ufc, checkpointFilePath)
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"updateCheckpointFile failed. err: %v", err)
				_err := o.AbortMultipartUpload(
					urchinServiceAddr,
					objectKey,
					ufc.UploadId,
					taskId)
				if _err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"AbortMultipartUpload failed."+
							" uploadId: %s, err: %v", ufc.UploadId, _err)
				}
				return nil, err
			}
		}
	}

	uploadPartError := o.uploadPartConcurrent(
		urchinServiceAddr,
		sourceFile,
		taskId,
		ufc,
		input)
	err = o.handleUploadFileResult(
		urchinServiceAddr,
		objectKey,
		taskId,
		uploadPartError,
		ufc,
		enableCheckpoint)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"S3:handleUploadFileResult failed. err: %v", err)
		return nil, err
	}

	completeOutput, err := o.completeParts(
		urchinServiceAddr,
		objectKey,
		taskId,
		ufc,
		enableCheckpoint,
		checkpointFilePath)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"S3:completeParts failed. err: %v", err)
		return completeOutput, err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:resumeUpload finish.")
	return completeOutput, err
}

func (o *S3) uploadPartConcurrent(
	urchinServiceAddr, sourceFile string,
	taskId int32,
	ufc *UploadCheckpoint,
	input *obs.UploadFileInput) error {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadPartConcurrent start."+
		" urchinServiceAddr: %s, sourceFile: %s, taskId: %d",
		urchinServiceAddr, sourceFile, taskId)

	//pool := obs.NewRoutinePool(input.TaskNum, obs.MAX_PART_NUM)
	var wg sync.WaitGroup
	pool, err := ants.NewPool(DefaultS3DownloadMultiTaskNum)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"ants.NewPool for download part failed. err: %v", err)
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
		//pool.ExecuteFunc(func() interface{} {
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
			}()
			result := task.Run(urchinServiceAddr, sourceFile, ufc.UploadId, taskId)
			err := o.handleUploadTaskResult(
				result,
				ufc,
				task.PartNumber,
				input.EnableCheckpoint,
				input.CheckpointFile,
				lock)
			if err != nil && atomic.CompareAndSwapInt32(&errFlag, 0, 1) {
				obs.DoLog(obs.LEVEL_ERROR,
					"S3:handleUploadTaskResult failed. err: %v", err)
				uploadPartError.Store(err)
			}
			obs.DoLog(obs.LEVEL_DEBUG, "S3:handleUploadTaskResult finish.")
			return
		})
	}
	//pool.ShutDown()
	wg.Wait()
	if err, ok := uploadPartError.Load().(error); ok {
		obs.DoLog(obs.LEVEL_ERROR, "uploadPartError. err: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:uploadPartConcurrent finish.")
	return nil
}

func (o *S3) getUploadCheckpointFile(
	urchinServiceAddr, objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *obs.UploadFileInput) (needCheckpoint bool, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:getUploadCheckpointFile start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	checkpointFilePath := input.CheckpointFile
	checkpointFileStat, err := os.Stat(checkpointFilePath)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Stat checkpoint file failed."+
				" checkpointFilePath: %s, err: %v", checkpointFilePath, err)
		return true, nil
	}
	if checkpointFileStat.IsDir() {
		obs.DoLog(obs.LEVEL_ERROR,
			"Checkpoint file can not be a folder.")
		return false,
			errors.New("checkpoint file can not be a folder")
	}
	err = o.loadCheckpointFile(checkpointFilePath, ufc)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Load checkpoint file failed. err: %v", err)
		return true, nil
	} else if !ufc.isValid(input.Key, input.UploadFile, uploadFileStat) {
		if ufc.Key != "" && ufc.UploadId != "" {
			_err := o.AbortMultipartUpload(
				urchinServiceAddr,
				objectKey,
				ufc.UploadId,
				taskId)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"AbortMultipartUpload failed."+
						" task: %s, err: %v", ufc.UploadId, _err)
			}
		}
		_err := os.Remove(checkpointFilePath)
		if _err != nil {
			if !os.IsNotExist(_err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. checkpointFilePath: %s, err: %v",
					checkpointFilePath, _err)
			}
		}
	} else {
		obs.DoLog(obs.LEVEL_DEBUG, "S3:getUploadCheckpointFile finish.")
		return false, nil
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:getUploadCheckpointFile finish.")
	return true, nil
}

func (o *S3) prepareUpload(
	urchinServiceAddr, objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	uploadFileStat os.FileInfo,
	input *obs.UploadFileInput) error {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:prepareUpload start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	initiateMultipartUploadOutput, err := o.InitiateMultipartUpload(
		urchinServiceAddr,
		objectKey,
		taskId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"InitiateMultipartUpload failed. err: %v", err)
		return err
	}

	ufc.Key = input.Key
	ufc.UploadFile = input.UploadFile
	ufc.FileInfo = obs.FileStatus{}
	ufc.FileInfo.Size = uploadFileStat.Size()
	ufc.FileInfo.LastModified = uploadFileStat.ModTime().Unix()
	ufc.UploadId = initiateMultipartUploadOutput.UploadId

	err = o.sliceFile(input.PartSize, ufc)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"S3:sliceFile failed. err: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:prepareUpload finish.")
	return err
}

func (o *S3) sliceFile(partSize int64, ufc *UploadCheckpoint) (err error) {
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
		obs.DoLog(obs.LEVEL_ERROR,
			"upload file part too large.")
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
	return nil
}

func (o *S3) handleUploadTaskResult(
	result interface{},
	ufc *UploadCheckpoint,
	partNum int,
	enableCheckpoint bool,
	checkpointFilePath string,
	lock *sync.Mutex) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:handleUploadTaskResult start."+
		" partNum: %d", partNum)

	if uploadPartOutput, ok := result.(*obs.UploadPartOutput); ok {
		lock.Lock()
		defer lock.Unlock()
		ufc.UploadParts[partNum-1].Etag = uploadPartOutput.ETag
		ufc.UploadParts[partNum-1].IsCompleted = true

		if enableCheckpoint {
			_err := o.updateCheckpointFile(ufc, checkpointFilePath)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"updateCheckpointFile failed. err: %v", _err)
			}
		}
	} else if result != errAbort {
		if _err, ok := result.(error); ok {
			obs.DoLog(obs.LEVEL_ERROR,
				"handleUploadTaskResult result err: %v", _err)
			err = _err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:handleUploadTaskResult finish.")
	return
}

func (o *S3) handleUploadFileResult(
	urchinServiceAddr, objectKey string,
	taskId int32,
	uploadPartError error,
	ufc *UploadCheckpoint,
	enableCheckpoint bool) error {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:handleUploadFileResult start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	if uploadPartError != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"handleUploadFileResult uploadPartError: %v", uploadPartError)
		if enableCheckpoint {
			return uploadPartError
		}
		_err := o.AbortMultipartUpload(
			urchinServiceAddr,
			objectKey,
			ufc.UploadId,
			taskId)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"AbortMultipartUpload failed. err: %v", _err)
		}
		return uploadPartError
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:handleUploadFileResult finish.")
	return nil
}

func (o *S3) completeParts(
	urchinServiceAddr, objectKey string,
	taskId int32,
	ufc *UploadCheckpoint,
	enableCheckpoint bool,
	checkpointFilePath string) (output *obs.CompleteMultipartUploadOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:completeParts start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	parts := make([]XPart, 0, len(ufc.UploadParts))
	for _, uploadPart := range ufc.UploadParts {
		part := XPart{}
		part.PartNumber = uploadPart.PartNumber
		part.ETag = uploadPart.Etag
		parts = append(parts, part)
	}
	var partSlice PartSlice = parts
	sort.Sort(partSlice)
	output, err = o.CompleteMultipartUpload(
		urchinServiceAddr,
		objectKey,
		ufc.UploadId,
		taskId,
		partSlice)

	if err == nil {
		if enableCheckpoint {
			_err := os.Remove(checkpointFilePath)
			if _err != nil {
				if !os.IsNotExist(_err) {
					obs.DoLog(obs.LEVEL_ERROR,
						"os.Remove failed. checkpointFilePath: %s, err: %v",
						checkpointFilePath, _err)
				}
			}
		}
		obs.DoLog(obs.LEVEL_DEBUG, "S3:completeParts finish.")
		return output, err
	}
	if !enableCheckpoint {
		_err := o.AbortMultipartUpload(
			urchinServiceAddr,
			objectKey,
			ufc.UploadId,
			taskId)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"AbortMultipartUpload failed."+
					" UploadId: %v, err: %v", ufc.UploadId, _err)
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:completeParts failed.")
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
	key, uploadFile string,
	fileStat os.FileInfo) bool {

	if ufc.Key != key ||
		ufc.UploadFile != uploadFile {
		obs.DoLog(obs.LEVEL_ERROR,
			"Checkpoint file is invalid."+
				" bucketName or objectKey or uploadFile was changed.")
		return false
	}
	if ufc.FileInfo.Size != fileStat.Size() ||
		ufc.FileInfo.LastModified != fileStat.ModTime().Unix() {
		obs.DoLog(obs.LEVEL_ERROR,
			"Checkpoint file is invalid."+
				" uploadFile was changed.")
		return false
	}
	if ufc.UploadId == "" {
		obs.DoLog(obs.LEVEL_ERROR,
			"UploadId is invalid.")
		return false
	}
	return true
}

type UploadPartTask struct {
	obs.UploadPartInput
	obsClient        *obs.ObsClient
	abort            *int32
	enableCheckpoint bool
}

func (task *UploadPartTask) Run(
	urchinServiceAddr, sourceFile, uploadId string,
	taskId int32) interface{} {

	obs.DoLog(obs.LEVEL_DEBUG,
		"UploadPartTask:Run start."+
			" urchinServiceAddr: %s, sourceFile: %s, taskId: %d",
		urchinServiceAddr, sourceFile, taskId)

	if atomic.LoadInt32(task.abort) == 1 {
		return errAbort
	}

	fd, err := os.Open(sourceFile)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "os.Open failed. error: %v", err)
		return err
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

	readerWrapper.TotalCount = task.PartSize
	readerWrapper.Mark = task.Offset
	if _, err = fd.Seek(task.Offset, io.SeekStart); err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "fd.Seek failed. error: %v", err)
		return err
	}

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	createUploadPartSignedUrlReq := new(CreateUploadPartSignedUrlReq)
	createUploadPartSignedUrlReq.UploadId = uploadId
	createUploadPartSignedUrlReq.PartNumber = int32(task.PartNumber)
	createUploadPartSignedUrlReq.TaskId = taskId
	createUploadPartSignedUrlReq.Source = task.Key
	err, createUploadPartSignedUrlResp :=
		urchinService.CreateUploadPartSignedUrl(
			createUploadPartSignedUrlReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateUploadPartSignedUrl failed. err: %v", err)
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

	if err == nil {
		if uploadPartOutput.ETag == "" {
			obs.DoLog(obs.LEVEL_ERROR,
				"Get invalid etag value. PartNumber: %d", task.PartNumber)
			if !task.enableCheckpoint {
				atomic.CompareAndSwapInt32(task.abort, 0, 1)
				obs.DoLog(obs.LEVEL_ERROR,
					"Task aborted. PartNumber: %d", task.PartNumber)
			}
			return errors.New("invalid etag value")
		}
		return uploadPartOutput
	} else if obsError, ok := err.(obs.ObsError); ok &&
		obsError.StatusCode >= 400 && obsError.StatusCode < 500 {

		atomic.CompareAndSwapInt32(task.abort, 0, 1)
		obs.DoLog(obs.LEVEL_ERROR,
			"obsClient.UploadPartWithSignedUrl failed."+
				" obsCode: %s, obsMessage: %s", obsError.Code, obsError.Message)
	}
	return err
}

func (o *S3) Download(
	urchinServiceAddr, targetPath string,
	taskId int32,
	bucketName string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:Download start."+
		" urchinServiceAddr: %s, targetPath: %s, taskId: %d, bucketName: %s",
		urchinServiceAddr, targetPath, taskId, bucketName)
	listObjectsOutput, err := o.ListObjectsWithSignedUrl(urchinServiceAddr, taskId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"S3:ListObjectsWithSignedUrl failed."+
				" taskId: %d, err: %v", taskId, err)
		return err
	}
	err = o.downloadObjects(
		urchinServiceAddr,
		targetPath,
		taskId,
		bucketName,
		listObjectsOutput)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "S3:downloadObjects failed."+
			" urchinServiceAddr: %s, targetPath: %s,"+
			" taskId: %d, bucketName: %s err: %v",
			urchinServiceAddr, targetPath, taskId, bucketName, err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:Download finish.")
	return nil
}

func (o *S3) downloadObjects(
	urchinServiceAddr, targetPath string,
	taskId int32,
	bucketName string,
	listObjectsOutput *obs.ListObjectsOutput) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:downloadObjects start."+
		" urchinServiceAddr: %s, targetPath: %s, taskId: %d, bucketName: %s",
		urchinServiceAddr, targetPath, taskId, bucketName)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := urchinService.GetTask(getTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. err: %v", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		obs.DoLog(obs.LEVEL_ERROR, "task not exist. taskId: %d", taskId)
		return errors.New("task not exist")
	}
	task := getTaskResp.Data.List[0].Task
	downloadObjectTaskParams := new(DownloadObjectTaskParams)
	err = json.Unmarshal([]byte(task.Params), downloadObjectTaskParams)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"DownloadObjectTaskParams Unmarshal failed."+
				" params: %s, error: %v", task.Params, err)
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
		obs.DoLog(obs.LEVEL_ERROR,
			"ReadFile failed. downloadFolderRecord: %s, err: %v",
			downloadFolderRecord, err)
		return err
	}

	var isAllSuccess = true
	var wg sync.WaitGroup
	pool, err := ants.NewPool(DefaultS3DownloadFileTaskNum)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"ants.NewPool for download object failed. err: %v", err)
		return err
	}
	defer pool.Release()
	for index, object := range listObjectsOutput.Contents {
		obs.DoLog(obs.LEVEL_DEBUG,
			"Object: Content[%d]-ETag:%s, Key:%s, Size:%d",
			index, object.ETag, object.Key, object.Size)
		// 处理文件
		itemObject := object
		if _, exists := fileMap[itemObject.Key]; exists {
			obs.DoLog(obs.LEVEL_INFO,
				"file already success. objectKey: %s", itemObject.Key)
			continue
		}
		wg.Add(1)
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"downloadFile failed. err: %v", err)
					isAllSuccess = false
				}
			}()
			_, err = o.downloadPartWithSignedUrl(
				urchinServiceAddr,
				bucketName,
				itemObject.Key,
				targetPath+itemObject.Key,
				taskId)
			if err != nil {
				isAllSuccess = false
				obs.DoLog(obs.LEVEL_ERROR,
					"downloadPartWithSignedUrl failed."+
						" urchinServiceAddr: %s, objectKey: %s, taskId: %d, err: %v",
					urchinServiceAddr, itemObject.Key, taskId, err)
			}
			fileMutex.Lock()
			defer fileMutex.Unlock()
			f, err := os.OpenFile(
				downloadFolderRecord,
				os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				isAllSuccess = false
				obs.DoLog(obs.LEVEL_ERROR,
					"os.OpenFile failed."+
						" downloadFolderRecord: %s, err: %v",
					downloadFolderRecord, err)
				return
			}
			defer func() {
				errMsg := f.Close()
				if errMsg != nil {
					obs.DoLog(obs.LEVEL_WARN,
						"Failed to close file. error: %v", errMsg)
				}
			}()
			_, err = f.Write([]byte(itemObject.Key + "\n"))
			if err != nil {
				isAllSuccess = false
				obs.DoLog(obs.LEVEL_ERROR,
					"Write file failed."+
						" downloadFolderRecord: %s, file item: %s, err: %v",
					downloadFolderRecord, itemObject.Key, err)
				return
			}
		})
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"ants.Submit for download object failed. err: %v", err)
			return err
		}
	}
	wg.Wait()
	if !isAllSuccess {
		obs.DoLog(obs.LEVEL_ERROR,
			"S3:downloadObjects not all success."+
				" uuid: %s", downloadObjectTaskParams.Request.ObjUuid)
		return errors.New("downloadObjects not all success")
	} else {
		_err := os.Remove(downloadFolderRecord)
		if nil != _err {
			if !os.IsNotExist(_err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. downloadFolderRecord: %s, err: %v",
					downloadFolderRecord, _err)
			}
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG, "S3:downloadObjects finish.")
	return nil
}

func (o *S3) downloadPartWithSignedUrl(
	urchinServiceAddr, bucketName, objectKey, targetFile string,
	taskId int32) (output *obs.GetObjectMetadataOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:downloadPartWithSignedUrl start."+
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
				fmt.Errorf(
					"cannot create folder: %s same file exists", parentDir)
		} else {
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
		urchinServiceAddr,
		objectKey,
		taskId,
		downloadFileInput)

	obs.DoLog(obs.LEVEL_DEBUG,
		"S3:downloadPartWithSignedUrl success.")

	return
}

func (o *S3) resumeDownload(
	urchinServiceAddr, objectKey string,
	taskId int32,
	input *obs.DownloadFileInput) (
	output *obs.GetObjectMetadataOutput, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:resumeDownload start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	getObjectMetaOutput, err := o.GetObjectInfoWithSignedUrl(
		urchinServiceAddr, objectKey, taskId)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetObjectInfoWithSignedUrl failed."+
			" urchinServiceAddr: %s, objectKey: %s, taskId: %d, err: %v",
			urchinServiceAddr, objectKey, taskId, err)
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
			dfc,
			input,
			getObjectMetaOutput)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "getDownloadCheckpointFile failed."+
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
		dfc.ObjectInfo.LastModified = getObjectMetaOutput.LastModified.Unix()
		dfc.ObjectInfo.Size = getObjectMetaOutput.ContentLength
		dfc.ObjectInfo.ETag = getObjectMetaOutput.ETag
		dfc.TempFileInfo = obs.TempFileInfo{}
		dfc.TempFileInfo.TempFileUrl = input.DownloadFile + ".tmp"
		dfc.TempFileInfo.Size = getObjectMetaOutput.ContentLength

		o.sliceObject(objectSize, partSize, dfc)
		_err := o.prepareTempFile(dfc.TempFileInfo.TempFileUrl, dfc.TempFileInfo.Size)
		if _err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"prepareTempFile failed. TempFileUrl: %s, Size: %d, err: %v",
				dfc.TempFileInfo.TempFileUrl, dfc.TempFileInfo.Size, _err)
			return nil, _err
		}

		if enableCheckpoint {
			_err := o.updateCheckpointFile(dfc, checkpointFilePath)
			if _err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"Failed to update checkpoint file. error: %v", _err)
				_errMsg := os.Remove(dfc.TempFileInfo.TempFileUrl)
				if _errMsg != nil {
					if !os.IsNotExist(_errMsg) {
						obs.DoLog(obs.LEVEL_ERROR,
							"os.Remove failed. TempFileUrl: %s, err: %v",
							dfc.TempFileInfo.TempFileUrl, _errMsg)
					}
				}
				return nil, _err
			}
		}
	}

	downloadFileError := o.downloadFileConcurrent(
		urchinServiceAddr, objectKey, taskId, input, dfc)
	err = o.handleDownloadFileResult(
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
		_err := os.Remove(checkpointFilePath)
		if _err != nil {
			if !os.IsNotExist(_err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. checkpointFilePath: %s, err: %v",
					checkpointFilePath, _err)
			}
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:resumeDownload success.")
	return getObjectMetaOutput, nil
}

func (o *S3) downloadFileConcurrent(
	urchinServiceAddr, objectKey string,
	taskId int32,
	input *obs.DownloadFileInput,
	dfc *DownloadCheckpoint) error {

	obs.DoLog(obs.LEVEL_DEBUG, "S3:downloadFileConcurrent start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d",
		urchinServiceAddr, objectKey, taskId)

	//pool := obs.NewRoutinePool(input.TaskNum, obs.MAX_PART_NUM)
	var wg sync.WaitGroup
	pool, err := ants.NewPool(DefaultS3DownloadMultiTaskNum)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"ants.NewPool for download part failed. err: %v", err)
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
		obs.DoLog(obs.LEVEL_DEBUG,
			"DownloadPartTask params."+
				" rangeStart: %d, rangeEnd: %d, partNumber: %d,"+
				" tempFileURL: %s, enableCheckpoint: %t",
			downloadPart.Offset,
			downloadPart.RangeEnd,
			downloadPart.PartNumber,
			dfc.TempFileInfo.TempFileUrl,
			input.EnableCheckpoint)

		wg.Add(1)
		//pool.ExecuteFunc(func() interface{} {
		err = pool.Submit(func() {
			defer func() {
				wg.Done()
			}()
			if 0 == dfc.ObjectInfo.Size {
				lock.Lock()
				defer lock.Unlock()
				dfc.DownloadParts[task.partNumber-1].IsCompleted = true

				if input.EnableCheckpoint {
					err := o.updateCheckpointFile(dfc, input.CheckpointFile)
					if err != nil {
						obs.DoLog(obs.LEVEL_WARN,
							"Failed to update checkpoint file. error: %v", err)
						downloadPartError.Store(err)
					}
				}
				return
			} else {
				result := task.Run(urchinServiceAddr, objectKey, taskId)
				err := o.handleDownloadTaskResult(
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
				return
			}
		})
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"ants.Submit for download part failed. err: %v", err)
			return err
		}
	}
	//pool.ShutDown()
	wg.Wait()
	if err, ok := downloadPartError.Load().(error); ok {
		obs.DoLog(obs.LEVEL_ERROR, "downloadPartError. err: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "S3:downloadFileConcurrent success.")
	return nil
}

func (o *S3) getDownloadCheckpointFile(
	dfc *DownloadCheckpoint,
	input *obs.DownloadFileInput,
	output *obs.GetObjectMetadataOutput) (needCheckpoint bool, err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "getDownloadCheckpointFile start."+
		" CheckpointFile: %s", input.CheckpointFile)

	checkpointFilePath := input.CheckpointFile
	checkpointFileStat, err := os.Stat(checkpointFilePath)
	if err != nil {
		obs.DoLog(obs.LEVEL_DEBUG,
			"Stat checkpoint file failed. error: %v", err)
		return true, nil
	}
	if checkpointFileStat.IsDir() {
		obs.DoLog(obs.LEVEL_ERROR, "Checkpoint file can not be a folder.")
		return false,
			errors.New("checkpoint file can not be a folder")
	}
	err = o.loadCheckpointFile(checkpointFilePath, dfc)
	if err != nil {
		obs.DoLog(obs.LEVEL_WARN,
			"Load checkpoint file failed. error: %v", err)
		return true, nil
	} else if !dfc.IsValid(input, output) {
		if dfc.TempFileInfo.TempFileUrl != "" {
			_err := os.Remove(dfc.TempFileInfo.TempFileUrl)
			if _err != nil {
				if !os.IsNotExist(_err) {
					obs.DoLog(obs.LEVEL_ERROR,
						"os.Remove failed. TempFileUrl: %s, err: %v",
						dfc.TempFileInfo.TempFileUrl, _err)
				}
			}
		}
		_err := os.Remove(checkpointFilePath)
		if _err != nil {
			if !os.IsNotExist(_err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. checkpointFilePath: %s, err: %v",
					checkpointFilePath, _err)
			}
		}
	} else {
		obs.DoLog(obs.LEVEL_DEBUG, "no need to check point.")
		return false, nil
	}
	obs.DoLog(obs.LEVEL_DEBUG, "need to check point.")
	return true, nil
}

func (o *S3) prepareTempFile(tempFileURL string, fileSize int64) error {
	obs.DoLog(obs.LEVEL_DEBUG, "prepareTempFile start."+
		" tempFileURL: %s, fileSize: %d", tempFileURL, fileSize)

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
		return fmt.Errorf(
			"cannot create folder: %s due to a same file exists", parentDir)
	}

	err = o.createFile(tempFileURL, fileSize)
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

func (o *S3) createFile(tempFileURL string, fileSize int64) error {
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

func (o *S3) handleDownloadTaskResult(
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
			_err := o.updateCheckpointFile(dfc, checkpointFile)
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

func (o *S3) handleDownloadFileResult(
	tempFileURL string,
	enableCheckpoint bool,
	downloadFileError error) error {

	obs.DoLog(obs.LEVEL_DEBUG,
		"handleDownloadFileResult start. tempFileURL: %s", tempFileURL)

	if downloadFileError != nil {
		if !enableCheckpoint {
			_err := os.Remove(tempFileURL)
			if _err != nil {
				if !os.IsNotExist(_err) {
					obs.DoLog(obs.LEVEL_ERROR,
						"os.Remove failed. tempFileURL: %s, err: %v",
						tempFileURL, _err)
				}
			}
		}
		obs.DoLog(obs.LEVEL_DEBUG, "handleDownloadFileResult finish.")
		return downloadFileError
	}

	obs.DoLog(obs.LEVEL_DEBUG, "handleDownloadFileResult success.")
	return nil
}

func (o *S3) updateDownloadFile(
	filePath string,
	rangeStart int64,
	output *obs.GetObjectOutput) error {

	obs.DoLog(obs.LEVEL_DEBUG, "updateDownloadFile start."+
		" filePath: %s, rangeStart: %d", filePath, rangeStart)

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
				obs.DoLog(obs.LEVEL_ERROR,
					"Failed to write to file. error: %v", werr)
				return werr
			}
			if wcnt != readCount {
				obs.DoLog(obs.LEVEL_ERROR, "Failed to write to file."+
					" filePath: %s, expect: %d, actual: %d",
					filePath, readCount, wcnt)
				return fmt.Errorf("failed to write to file."+
					" filePath: %s, expect: %d, actual: %d",
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

	obs.DoLog(obs.LEVEL_DEBUG,
		"updateDownloadFile success. readTotal: %d", readTotal)
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

		obs.DoLog(obs.LEVEL_INFO, "Checkpoint file is invalid, "+
			"the bucketName or objectKey or downloadFile was changed."+
			" clear the record.")
		return false
	}
	if dfc.ObjectInfo.LastModified != output.LastModified.Unix() ||
		dfc.ObjectInfo.ETag != output.ETag ||
		dfc.ObjectInfo.Size != output.ContentLength {

		obs.DoLog(obs.LEVEL_INFO,
			"Checkpoint file is invalid, the object info was changed."+
				" clear the record.")
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
	s3               *S3
	abort            *int32
	partNumber       int64
	tempFileURL      string
	enableCheckpoint bool
}

func (task *DownloadPartTask) Run(
	urchinServiceAddr, objectKey string, taskId int32) interface{} {

	obs.DoLog(obs.LEVEL_DEBUG, "DownloadPartTask:Run start."+
		" urchinServiceAddr: %s, objectKey: %s, taskId: %d, partNumber: %d",
		urchinServiceAddr, objectKey, taskId, task.partNumber)

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
		obs.DoLog(obs.LEVEL_DEBUG, "obsClient.GetObjectWithSignedUrl success.")
		defer func() {
			errMsg := getObjectWithSignedUrlOutput.Body.Close()
			if errMsg != nil {
				obs.DoLog(obs.LEVEL_WARN, "Failed to close response body.")
			}
		}()
		_err := task.s3.updateDownloadFile(
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
		obs.DoLog(obs.LEVEL_DEBUG, "DownloadPartTask:Run success.")
		return getObjectWithSignedUrlOutput
	} else if obsError, ok := err.(obs.ObsError); ok &&
		obsError.StatusCode >= 400 &&
		obsError.StatusCode < 500 {

		atomic.CompareAndSwapInt32(task.abort, 0, 1)
		obs.DoLog(obs.LEVEL_WARN,
			"Task is aborted, part number: %d", task.partNumber)
	}

	obs.DoLog(obs.LEVEL_DEBUG, "DownloadPartTask:Run failed. err: %v", err)
	return err
}
