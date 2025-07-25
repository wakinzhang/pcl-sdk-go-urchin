package module

import (
	"context"
	"encoding/xml"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	"go.uber.org/zap"
	"io"
	"os"
)

const (
	ScowDefaultErrorCode = -1
	ScowSuccessCode      = 200
	ScowSSHErrorCode     = 400

	ScowAttempts = 3
	ScowDelay    = 1

	ScowSuccessMessage       = "success"
	ScowAlreadyExistsMessage = "already exists"

	DefaultScowUploadMultiSize = 500 * 1024 * 1024

	DefaultScowUploadFileTaskNum    = 100
	DefaultScowUploadMultiTaskNum   = 20
	DefaultScowDownloadFileTaskNum  = 100
	DefaultScowDownloadMultiTaskNum = 20

	DefaultScowRateLimit = 65535
	DefaultScowRateBurst = 65535

	DefaultScowMaxPartSize = 5 * 1024 * 1024 * 1024
	DefaultScowMinPartSize = 100 * 1024

	DefaultScowTokenExpireHours = 6

	ScowHttpHeaderAuth = "Authorization"

	ScowMultiPartFormFiledFileMd5Name = "fileMd5Name"
	ScowMultiPartFormFiledFile        = "file"

	ScowGetTokenInterface     = "/v1/sys/user/login"
	ScowCheckExistInterface   = "/v1/ai/api/file/checkExist"
	ScowMkdirInterface        = "/v1/ai/api/file/mkdir"
	ScowDeleteInterface       = "/v1/ai/api/file/delete"
	ScowListInterface         = "/v1/ai/api/file/listDirectory"
	ScowUploadInterface       = "/v1/ai/api/files/upload"
	ScowUploadChunksInterface = "/v1/ai/api/files/uploadChunks"
	ScowMergeChunksInterface  = "/v1/ai/api/file/mergeChunks"
	ScowDownloadInterface     = "/v1/ai/api/file/download"

	ScowObjectTypeFile   = "FILE"
	ScowObjectTypeFolder = "DIR"
)

type ScowMkdirInput struct {
	Path string
}

type ScowUploadInput struct {
	SourcePath string
	TargetPath string
	NeedPure   bool
}

type ScowDownloadInput struct {
	SourcePath string
	TargetPath string
}

type ScowDeleteInput struct {
	Path   string
	Target string
}

type ScowBaseResponse struct {
	RespCode    int32       `json:"respCode"`
	RespError   string      `json:"respError"`
	RespMessage string      `json:"respMessage"`
	RespBody    interface{} `json:"respBody"`
}

type ScowBaseMessageResponse struct {
	Message string `json:"message"`
}

type ScowGetTokenReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type ScowGetTokenResponse struct {
	RespCode    int32                    `json:"respCode"`
	RespError   string                   `json:"respError"`
	RespMessage string                   `json:"respMessage"`
	RespBody    ScowGetTokenResponseBody `json:"respBody"`
}

type ScowGetTokenResponseBody struct {
	ID    int32  `json:"id"`
	Token string `json:"core-sctoken"`
}

type ScowCheckExistReq struct {
	ClusterId string `url:"clusterId"`
	Path      string `url:"path"`
}

type ScowCheckExistResponse struct {
	RespCode    int32                      `json:"respCode"`
	RespError   string                     `json:"respError"`
	RespMessage string                     `json:"respMessage"`
	RespBody    ScowCheckExistResponseBody `json:"respBody"`
}

type ScowCheckExistResponseBody struct {
	Data ScowCheckExistResponseBodyData `json:"data"`
}

type ScowCheckExistResponseBodyData struct {
	Exists bool `json:"exists"`
}

type ScowMkdirReq struct {
	ClusterId string `json:"clusterId"`
	Path      string `json:"path"`
}

type ScowDeleteReq struct {
	ClusterId string `json:"clusterId"`
	Target    string `json:"target"`
	Path      string `json:"path"`
}

type ScowUploadReq struct {
	ClusterId string `json:"clusterId" url:"clusterId"`
	Path      string `json:"path" url:"path"`
}

type ScowUploadChunksReq struct {
	ClusterId string `json:"clusterId" url:"clusterId"`
	Path      string `json:"path" url:"path"`
}

type ScowMergeChunksReq struct {
	ClusterId string `json:"clusterId"`
	Path      string `json:"path"`
	Md5       string `json:"md5"`
	FileName  string `json:"fileName"`
}

type ScowListReq struct {
	ClusterId string `url:"clusterId"`
	Path      string `url:"path"`
}

type ScowListResponse struct {
	RespCode    int32                 `json:"respCode"`
	RespError   string                `json:"respError"`
	RespMessage string                `json:"respMessage"`
	RespBody    *ScowListResponseBody `json:"respBody"`
}

type ScowListResponseBody struct {
	Total       int32         `json:"total"`
	ScowObjects []*ScowObject `json:"data"`
}

type ScowDownloadReq struct {
	ClusterId string `url:"clusterId"`
	Path      string `url:"path"`
	Download  string `url:"download"`
}

type ScowObject struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	MTime   string `json:"mtime"`
	Size    int64  `json:"size"`
	Mode    int32  `json:"mode"`
	PathExt string `json:"path"`
}

type ScowUploadFileInput struct {
	ObjectPath       string
	UploadFile       string
	FileName         string
	Md5              string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
	EncodingType     string
}

type ScowFileStatus struct {
	XMLName      xml.Name `xml:"FileInfo"`
	LastModified int64    `xml:"LastModified"`
	Size         int64    `xml:"Size"`
}

type ScowUploadPartInfo struct {
	XMLName     xml.Name `xml:"UploadPart"`
	PartNumber  int32    `xml:"PartNumber"`
	PartSize    int64    `xml:"PartSize"`
	Offset      int64    `xml:"Offset"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type ScowUploadCheckpoint struct {
	XMLName     xml.Name             `xml:"UploadFileCheckpoint"`
	ObjectPath  string               `xml:"ObjectPath"`
	UploadFile  string               `xml:"FileUrl"`
	FileInfo    ScowFileStatus       `xml:"FileInfo"`
	UploadParts []ScowUploadPartInfo `xml:"UploadParts>UploadPart"`
}

func (ufc *ScowUploadCheckpoint) IsValid(
	ctx context.Context,
	uploadFile string,
	fileStat os.FileInfo) bool {

	InfoLogger.WithContext(ctx).Debug(
		"ScowUploadCheckpoint:isValid start.",
		zap.String("uploadFile", uploadFile),
		zap.String("ufcUploadFile", ufc.UploadFile),
		zap.Int64("fileStatSize", fileStat.Size()),
		zap.Int64("ufcFileInfoSize", ufc.FileInfo.Size))

	if ufc.UploadFile != uploadFile {
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

	InfoLogger.WithContext(ctx).Debug(
		"ScowUploadCheckpoint:isValid finish.")
	return true
}

type ScowDownloadFileInput struct {
	DownloadFile     string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
}

type ScowObjectInfo struct {
	XMLName xml.Name `xml:"ObjectInfo"`
	Size    int64    `xml:"Size"`
}

type ScowTempFileInfo struct {
	XMLName     xml.Name `xml:"TempFileInfo"`
	TempFileUrl string   `xml:"TempFileUrl"`
	Size        int64    `xml:"Size"`
}

type ScowDownloadPartInfo struct {
	XMLName     xml.Name `xml:"DownloadPart"`
	PartNumber  int64    `xml:"PartNumber"`
	Offset      int64    `xml:"Offset"`
	Length      int64    `xml:"Length"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type ScowDownloadPartOutput struct {
	Body io.ReadCloser
}

type ScowDownloadCheckpoint struct {
	XMLName       xml.Name               `xml:"DownloadFileCheckpoint"`
	ObjectPath    string                 `xml:"ObjectPath"`
	DownloadFile  string                 `xml:"FileUrl"`
	ObjectInfo    ScowObjectInfo         `xml:"ObjectInfo"`
	TempFileInfo  ScowTempFileInfo       `xml:"TempFileInfo"`
	DownloadParts []ScowDownloadPartInfo `xml:"DownloadParts>DownloadPart"`
}

func (dfc *ScowDownloadCheckpoint) IsValid(
	ctx context.Context,
	input *ScowDownloadFileInput,
	object *ScowObject) bool {

	InfoLogger.WithContext(ctx).Debug(
		"ScowDownloadCheckpoint:IsValid start.",
		zap.String("dfcDownloadFile", dfc.DownloadFile),
		zap.String("input.DownloadFile", input.DownloadFile),
		zap.Int64("dfcObjectInfoSize", dfc.ObjectInfo.Size),
		zap.Int64("objectSize", object.Size),
		zap.Int64("dfcTempFileInfoSize: ", dfc.TempFileInfo.Size))

	if dfc.DownloadFile != input.DownloadFile {
		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid. downloadFile was changed." +
				" clear the record.")
		return false
	}
	if dfc.ObjectInfo.Size != object.Size {
		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid. the object info was changed." +
				" clear the record.")
		return false
	}
	if dfc.TempFileInfo.Size != object.Size {
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
		"ScowDownloadCheckpoint:IsValid finish.")
	return true
}
