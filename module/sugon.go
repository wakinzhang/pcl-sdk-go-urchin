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
	SugonSuccessCode     = "0"
	SugonErrFileNotExist = "911020"
	SugonErrFileExist    = "911021"

	DefaultSugonUploadMultiSize = 500 * 1024 * 1024

	DefaultSugonUploadFileTaskNum    = 100
	DefaultSugonUploadMultiTaskNum   = 20
	DefaultSugonDownloadFileTaskNum  = 100
	DefaultSugonDownloadMultiTaskNum = 20

	DefaultSugonRateLimit = 65535
	DefaultSugonRateBurst = 65535

	DefaultSugonMaxPartSize = 5 * 1024 * 1024 * 1024
	DefaultSugonMinPartSize = 100 * 1024

	DefaultSugonListLimit = 100

	SugonGetTokenInterface     = "/ac/openapi/v2/tokens/state"
	SugonPostTokenInterface    = "/ac/openapi/v2/tokens"
	SugonMkdirInterface        = "/efile/openapi/v2/file/mkdir"
	SugonDeleteInterface       = "/efile/openapi/v2/file/remove"
	SugonUploadInterface       = "/efile/openapi/v2/file/upload"
	SugonUploadChunksInterface = "/efile/openapi/v2/file/burst"
	SugonMergeChunksInterface  = "/efile/openapi/v2/file/merge"
	SugonListInterface         = "/efile/openapi/v2/file/list"
	SugonDownloadInterface     = "/efile/openapi/v2/file/download"

	SugonHttpHeaderUser     = "user"
	SugonHttpHeaderPassword = "password"
	SugonHttpHeaderOrgId    = "orgId"
	SugonHttpHeaderToken    = "token"

	TokenStateValid = "token is valid"

	SugonMultiPartFormFiledChunkNumber      = "chunkNumber"
	SugonMultiPartFormFiledCover            = "cover"
	SugonMultiPartFormFiledFileName         = "filename"
	SugonMultiPartFormFiledPath             = "path"
	SugonMultiPartFormFiledRelativePath     = "relativePath"
	SugonMultiPartFormFiledTotalChunks      = "totalChunks"
	SugonMultiPartFormFiledTotalSize        = "totalSize"
	SugonMultiPartFormFiledChunkSize        = "chunkSize"
	SugonMultiPartFormFiledCurrentChunkSize = "currentChunkSize"
	SugonMultiPartFormFiledFile             = "file"

	SugonMultiPartFormFiledCoverECover = "cover"
)

type SugonMkdirInput struct {
	Path string
}

type SugonUploadInput struct {
	SourcePath string
	TargetPath string
	NeedPure   bool
}

type SugonDownloadInput struct {
	SourcePath string
	TargetPath string
}

type SugonDeleteInput struct {
	Path string
}

type SugonBaseResponse struct {
	Code string      `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

type SugonPostTokenResponse struct {
	Code string            `json:"code"`
	Msg  string            `json:"msg"`
	Data []*SugonTokenInfo `json:"data"`
}

type SugonTokenInfo struct {
	ClusterId   string `json:"clusterId"`
	ClusterName string `json:"clusterName"`
	Token       string `json:"token"`
}

type SugonMkdirReq struct {
	Path          string `url:"path"`
	CreateParents bool   `url:"createParents"`
}

type SugonDeleteReq struct {
	Paths     string `url:"paths"`
	Recursive bool   `url:"recursive"`
}

type SugonMergeChunksReq struct {
	Filename     string `url:"filename"`
	Path         string `url:"path"`
	RelativePath string `url:"relativePath"`
	Cover        string `url:"cover"`
}

type SugonListReq struct {
	Path  string `url:"path"`
	Start int32  `url:"start"`
	Limit int32  `url:"limit"`
}

type SugonListResponse struct {
	Code string                 `json:"code"`
	Msg  string                 `json:"msg"`
	Data *SugonListResponseData `json:"data"`
}

type SugonListResponseData struct {
	Total        int32              `json:"total"`
	Path         string             `json:"path"`
	ShareEnabled bool               `json:"shareEnabled"`
	Keyword      string             `json:"keyword"`
	FileList     []*SugonFileInfo   `json:"fileList"`
	Children     []*SugonFolderInfo `json:"children"`
}

type SugonFolderInfo struct {
	Id    int32  `json:"id"`
	Label string `json:"label"`
	Path  string `json:"path"`
}

type SugonFileInfo struct {
	CreationTime     string                `json:"creationTime"`
	FileKey          int64                 `json:"fileKey"`
	Group            string                `json:"group"`
	IsDirectory      bool                  `json:"isDirectory"`
	IsShare          bool                  `json:"isShare"`
	IsSymbolicLink   bool                  `json:"isSymbolicLink"`
	LastAccessTime   string                `json:"lastAccessTime"`
	LastModifiedTime string                `json:"lastModifiedTime"`
	Name             string                `json:"name"`
	Owner            string                `json:"owner"`
	Path             string                `json:"path"`
	Permission       string                `json:"permission"`
	PermissionAction SugonPermissionAction `json:"permissionAction"`
	Size             int64                 `json:"size"`
	IsOther          bool                  `json:"isOther"`
	IsRegularFile    bool                  `json:"isRegularFile"`
	Type             string                `json:"type"`
}

type SugonPermissionAction struct {
	Allowed bool `json:"allowed"`
	Execute bool `json:"execute"`
	Read    bool `json:"read"`
	Write   bool `json:"write"`
}

type SugonDownloadReq struct {
	Path string `url:"path"`
}

type SugonDownloadPartOutput struct {
	Body io.ReadCloser
}

type SugonUploadFileInput struct {
	ObjectPath       string
	UploadFile       string
	FileName         string
	RelativePath     string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
	EncodingType     string
}

type SugonFileStatus struct {
	XMLName      xml.Name `xml:"FileInfo"`
	LastModified int64    `xml:"LastModified"`
	Size         int64    `xml:"Size"`
}

type SugonUploadPartInfo struct {
	XMLName     xml.Name `xml:"UploadPart"`
	PartNumber  int32    `xml:"PartNumber"`
	PartSize    int64    `xml:"PartSize"`
	Offset      int64    `xml:"Offset"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type SugonUploadCheckpoint struct {
	XMLName     xml.Name              `xml:"UploadFileCheckpoint"`
	ObjectPath  string                `xml:"ObjectPath"`
	UploadFile  string                `xml:"FileUrl"`
	FileInfo    SugonFileStatus       `xml:"FileInfo"`
	TotalParts  int32                 `xml:"TotalParts"`
	UploadParts []SugonUploadPartInfo `xml:"UploadParts>UploadPart"`
}

func (ufc *SugonUploadCheckpoint) IsValid(
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

type SugonDownloadFileInput struct {
	DownloadFile     string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
}

type SugonTempFileInfo struct {
	XMLName     xml.Name `xml:"TempFileInfo"`
	TempFileUrl string   `xml:"TempFileUrl"`
	Size        int64    `xml:"Size"`
}

type SugonDownloadPartInfo struct {
	XMLName     xml.Name `xml:"DownloadPart"`
	PartNumber  int64    `xml:"PartNumber"`
	Offset      int64    `xml:"Offset"`
	Length      int64    `xml:"Length"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type SugonDownloadCheckpoint struct {
	XMLName       xml.Name                `xml:"DownloadFileCheckpoint"`
	ObjectPath    string                  `xml:"ObjectPath"`
	DownloadFile  string                  `xml:"FileUrl"`
	ObjectInfo    SugonFileInfo           `xml:"ObjectInfo"`
	TempFileInfo  SugonTempFileInfo       `xml:"TempFileInfo"`
	DownloadParts []SugonDownloadPartInfo `xml:"DownloadParts>DownloadPart"`
}

func (dfc *SugonDownloadCheckpoint) IsValid(
	ctx context.Context,
	input *SugonDownloadFileInput,
	object *SugonFileInfo) bool {

	InfoLogger.WithContext(ctx).Debug(
		"SugonDownloadCheckpoint:IsValid start.",
		zap.String("dfcDownloadFile", dfc.DownloadFile),
		zap.String("input.DownloadFile", input.DownloadFile),
		zap.Int64("dfcObjectInfoSize", dfc.ObjectInfo.Size),
		zap.Int64("objectSize", object.Size),
		zap.Int64("dfcTempFileInfoSize: ", dfc.TempFileInfo.Size))

	if dfc.DownloadFile != input.DownloadFile {
		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid." +
				" downloadFile was changed. clear the record.")
		return false
	}
	if dfc.ObjectInfo.Size != object.Size {
		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid." +
				" the object info was changed. clear the record.")
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
		"SugonDownloadCheckpoint:IsValid finish.")
	return true
}
