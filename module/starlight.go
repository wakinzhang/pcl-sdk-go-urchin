package module

import (
	"context"
	"encoding/xml"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	"io"
	"os"
)

const (
	SLSuccessCode = 200

	DefaultSLUploadFileTaskNum    = 100
	DefaultSLUploadMultiTaskNum   = 20
	DefaultSLDownloadFileTaskNum  = 100
	DefaultSLDownloadMultiTaskNum = 20
	DefaultSLMaxPartSize          = 5 * 1024 * 1024 * 1024
	DefaultSLMinPartSize          = 100 * 1024

	DefaultStarLightTokenExpireHours = 12

	StarLightHttpHeaderAuth = "bihu-token"

	StarLightGetTokenInterface         = "/api/keystone/short_term_token/name"
	StarLightStorageOperationInterface = "/api/storage/operation"
	StarLightUploadInterface           = "/api/storage/upload"
	StarLightListInterface             = "/api/storage/dir_info"
	StarLightDownloadInterface         = "/api/storage/download"

	StarLightStorageOperationMkdir = "mkdir"
	StarLightStorageOperationRm    = "rm"

	SLObjectTypeFile   = 0
	SLObjectTypeFolder = 1
)

type StarLightUploadInput struct {
	SourcePath string
	TargetPath string
	NeedPure   bool
}

type StarLightDownloadInput struct {
	SourcePath string
	TargetPath string
}

type SLBaseResponse struct {
	Uuid  string `json:"uuid"`
	Code  int32  `json:"code"`
	Info  string `json:"info"`
	Kind  string `json:"kind"`
	Total int32  `json:"total"`
	Spec  string `json:"spec"`
}

type SLGetTokenReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SLStorageOperationReq struct {
	From      string `json:"from" url:"from"`
	Target    string `json:"target" url:"target"`
	Force     string `json:"force" url:"force"`
	Recursive string `json:"recursive" url:"recursive"`
	Opt       string `json:"opt" url:"opt"`
	Mod       string `json:"mod" url:"mod"`
}

type SLUploadChunksReq struct {
	File      string `json:"file" url:"file"`
	Overwrite string `json:"overwrite" url:"overwrite"`
}

type SLUploadChunksResponse struct {
	Uuid  string                     `json:"uuid"`
	Code  int32                      `json:"code"`
	Info  string                     `json:"info"`
	Kind  string                     `json:"kind"`
	Total int32                      `json:"total"`
	Spec  SLUploadChunksResponseSpec `json:"spec"`
}

type SLUploadChunksResponseSpec struct {
	File    string `json:"File"`
	Written int64  `json:"Written"`
}

type SLListReq struct {
	Dir        string `json:"dir" url:"dir"`
	ShowHidden string `json:"show_hidden" url:"show_hidden"`
}

type SLListOutput struct {
	Uuid  string      `json:"uuid"`
	Code  int32       `json:"code"`
	Info  string      `json:"info"`
	Kind  string      `json:"kind"`
	Total int32       `json:"total"`
	Spec  []*SLObject `json:"spec"`
}

type SLDownloadReq struct {
	File string `json:"file" url:"file"`
}

type SLObject struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Size int64  `json:"size"`
	Type int32  `json:"type"`
	Perm string `json:"perm"`
	Time string `json:"time"`
	Uid  int64  `json:"uid"`
	Gid  int64  `json:"gid"`
}

type SLUploadFileInput struct {
	ObjectPath       string
	UploadFile       string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
	EncodingType     string
}

type SLFileStatus struct {
	XMLName      xml.Name `xml:"FileInfo"`
	LastModified int64    `xml:"LastModified"`
	Size         int64    `xml:"Size"`
}

type SLUploadPartInfo struct {
	XMLName     xml.Name `xml:"UploadPart"`
	PartNumber  int32    `xml:"PartNumber"`
	PartSize    int64    `xml:"PartSize"`
	Offset      int64    `xml:"Offset"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type SLUploadCheckpoint struct {
	XMLName     xml.Name           `xml:"UploadFileCheckpoint"`
	ObjectPath  string             `xml:"ObjectPath"`
	UploadFile  string             `xml:"FileUrl"`
	FileInfo    SLFileStatus       `xml:"FileInfo"`
	UploadParts []SLUploadPartInfo `xml:"UploadParts>UploadPart"`
}

func (ufc *SLUploadCheckpoint) IsValid(
	ctx context.Context,
	uploadFile string,
	fileStat os.FileInfo) bool {

	Logger.WithContext(ctx).Debug(
		"SLUploadCheckpoint:isValid start.",
		" uploadFile: ", uploadFile,
		" ufc.UploadFile: ", ufc.UploadFile,
		" fileStat.Size: ", fileStat.Size(),
		" ufc.FileInfo.Size: ", ufc.FileInfo.Size)

	if ufc.UploadFile != uploadFile {
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

	Logger.WithContext(ctx).Debug(
		"SLUploadCheckpoint:isValid finish.")
	return true
}

type SLDownloadFileInput struct {
	DownloadFile     string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
}

type SLObjectInfo struct {
	XMLName xml.Name `xml:"ObjectInfo"`
	Size    int64    `xml:"Size"`
}

type SLTempFileInfo struct {
	XMLName     xml.Name `xml:"TempFileInfo"`
	TempFileUrl string   `xml:"TempFileUrl"`
	Size        int64    `xml:"Size"`
}

type SLDownloadPartInfo struct {
	XMLName     xml.Name `xml:"DownloadPart"`
	PartNumber  int64    `xml:"PartNumber"`
	Offset      int64    `xml:"Offset"`
	Length      int64    `xml:"Length"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type SLDownloadPartOutput struct {
	Body io.ReadCloser
}

type SLDownloadCheckpoint struct {
	XMLName       xml.Name             `xml:"DownloadFileCheckpoint"`
	ObjectPath    string               `xml:"ObjectPath"`
	DownloadFile  string               `xml:"FileUrl"`
	ObjectInfo    SLObjectInfo         `xml:"ObjectInfo"`
	TempFileInfo  SLTempFileInfo       `xml:"TempFileInfo"`
	DownloadParts []SLDownloadPartInfo `xml:"DownloadParts>DownloadPart"`
}

func (dfc *SLDownloadCheckpoint) IsValid(
	ctx context.Context,
	input *SLDownloadFileInput,
	object *SLObject) bool {

	Logger.WithContext(ctx).Debug(
		"SLDownloadCheckpoint:IsValid start.",
		" dfc.DownloadFile: ", dfc.DownloadFile,
		" input.DownloadFile: ", input.DownloadFile,
		" dfc.ObjectInfo.Size: ", dfc.ObjectInfo.Size,
		" object.Size: ", object.Size,
		" dfc.TempFileInfo.Size: ", dfc.TempFileInfo.Size)

	if dfc.DownloadFile != input.DownloadFile {
		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" downloadFile was changed.",
			" clear the record.")
		return false
	}
	if dfc.ObjectInfo.Size != object.Size {
		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" the object info was changed.",
			" clear the record.")
		return false
	}
	if dfc.TempFileInfo.Size != object.Size {
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
		"SLDownloadCheckpoint:IsValid finish.")
	return true
}
