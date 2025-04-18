package module

import (
	"context"
	"encoding/xml"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	"io"
	"os"
	"strconv"
)

const (
	JCSSuccessCode = "OK"

	DefaultJCSUploadMultiSize = 500 * 1024 * 1024

	DefaultJCSUploadFileTaskNum    = 100
	DefaultJCSUploadMultiTaskNum   = 20
	DefaultJCSDownloadFileTaskNum  = 100
	DefaultJCSDownloadMultiTaskNum = 20
	DefaultJCSMaxPartSize          = 5 * 1024 * 1024 * 1024
	DefaultJCSMinPartSize          = 100 * 1024

	DefaultJCSClientReqTimeout    = 3600
	DefaultJCSClientMaxConnection = 50
)

type JCSBaseResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type JCSListResponse struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Data    *JCSListData `json:"data"`
}

type JCSListData struct {
	NextContinuationToken string       `json:"nextContinuationToken"`
	IsTruncated           bool         `json:"isTruncated"`
	CommonPrefixes        []string     `json:"commonPrefixes"`
	Objects               []*JCSObject `json:"objects"`
}

type JCSNewMultiPartUploadResponse struct {
	Code    string                     `json:"code"`
	Message string                     `json:"message"`
	Data    *JCSNewMultiPartUploadData `json:"data"`
}

type JCSNewMultiPartUploadData struct {
	Object *JCSObject `json:"object"`
}

type JCSObject struct {
	ObjectID   int32                `json:"objectID"`
	PackageID  int32                `json:"packageID"`
	Path       string               `json:"path"`
	Size       string               `json:"size"`
	FileHash   string               `json:"fileHash"`
	Redundancy *JCSObjectRedundancy `json:"redundancy"`
	CreateTime string               `json:"createTime"`
	UpdateTime string               `json:"updateTime"`
}

type JCSObjectRedundancy struct {
	Type string `json:"type"`
}

type JCSUploadFileInput struct {
	ObjectPath       string
	UploadFile       string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
	EncodingType     string
}

type JCSFileStatus struct {
	XMLName      xml.Name `xml:"FileInfo"`
	LastModified int64    `xml:"LastModified"`
	Size         int64    `xml:"Size"`
}

type JCSUploadPartInfo struct {
	XMLName     xml.Name `xml:"UploadPart"`
	PartNumber  int32    `xml:"PartNumber"`
	PartSize    int64    `xml:"PartSize"`
	Offset      int64    `xml:"Offset"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type JCSUploadCheckpoint struct {
	XMLName     xml.Name            `xml:"UploadFileCheckpoint"`
	ObjectId    int32               `xml:"ObjectId"`
	ObjectPath  string              `xml:"ObjectPath"`
	UploadFile  string              `xml:"FileUrl"`
	FileInfo    JCSFileStatus       `xml:"FileInfo"`
	UploadParts []JCSUploadPartInfo `xml:"UploadParts>UploadPart"`
}

func (ufc *JCSUploadCheckpoint) IsValid(
	ctx context.Context,
	uploadFile string,
	fileStat os.FileInfo) bool {

	Logger.WithContext(ctx).Debug(
		"JCSUploadCheckpoint:isValid start.",
		" ufc.ObjectId: ", ufc.ObjectId,
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
		"JCSUploadCheckpoint:isValid finish.")
	return true
}

type JCSDownloadFileInput struct {
	Path             string
	DownloadFile     string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
}

type JCSObjectInfo struct {
	XMLName xml.Name `xml:"ObjectInfo"`
	Size    int64    `xml:"Size"`
}

type JCSTempFileInfo struct {
	XMLName     xml.Name `xml:"TempFileInfo"`
	TempFileUrl string   `xml:"TempFileUrl"`
	Size        int64    `xml:"Size"`
}

type JCSDownloadPartInfo struct {
	XMLName     xml.Name `xml:"DownloadPart"`
	PartNumber  int64    `xml:"PartNumber"`
	Offset      int64    `xml:"Offset"`
	Length      int64    `xml:"Length"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type JCSDownloadPartOutput struct {
	Body io.ReadCloser
}

type JCSDownloadCheckpoint struct {
	XMLName       xml.Name              `xml:"DownloadFileCheckpoint"`
	ObjectId      int32                 `xml:"ObjectId"`
	DownloadFile  string                `xml:"FileUrl"`
	ObjectInfo    JCSObjectInfo         `xml:"ObjectInfo"`
	TempFileInfo  JCSTempFileInfo       `xml:"TempFileInfo"`
	DownloadParts []JCSDownloadPartInfo `xml:"DownloadParts>DownloadPart"`
}

func (dfc *JCSDownloadCheckpoint) IsValid(
	ctx context.Context,
	input *JCSDownloadFileInput,
	object *JCSObject) bool {

	Logger.WithContext(ctx).Debug(
		"JCSDownloadCheckpoint:IsValid start.",
		" dfc.ObjectId: ", dfc.ObjectId,
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
	objectSize, _ := strconv.ParseInt(object.Size, 10, 64)
	if dfc.ObjectInfo.Size != objectSize {
		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" the object info was changed.",
			" clear the record.")
		return false
	}
	if dfc.TempFileInfo.Size != objectSize {
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
		"JCSDownloadCheckpoint:IsValid finish.")
	return true
}
