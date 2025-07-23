package module

import (
	"context"
	"encoding/xml"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	"go.uber.org/zap"
	"io"
	"os"
)

type PCDownloadPartOutput struct {
	Body io.ReadCloser
}

const (
	ParaCloudLockConflictCode = 423

	ParaCloudAttempts = 5
	ParaCloudDelay    = 3

	DefaultParaCloudUploadFileTaskNum    = 1
	DefaultParaCloudDownloadFileTaskNum  = 100
	DefaultParaCloudDownloadMultiTaskNum = 20

	DefaultParaCloudRateLimit = 65535
	DefaultParaCloudRateBurst = 65535
)

type ParaCloudMkdirInput struct {
	SourceFolder string
	TargetFolder string
}

type ParaCloudUploadInput struct {
	SourcePath string
	TargetPath string
	NeedPure   bool
}

type ParaCloudDownloadInput struct {
	SourcePath string
	TargetPath string
}

type ParaCloudDeleteInput struct {
	Path string
}

type PCObject struct {
	ObjectPath     string
	ObjectFileInfo os.FileInfo
}

type PCObjectInfo struct {
	XMLName xml.Name `xml:"ObjectInfo"`
	Size    int64    `xml:"Size"`
}

type PCTempFileInfo struct {
	XMLName     xml.Name `xml:"TempFileInfo"`
	TempFileUrl string   `xml:"TempFileUrl"`
	Size        int64    `xml:"Size"`
}

type PCDownloadPartInfo struct {
	XMLName     xml.Name `xml:"DownloadPart"`
	PartNumber  int64    `xml:"PartNumber"`
	Offset      int64    `xml:"Offset"`
	Length      int64    `xml:"Length"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type PCDownloadFileInput struct {
	DownloadFile     string
	PartSize         int64
	TaskNum          int
	EnableCheckpoint bool
	CheckpointFile   string
}

type PCDownloadCheckpoint struct {
	XMLName       xml.Name             `xml:"DownloadFileCheckpoint"`
	ObjectPath    string               `xml:"ObjectPath"`
	DownloadFile  string               `xml:"FileUrl"`
	ObjectInfo    PCObjectInfo         `xml:"ObjectInfo"`
	TempFileInfo  PCTempFileInfo       `xml:"TempFileInfo"`
	DownloadParts []PCDownloadPartInfo `xml:"DownloadParts>DownloadPart"`
}

func (dfc *PCDownloadCheckpoint) IsValid(
	ctx context.Context,
	input *PCDownloadFileInput,
	object *PCObject) bool {

	InfoLogger.WithContext(ctx).Debug(
		"PCDownloadCheckpoint:IsValid start.",
		zap.String("dfcDownloadFile", dfc.DownloadFile),
		zap.String("inputDownloadFile", input.DownloadFile),
		zap.Int64("dfcObjectInfoSize", dfc.ObjectInfo.Size),
		zap.Int64("objectSize", object.ObjectFileInfo.Size()),
		zap.Int64("dfcTempFileInfoSize: ", dfc.TempFileInfo.Size))

	if dfc.DownloadFile != input.DownloadFile {
		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid. downloadFile was changed." +
				" clear the record.")
		return false
	}
	if dfc.ObjectInfo.Size != object.ObjectFileInfo.Size() {
		InfoLogger.WithContext(ctx).Info(
			"Checkpoint file is invalid. the object info was changed." +
				" clear the record.")
		return false
	}
	if dfc.TempFileInfo.Size != object.ObjectFileInfo.Size() {
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
		"PCDownloadCheckpoint:IsValid finish.")
	return true
}
