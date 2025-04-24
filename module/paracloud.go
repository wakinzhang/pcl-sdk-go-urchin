package module

import (
	"context"
	"encoding/xml"
	"io"
	"os"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
)

type PCDownloadPartOutput struct {
	Body io.ReadCloser
}

const (
	DefaultParaCloudUploadFileTaskNum    = 100
	DefaultParaCloudDownloadFileTaskNum  = 100
	DefaultParaCloudDownloadMultiTaskNum = 20
)

type ParaCloudUploadInput struct {
	SourcePath string
	TargetPath string
	NeedPure   bool
}

type ParaCloudDownloadInput struct {
	SourcePath string
	TargetPath string
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
	DownloadFile  string               `xml:"FileUrl"`
	ObjectInfo    PCObjectInfo         `xml:"ObjectInfo"`
	TempFileInfo  PCTempFileInfo       `xml:"TempFileInfo"`
	DownloadParts []PCDownloadPartInfo `xml:"DownloadParts>DownloadPart"`
}

func (dfc *PCDownloadCheckpoint) IsValid(
	ctx context.Context,
	input *PCDownloadFileInput,
	object *PCObject) bool {

	Logger.WithContext(ctx).Debug(
		"PCDownloadCheckpoint:IsValid start.",
		" dfc.DownloadFile: ", dfc.DownloadFile,
		" input.DownloadFile: ", input.DownloadFile,
		" dfc.ObjectInfo.Size: ", dfc.ObjectInfo.Size,
		" object.Size: ", object.ObjectFileInfo.Size(),
		" dfc.TempFileInfo.Size: ", dfc.TempFileInfo.Size)

	if dfc.DownloadFile != input.DownloadFile {
		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" downloadFile was changed.",
			" clear the record.")
		return false
	}
	if dfc.ObjectInfo.Size != object.ObjectFileInfo.Size() {
		Logger.WithContext(ctx).Info(
			"Checkpoint file is invalid.",
			" the object info was changed.",
			" clear the record.")
		return false
	}
	if dfc.TempFileInfo.Size != object.ObjectFileInfo.Size() {
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
		"PCDownloadCheckpoint:IsValid finish.")
	return true
}
