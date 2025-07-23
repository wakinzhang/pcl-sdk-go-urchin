package common

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var LogLevelMap = map[int64]zapcore.Level{
	-1: zap.DebugLevel,
	0:  zap.InfoLevel,
	1:  zap.WarnLevel,
	2:  zap.ErrorLevel,
	3:  zap.DPanicLevel,
	4:  zap.PanicLevel,
	5:  zap.FatalLevel}

const (
	DefaultUrchinClientUserId = "test"
	DefaultUrchinClientToken  = "test"

	DefaultPartSize = 100 * 1024 * 1024

	DefaultPageIndex = 1
	DefaultPageSize  = 10

	HttpHeaderContentType           = "Content-Type"
	HttpHeaderContentTypeJson       = "application/json"
	HttpHeaderContentTypeText       = "text/plain"
	HttpHeaderContentTypeStream     = "application/octet-stream"
	HttpHeaderContentTypeFormData   = "form-data"
	HttpHeaderContentTypeUrlEncoded = "application/x-www-form-urlencoded"

	HttpHeaderContentLength = "Content-Length"

	HttpHeaderRange        = "Range"
	HttpHeaderContentRange = "Content-Range"

	UploadFileRecordSuffix     = ".upload_file_record"
	UploadFolderRecordSuffix   = ".upload_folder_record"
	DownloadFileRecordSuffix   = ".download_file_record"
	DownloadFolderRecordSuffix = ".download_folder_record"
)
