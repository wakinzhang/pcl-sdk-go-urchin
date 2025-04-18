package common

import "github.com/sirupsen/logrus"

var LogLevelMap = map[int64]logrus.Level{
	0: logrus.PanicLevel,
	1: logrus.FatalLevel,
	2: logrus.ErrorLevel,
	3: logrus.WarnLevel,
	4: logrus.InfoLevel,
	5: logrus.DebugLevel,
	6: logrus.TraceLevel}

const (
	DefaultPartSize = 100 * 1024 * 1024

	DefaultPageIndex = 1
	DefaultPageSize  = 10

	HttpHeaderContentType           = "Content-Type"
	HttpHeaderContentTypeJson       = "application/json"
	HttpHeaderContentTypeText       = "text/plain"
	HttpHeaderContentTypeStream     = "application/octet-stream"
	HttpHeaderContentTypeData       = "multipart/form-data"
	HttpHeaderContentTypeUrlEncoded = "multipart/application/x-www-from" +
		"-urlencoded"

	HttpHeaderRange        = "Range"
	HttpHeaderContentRange = "Content-Range"
)
