package common

import (
	"bytes"
	"encoding/xml"
	"io"
	"os"
	"strings"
)

type CompleteMultipartUploadPart struct {
	XMLName   xml.Name  `xml:"CompleteMultipartUpload"`
	PartSlice PartSlice `xml:"Part"`
}

type PartSlice []XPart

type XPart struct {
	XMLName    xml.Name `xml:"Part"`
	PartNumber int      `xml:"PartNumber"`
	ETag       string   `xml:"ETag"`
	Result     int      `xml:"Result"`
}

func (parts PartSlice) Len() int {
	return len(parts)
}

func (parts PartSlice) Less(i, j int) bool {
	return parts[i].PartNumber < parts[j].PartNumber
}

func (parts PartSlice) Swap(i, j int) {
	parts[i], parts[j] = parts[j], parts[i]
}

type ReaderWrapper struct {
	Reader      io.Reader
	Mark        int64
	TotalCount  int64
	ReadedCount int64
}

func (rw *ReaderWrapper) seek(offset int64, whence int) (int64, error) {
	if r, ok := rw.Reader.(*strings.Reader); ok {
		return r.Seek(offset, whence)
	} else if r, ok := rw.Reader.(*bytes.Reader); ok {
		return r.Seek(offset, whence)
	} else if r, ok := rw.Reader.(*os.File); ok {
		return r.Seek(offset, whence)
	}
	return offset, nil
}

func (rw *ReaderWrapper) Read(p []byte) (n int, err error) {
	if rw.TotalCount == 0 {
		return 0, io.EOF
	}
	if rw.TotalCount > 0 {
		n, err = rw.Reader.Read(p)
		readedOnce := int64(n)
		remainCount := rw.TotalCount - rw.ReadedCount
		if remainCount > readedOnce {
			rw.ReadedCount += readedOnce
			return n, err
		}
		rw.ReadedCount += remainCount
		return int(remainCount), io.EOF
	}
	return rw.Reader.Read(p)
}
