package client

import (
	"context"
	"errors"
	"github.com/studio-b12/gowebdav"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
	"os"
	// ParaCloud "github.com/urchinfs/paracloud-sdk/paracloud"
)

type ParaCloudClient struct {
	username string
	password string
	endpoint string
	pcClient *gowebdav.Client
}

func (o *ParaCloudClient) Init(
	ctx context.Context,
	username,
	password,
	endpoint string) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Init start.",
		zap.String("endpoint", endpoint))

	o.username = username
	o.password = password
	o.endpoint = endpoint
	o.pcClient = gowebdav.NewClient(endpoint, username, password)

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Init finish.")
}

func (o *ParaCloudClient) Mkdir(
	ctx context.Context,
	sourceFolder,
	targetFolder string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Mkdir start.",
		zap.String("sourceFolder", sourceFolder),
		zap.String("targetFolder", targetFolder))

	fileMode := os.ModePerm
	if "" != sourceFolder {
		stat, err := os.Stat(sourceFolder)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"os.Stat failed.",
				zap.String("sourceFolder", sourceFolder),
				zap.Error(err))
			return err
		}
		fileMode = stat.Mode()
	}

	err = o.pcClient.MkdirAll(targetFolder, fileMode)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"pcClient.MkdirAll failed.",
			zap.String("sourceFolder", sourceFolder),
			zap.String("targetFolder", targetFolder),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Mkdir finish.")
	return err
}

func (o *ParaCloudClient) Upload(
	ctx context.Context,
	sourceFile,
	targetFile string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Upload start.",
		zap.String("sourceFile", sourceFile),
		zap.String("targetFile", targetFile))

	fd, err := os.Open(sourceFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Open failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			ErrorLogger.WithContext(ctx).Warn(
				"close file failed.",
				zap.String("sourceFile", sourceFile),
				zap.Error(errMsg))
		}
	}()

	stat, err := os.Stat(sourceFile)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"os.Stat failed.",
			zap.String("sourceFile", sourceFile),
			zap.Error(err))
		return err
	}

	readerWrapper := new(ReaderWrapper)
	readerWrapper.Reader = fd

	readerWrapper.TotalCount = stat.Size()
	readerWrapper.Mark = 0

	err = o.pcClient.WriteStream(targetFile, readerWrapper, stat.Mode())
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"pcClient.WriteStream failed.",
			zap.String("sourceFile", sourceFile),
			zap.String("targetFile", targetFile),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Upload finish.")
	return err
}

func (o *ParaCloudClient) List(
	ctx context.Context,
	path string) (err error, fileInfoList []os.FileInfo) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:List start.",
		zap.String("path", path))

	fileInfoList = make([]os.FileInfo, 0)

	fileInfoList, err = o.pcClient.ReadDir(path)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"pcClient.ReadDir failed.",
			zap.String("path", path),
			zap.Error(err))
		return err, fileInfoList
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:List finish.")
	return err, fileInfoList
}

func (o *ParaCloudClient) Download(
	ctx context.Context,
	sourceFile string,
	offset,
	length int64) (err error, output *PCDownloadPartOutput) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Download start.",
		zap.String("sourceFile", sourceFile),
		zap.Int64("offset", offset),
		zap.Int64("length", length))

	ioReadCloser, err := o.pcClient.ReadStreamRange(
		sourceFile,
		offset,
		length)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"pcClient.ReadStreamRange failed.",
			zap.String("sourceFile", sourceFile),
			zap.Int64("offset", offset),
			zap.Int64("length", length),
			zap.Error(err))
		return err, output
	}

	output = new(PCDownloadPartOutput)
	output.Body = ioReadCloser

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Download finish.")
	return err, output
}

func (o *ParaCloudClient) Rm(
	ctx context.Context,
	path string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Rm start.",
		zap.String("path", path))

	err = o.pcClient.RemoveAll(path)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"pcClient.RemoveAll failed.",
			zap.String("path", path),
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ParaCloudClient:Rm finish.")
	return err
}
