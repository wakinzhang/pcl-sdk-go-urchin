package client

import (
	"context"
	"errors"
	"github.com/studio-b12/gowebdav"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"os"
	"time"
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

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Init start.",
		" username: ", "***",
		" password: ", "***",
		" endpoint: ", endpoint)

	o.username = username
	o.password = password
	o.endpoint = endpoint
	o.pcClient = gowebdav.NewClient(endpoint, username, password)

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Init finish.")
}

func (o *ParaCloudClient) Mkdir(
	ctx context.Context,
	sourceFolder,
	targetFolder string) (err error) {

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Mkdir start.",
		" sourceFolder: ", sourceFolder,
		" targetFolder: ", targetFolder)

	fileMode := os.ModePerm
	if "" != sourceFolder {
		stat, err := os.Stat(sourceFolder)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"os.Stat failed.",
				" sourceFolder: ", sourceFolder,
				" err: ", err)
			return err
		}
		fileMode = stat.Mode()
	}

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			return o.pcClient.MkdirAll(targetFolder, fileMode)
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"pcClient.MkdirAll failed.",
			" sourceFolder: ", sourceFolder,
			" targetFolder: ", targetFolder,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Mkdir finish.")
	return err
}

func (o *ParaCloudClient) Upload(
	ctx context.Context,
	sourceFile,
	targetFile string) (err error) {

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Upload start.",
		" sourceFile: ", sourceFile,
		" targetFile: ", targetFile)

	fd, err := os.Open(sourceFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Open failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}
	defer func() {
		errMsg := fd.Close()
		if errMsg != nil && !errors.Is(errMsg, os.ErrClosed) {
			Logger.WithContext(ctx).Warn(
				"close file failed.",
				" sourceFile: ", sourceFile,
				" err: ", errMsg)
		}
	}()

	stat, err := os.Stat(sourceFile)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" sourceFile: ", sourceFile,
			" err: ", err)
		return err
	}

	readerWrapper := new(ReaderWrapper)
	readerWrapper.Reader = fd

	readerWrapper.TotalCount = stat.Size()
	readerWrapper.Mark = 0

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			return o.pcClient.WriteStream(
				targetFile,
				readerWrapper,
				stat.Mode())
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"pcClient.WriteStream failed.",
			" sourceFile: ", sourceFile,
			" targetFile: ", targetFile,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Upload finish.")
	return err
}

func (o *ParaCloudClient) List(
	ctx context.Context,
	path string) (err error, fileInfoList []os.FileInfo) {

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:List start.",
		" path: ", path)

	fileInfoList = make([]os.FileInfo, 0)
	fileInfoList, err = o.pcClient.ReadDir(path)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"pcClient.ReadDir failed.",
			" path: ", path,
			" err: ", err)
		return err, fileInfoList
	}

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:List finish.")
	return err, fileInfoList
}

func (o *ParaCloudClient) Download(
	ctx context.Context,
	sourceFile string,
	offset,
	length int64) (err error, output *PCDownloadPartOutput) {

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Download start.",
		" sourceFile: ", sourceFile,
		" offset: ", offset,
		" length: ", length)

	ioReadCloser, err := o.pcClient.ReadStreamRange(sourceFile, offset, length)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"pcClient.ReadStreamRange failed.",
			" sourceFile: ", sourceFile,
			" offset: ", offset,
			" length: ", length,
			" err: ", err)
		return err, output
	}

	output = new(PCDownloadPartOutput)
	output.Body = ioReadCloser

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Download finish.")
	return err, output
}

func (o *ParaCloudClient) Rm(
	ctx context.Context,
	path string) (err error) {

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Rm start.",
		" path: ", path)

	err = RetryV1(
		ctx,
		Attempts,
		Delay*time.Second,
		func() error {
			return o.pcClient.RemoveAll(path)
		})
	if nil != err {
		Logger.WithContext(ctx).Error(
			"pcClient.RemoveAll failed.",
			" path: ", path,
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"ParaCloudClient:Rm finish.")
	return err
}
