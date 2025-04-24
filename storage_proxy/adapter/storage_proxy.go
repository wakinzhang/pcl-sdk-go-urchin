package adapter

import (
	"context"
	"errors"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage_proxy/adaptee"
)

type StorageProxy interface {
	Upload(
		ctx context.Context,
		sourcePath string,
		taskId int32,
		needPure bool) error

	Download(
		ctx context.Context,
		targetPath string,
		taskId int32,
		bucketName string) error
}

func NewStorageProxy(
	ctx context.Context,
	nodeType int32) (err error, storage StorageProxy) {

	Logger.WithContext(ctx).Debug(
		"NewStorageProxy start. nodeType: ", nodeType)
	if StorageCategoryEObs == nodeType ||
		StorageCategoryEMinio == nodeType {
		var s3proxy S3Proxy
		err = s3proxy.Init(ctx)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"s3proxy.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage S3Proxy finish.")
		return nil, &s3proxy
	} else if StorageCategoryEIpfs == nodeType {
		Logger.WithContext(ctx).Debug(
			"NewStorage IPFSProxy finish.")
		return nil, new(IPFSProxy)
	} else {
		Logger.WithContext(ctx).Error(
			"invalid storage node type.")
		return errors.New("invalid storage node type"), storage
	}
}
