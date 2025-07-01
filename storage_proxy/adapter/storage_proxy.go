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
		userId string,
		sourcePath string,
		taskId int32,
		needPure bool) error

	Download(
		ctx context.Context,
		userId string,
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
		var s3Proxy S3Proxy
		err = s3Proxy.Init(ctx, nodeType)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3Proxy.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage S3Proxy finish.")
		return nil, &s3Proxy
	} else if StorageCategoryEIpfs == nodeType {
		Logger.WithContext(ctx).Debug(
			"NewStorage IPFSProxy finish.")
		return nil, new(IPFSProxy)
	} else if StorageCategoryEJcs == nodeType {
		var jcsProxy JCSProxy
		err = jcsProxy.Init(
			ctx,
			DefaultJCSReqTimeout,
			DefaultJCSMaxConnection)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCSProxy.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage JCSProxy finish.")
		return nil, &jcsProxy
	} else {
		Logger.WithContext(ctx).Error(
			"invalid storage node type.")
		return errors.New("invalid storage node type"), storage
	}
}
