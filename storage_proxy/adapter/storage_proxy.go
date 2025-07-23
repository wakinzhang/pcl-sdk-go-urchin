package adapter

import (
	"context"
	"errors"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage_proxy/adaptee"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
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

	SetConcurrency(
		ctx context.Context,
		config *StorageNodeConcurrencyConfig) error

	SetRate(
		ctx context.Context,
		rateLimiter *rate.Limiter) error
}

func NewStorageProxy(
	ctx context.Context,
	nodeType int32) (err error, storage StorageProxy) {

	InfoLogger.WithContext(ctx).Debug(
		"NewStorageProxy start.",
		zap.Int32("nodeType", nodeType))
	if StorageCategoryEObs == nodeType ||
		StorageCategoryEMinio == nodeType {
		var s3Proxy S3Proxy
		err = s3Proxy.Init(
			ctx,
			nodeType,
			DefaultS3ReqTimeout,
			DefaultS3MaxConnection)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"S3Proxy.Init failed.",
				zap.Error(err))
			return err, storage
		}
		InfoLogger.WithContext(ctx).Debug(
			"NewStorage S3Proxy finish.")
		return nil, &s3Proxy
	} else if StorageCategoryEIpfs == nodeType {
		InfoLogger.WithContext(ctx).Debug(
			"NewStorage IPFSProxy finish.")
		return nil, new(IPFSProxy)
	} else if StorageCategoryEJcs == nodeType {
		var jcsProxy JCSProxy
		err = jcsProxy.Init(
			ctx,
			DefaultJCSReqTimeout,
			DefaultJCSMaxConnection)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"JCSProxy.Init failed.",
				zap.Error(err))
			return err, storage
		}
		InfoLogger.WithContext(ctx).Debug(
			"NewStorage JCSProxy finish.")
		return nil, &jcsProxy
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"invalid storage node type.")
		return errors.New("invalid storage node type"), storage
	}
}
