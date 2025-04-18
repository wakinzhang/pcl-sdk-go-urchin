package adapter

import (
	"context"
	"errors"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adaptee"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
)

type Storage interface {
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

func NewStorage(
	ctx context.Context,
	nodeType int32) (err error, storage Storage) {

	Logger.WithContext(ctx).Debug(
		"NewStorage start. nodeType: ", nodeType)
	if StorageCategoryEObs == nodeType ||
		StorageCategoryEMinio == nodeType {
		var s3 S3
		err = s3.Init(ctx)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"s3.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage S3 finish.")
		return nil, &s3
	} else if StorageCategoryEIpfs == nodeType {
		Logger.WithContext(ctx).Debug(
			"NewStorage IPFS finish.")
		return nil, new(IPFS)
	} else if StorageCategoryEJcs == nodeType {
		var jcs JCS
		err = jcs.Init(
			ctx,
			DefaultJCSClientReqTimeout,
			DefaultJCSClientMaxConnection)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage JCS finish.")
		return nil, &jcs
	} else {
		Logger.WithContext(ctx).Error(
			"invalid storage node type.")
		return errors.New("invalid storage node type"), storage
	}
}
