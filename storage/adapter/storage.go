package adapter

import (
	"context"
	"errors"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adaptee"
)

type Storage interface {
	Mkdir(
		ctx context.Context,
		input interface{}) error

	Upload(
		ctx context.Context,
		input interface{}) error

	Download(
		ctx context.Context,
		input interface{}) error
}

func NewStorage(
	ctx context.Context,
	nodeType int32,
	storageNodeConfig *StorageNodeConfig) (
	err error, storage Storage) {

	Logger.WithContext(ctx).Debug(
		"NewStorage start. nodeType: ", nodeType)
	switch nodeType {
	case StorageCategoryEObs,
		StorageCategoryEMinio,
		StorageCategoryEEos:

		var s3 S3
		err = s3.Init(
			ctx,
			storageNodeConfig.AccessKey,
			storageNodeConfig.SecretKey,
			storageNodeConfig.Endpoint,
			storageNodeConfig.BucketName)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"S3.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage S3 finish.")
		return nil, &s3
	case StorageCategoryEJcs:

		var jcs JCS
		err = jcs.Init(
			ctx,
			storageNodeConfig.AccessKey,
			storageNodeConfig.SecretKey,
			storageNodeConfig.Endpoint,
			storageNodeConfig.AuthService,
			storageNodeConfig.AuthRegion,
			storageNodeConfig.UserId,
			storageNodeConfig.BucketId,
			storageNodeConfig.BucketName,
			storageNodeConfig.ReqTimeout,
			storageNodeConfig.MaxConnection)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"JCS.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage JCS finish.")
		return nil, &jcs
	case StorageCategoryEStarLight:

		var starLight StarLight
		err = starLight.Init(
			ctx,
			storageNodeConfig.User,
			storageNodeConfig.Pass,
			storageNodeConfig.Endpoint,
			storageNodeConfig.ReqTimeout,
			storageNodeConfig.MaxConnection)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"StarLight.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage StarLight finish.")
		return nil, &starLight
	case StorageCategoryEParaCloud:

		var paraCloud ParaCloud
		err = paraCloud.Init(
			ctx,
			storageNodeConfig.User,
			storageNodeConfig.Pass,
			storageNodeConfig.Endpoint)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"ParaCloud.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage ParaCloud finish.")
		return nil, &paraCloud
	case StorageCategoryEScow:

		var scow Scow
		err = scow.Init(
			ctx,
			storageNodeConfig.User,
			storageNodeConfig.Pass,
			storageNodeConfig.Endpoint,
			storageNodeConfig.Url,
			storageNodeConfig.ClusterId,
			storageNodeConfig.ReqTimeout,
			storageNodeConfig.MaxConnection)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"Scow.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage Scow finish.")
		return nil, &scow
	case StorageCategoryESugon:

		var sugon Sugon
		err = sugon.Init(
			ctx,
			storageNodeConfig.User,
			storageNodeConfig.Pass,
			storageNodeConfig.Endpoint,
			storageNodeConfig.Url,
			storageNodeConfig.OrgId,
			storageNodeConfig.ClusterId,
			storageNodeConfig.ReqTimeout,
			storageNodeConfig.MaxConnection)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"Sugon.Init failed.",
				" err: ", err)
			return err, storage
		}
		Logger.WithContext(ctx).Debug(
			"NewStorage Sugon finish.")
		return nil, &sugon
	default:

		Logger.WithContext(ctx).Error(
			"invalid storage node type.")
		return errors.New("invalid storage node type"), storage
	}
}
