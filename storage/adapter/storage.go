package adapter

import (
	"errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adaptee"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
)

type Storage interface {
	Upload(urchinServiceAddr, sourcePath string, taskId int32) error
	Download(urchinServiceAddr, targetFile string, taskId int32, bucketName string) error
}

func NewStorage(nodeType int32) (err error, storage Storage) {
	obs.DoLog(obs.LEVEL_DEBUG, "NewStorage start. nodeType: %d", nodeType)
	if StorageCategoryEObs == nodeType ||
		StorageCategoryEMinio == nodeType {
		var obsAdapteeWithSignedUrl ObsAdapteeWithSignedUrl
		err = obsAdapteeWithSignedUrl.Init()
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"ObsAdapteeWithSignedUrl.Init failed. error: %v", err)
			return err, storage
		}
		return nil, &obsAdapteeWithSignedUrl
	} else {
		obs.DoLog(obs.LEVEL_ERROR, "invalid storage node type")
		return errors.New("invalid storage node type"), storage
	}
	obs.DoLog(obs.LEVEL_DEBUG, "NewStorage finish")
	return nil, storage
}
