package adapter

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/adaptee"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
)

type Storage interface {
	Upload(urchinServiceAddr, sourcePath string, taskId int32) error
	Download()
	Migrate()
}

func NewStorage(nodeType int32) (err error, storage Storage) {
	if StorageCategoryEObs == nodeType {
		var obsAdapteeWithSignedUrl ObsAdapteeWithSignedUrl
		err = obsAdapteeWithSignedUrl.Init()
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"ObsAdapteeWithSignedUrl.Init failed. error: %v", err)
			return err, storage
		}
		return nil, &obsAdapteeWithSignedUrl
	}
	return nil, storage
}
