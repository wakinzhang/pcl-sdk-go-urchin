package common

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type Log struct {
}

func (l *Log) Init() {
	// 设置日志文件存放的路径
	var logFullPath = "./logs/Urchin-SDK.log"
	// 设置每个日志文件的大小，单位：字节
	var maxLogSize int64 = 1024 * 1024 * 100
	// 设置保留日志文件的个数
	var backups = 10
	// 设置日志的级别
	var level = obs.LEVEL_DEBUG

	// 开启日志
	err := obs.InitLog(logFullPath, maxLogSize, backups, level, true)
	if err != nil {
		return
	}
}
