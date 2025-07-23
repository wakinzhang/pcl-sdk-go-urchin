package common

import (
	"context"
	"go.uber.org/zap"
	"time"
)

const (
	Attempts = 3
	Delay    = 1
)

// RetryV1
/*
 * gowebdav.IsErrCode(err, ParaCloudLockConflictCode)
 */
func RetryV1(
	ctx context.Context,
	attempts int,
	delay time.Duration,
	fn func() error) (err error) {

	for attempt := 0; attempt < attempts; attempt++ {
		err = fn()
		if nil == err {
			return nil

		} else {
			ErrorLogger.WithContext(ctx).Error(
				"access failed.",
				zap.Int(" attempt", attempt),
				zap.Error(err))
		}
		time.Sleep(delay)
		delay *= 2
	}
	return err
}

func RetryV4(
	ctx context.Context,
	attempts int,
	delay time.Duration,
	fn func() (error, interface{})) (
	err error, output interface{}) {

	for attempt := 0; attempt < attempts; attempt++ {
		err, output = fn()
		if nil == err {
			return nil, output

		} else {
			ErrorLogger.WithContext(ctx).Error(
				"access failed.",
				zap.Int(" attempt", attempt),
				zap.Error(err))
		}
		time.Sleep(delay)
		delay *= 2
	}
	return err, output
}
