package common

import (
	"context"
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
			Logger.WithContext(ctx).Error(
				"access failed.",
				" attempt: ", attempt,
				" err: ", err)
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
			Logger.WithContext(ctx).Error(
				"access failed.",
				" attempt: ", attempt,
				" err: ", err)
		}
		time.Sleep(delay)
		delay *= 2
	}
	return err, output
}
