package common

import (
	"context"
	"errors"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
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

func RetryV2(
	ctx context.Context,
	attempts int,
	delay time.Duration,
	successCode map[string]bool,
	fn func() (error, interface{})) (err error) {

	for attempt := 0; attempt < attempts; attempt++ {
		err, output := fn()
		if nil == err {
			if sugonBaseResponse, ok := output.(*SugonBaseResponse); ok {
				if _, exists := successCode[sugonBaseResponse.Code]; exists {
					return nil
				} else {
					Logger.WithContext(ctx).Error(
						"SugonClient response failed.",
						" attempt: ", attempt,
						" Code: ", sugonBaseResponse.Code,
						" Msg: ", sugonBaseResponse.Msg)
					err = errors.New(sugonBaseResponse.Msg)
				}
			} else {
				Logger.WithContext(ctx).Error(
					"output invalid.")
				return errors.New("output invalid")
			}

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

func RetryV3(
	ctx context.Context,
	attempts int,
	delay time.Duration,
	successCode map[string]bool,
	fn func() (error, interface{})) (
	err error, output interface{}) {

	for attempt := 0; attempt < attempts; attempt++ {
		err, output = fn()
		if nil == err {
			if sugonBaseResponse, ok := output.(*SugonBaseResponse); ok {
				if _, exists := successCode[sugonBaseResponse.Code]; exists {
					return nil, output
				} else {
					Logger.WithContext(ctx).Error(
						"SugonClient response failed.",
						" attempt: ", attempt,
						" Code: ", sugonBaseResponse.Code,
						" Msg: ", sugonBaseResponse.Msg)
					err = errors.New(sugonBaseResponse.Msg)
				}
			} else {
				Logger.WithContext(ctx).Error(
					"output invalid.")
				return errors.New("output invalid"), output
			}
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
