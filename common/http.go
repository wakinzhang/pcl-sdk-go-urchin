package common

import (
	"context"
	"github.com/hashicorp/go-retryablehttp"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strings"
)

func Post(
	ctx context.Context,
	url string,
	reqBody []byte,
	client *retryablehttp.Client) (err error, respBody []byte) {

	InfoLogger.WithContext(ctx).Debug(
		"http:Post start.",
		zap.String("url", url),
		zap.String("reqBody", string(reqBody)))

	respBuf, err := client.Post(
		url,
		"application/json; charset=UTF-8",
		strings.NewReader(string(reqBody)))
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Post failed.",
			zap.Error(err))
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				zap.Error(err))
		}
	}(respBuf.Body)

	respBody, err = io.ReadAll(respBuf.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err, respBody
	}

	InfoLogger.WithContext(ctx).Debug(
		"http:Post finish.",
		zap.String("respBody", string(respBody)))
	return nil, respBody
}

func Get(
	ctx context.Context,
	url string,
	client *retryablehttp.Client) (err error, respBody []byte) {

	InfoLogger.WithContext(ctx).Debug(
		"http:Get start.",
		zap.String("url", url))

	respBuf, err := client.Get(url)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Get failed.",
			zap.Error(err))
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				zap.Error(err))
		}
	}(respBuf.Body)

	respBody, err = io.ReadAll(respBuf.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err, respBody
	}

	InfoLogger.WithContext(ctx).Debug(
		"http:Get finish.",
		zap.String("respBody", string(respBody)))
	return nil, respBody
}

func Do(
	ctx context.Context,
	url,
	method string,
	header http.Header,
	reqBody interface{},
	client *retryablehttp.Client) (err error, respBody []byte) {

	InfoLogger.WithContext(ctx).Debug(
		"http:Do start.",
		zap.String("url", url),
		zap.String("method", method))

	request, err := retryablehttp.NewRequest(
		method,
		url,
		reqBody)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			zap.Error(err))
		return err, respBody
	}
	request.Header = header
	resp, err := client.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Do failed.",
			zap.Error(err))
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				zap.Error(_err))
		}
	}(resp.Body)

	respBody, err = io.ReadAll(resp.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err, respBody
	}

	InfoLogger.WithContext(ctx).Debug(
		"http:Do finish.",
		zap.String("respBody", string(respBody)))
	return nil, respBody
}
