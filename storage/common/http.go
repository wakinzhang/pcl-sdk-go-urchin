package common

import (
	"context"
	"github.com/hashicorp/go-retryablehttp"
	"io"
	"net/http"
	"strings"
)

func Post(
	ctx context.Context,
	url string,
	reqBody []byte,
	client *retryablehttp.Client) (err error, respBody []byte) {

	Logger.WithContext(ctx).Debug(
		"http:Post start.",
		" url: ", url,
		" reqBody: ", string(reqBody))

	respBuf, err := client.Post(
		url,
		"application/json; charset=UTF-8",
		strings.NewReader(string(reqBody)))
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Post failed.",
			" err: ", err)
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if nil != err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				" err: ", err)
		}
	}(respBuf.Body)

	respBody, err = io.ReadAll(respBuf.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err, respBody
	}

	Logger.WithContext(ctx).Debug(
		"http:Post finish. respBody: ", string(respBody))
	return nil, respBody
}

func Get(
	ctx context.Context,
	url string,
	client *retryablehttp.Client) (err error, respBody []byte) {

	Logger.WithContext(ctx).Debug(
		"http:Get start. url: ", url)

	respBuf, err := client.Get(url)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Get failed.",
			" err: ", err)
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if nil != err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				" err: ", err)
		}
	}(respBuf.Body)

	respBody, err = io.ReadAll(respBuf.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err, respBody
	}

	Logger.WithContext(ctx).Debug(
		"http:Get finish. respBody: ", string(respBody))
	return nil, respBody
}

func Do(
	ctx context.Context,
	url,
	method string,
	header http.Header,
	reqBody interface{},
	client *retryablehttp.Client) (err error, respBody []byte) {

	Logger.WithContext(ctx).Debug(
		"http:Do start.",
		" url: ", url,
		" method: ", method)

	request, err := retryablehttp.NewRequest(
		method,
		url,
		reqBody)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			" err: ", err)
		return err, respBody
	}
	request.Header = header
	resp, err := client.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Do failed.",
			" err: ", err)
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				" err: ", _err)
		}
	}(resp.Body)

	respBody, err = io.ReadAll(resp.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err, respBody
	}

	Logger.WithContext(ctx).Debug(
		"http:Do finish. respBody: ", string(respBody))
	return nil, respBody
}
