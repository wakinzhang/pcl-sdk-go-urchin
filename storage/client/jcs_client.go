package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/hashicorp/go-retryablehttp"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	"io"
	"net"
	"net/http"
	"time"
)

type JCSClient struct {
	jcsClient *retryablehttp.Client
}

func (o *JCSClient) Init(
	ctx context.Context,
	reqTimeout,
	maxConnection int) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:Init start.",
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

	timeout := time.Duration(reqTimeout) * time.Second

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(
			ctx context.Context,
			network,
			addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   3 * time.Second,  // 连接超时时间
				KeepAlive: 30 * time.Second, // 保持连接时长
			}
			return dialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     90 * time.Second,
		MaxIdleConnsPerHost: maxConnection,
	}
	o.jcsClient = retryablehttp.NewClient()
	o.jcsClient.RetryMax = 3
	o.jcsClient.RetryWaitMin = 1 * time.Second
	o.jcsClient.RetryWaitMax = 5 * time.Second
	o.jcsClient.HTTPClient.Timeout = timeout
	o.jcsClient.HTTPClient.Transport = transport

	Logger.WithContext(ctx).Debug(
		"JCSClient:Init finish.")
}

func (o *JCSClient) ListWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error, resp *JCSListResponse) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:ListWithSignedUrl start.",
		" signedUrl: ", signedUrl)

	resp = new(JCSListResponse)

	request, err := retryablehttp.NewRequest(
		http.MethodGet,
		signedUrl,
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			" err: ", err)
		return err, resp
	}

	response, err := o.jcsClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Do failed.",
			" err: ", err)
		return err, resp
	}

	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				" err: ", _err)
		}
	}(response.Body)

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:ListWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSClient) UploadFileWithSignedUrl(
	ctx context.Context,
	signedUrl string,
	data io.Reader) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:UploadFileWithSignedUrl start.",
		" signedUrl: ", signedUrl)

	request, err := retryablehttp.NewRequest(
		http.MethodPost,
		signedUrl,
		data)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			" err: ", err)
		return err
	}

	request.Header.Set(HttpHeaderContentType, HttpHeaderContentTypeStream)

	response, err := o.jcsClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Do failed.",
			" err: ", err)
		return err
	}

	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				" err: ", _err)
		}
	}(response.Body)

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	resp := new(JCSBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if JCSSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message)
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:UploadFileWithSignedUrl finish.")
	return nil
}

func (o *JCSClient) NewMultiPartUploadWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error,
	resp *JCSNewMultiPartUploadResponse) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:NewMultiPartUploadWithSignedUrl start.",
		" signedUrl: ", signedUrl)

	resp = new(JCSNewMultiPartUploadResponse)

	request, err := retryablehttp.NewRequest(
		http.MethodPost,
		signedUrl,
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			" err: ", err)
		return err, resp
	}

	response, err := o.jcsClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Do failed.",
			" err: ", err)
		return err, resp
	}

	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				" err: ", _err)
		}
	}(response.Body)

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:NewMultiPartUploadWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSClient) UploadPartWithSignedUrl(
	ctx context.Context,
	signedUrl string,
	data io.Reader) (err error, resp *JCSBaseResponse) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:UploadPartWithSignedUrl start.",
		" signedUrl: ", signedUrl)

	resp = new(JCSBaseResponse)

	request, err := retryablehttp.NewRequest(
		http.MethodPost,
		signedUrl,
		data)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			" err: ", err)
		return err, resp
	}

	request.Header.Set(HttpHeaderContentType, HttpHeaderContentTypeStream)

	response, err := o.jcsClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Do failed.",
			" err: ", err)
		return err, resp
	}

	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				" err: ", _err)
		}
	}(response.Body)

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:UploadPartWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSClient) CompleteMultiPartUploadWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error,
	resp *JCSNewMultiPartUploadResponse) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CompleteMultiPartUploadWithSignedUrl start.",
		" signedUrl: ", signedUrl)

	resp = new(JCSNewMultiPartUploadResponse)

	request, err := retryablehttp.NewRequest(
		http.MethodPost,
		signedUrl,
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			" err: ", err)
		return err, resp
	}

	response, err := o.jcsClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Do failed.",
			" err: ", err)
		return err, resp
	}

	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				" err: ", _err)
		}
	}(response.Body)

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:CompleteMultiPartUploadWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSClient) DownloadPartWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error, output *JCSDownloadPartOutput) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:DownloadPartWithSignedUrl start.",
		" signedUrl: ", signedUrl)

	output = new(JCSDownloadPartOutput)

	request, err := retryablehttp.NewRequest(
		http.MethodGet,
		signedUrl,
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			" err: ", err)
		return err, output
	}

	response, err := o.jcsClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"client.Do failed.",
			" err: ", err)
		return err, output
	}

	if HttpHeaderContentTypeJson ==
		response.Header.Get(HttpHeaderContentType) {

		defer func(body io.ReadCloser) {
			_err := body.Close()
			if nil != _err {
				Logger.WithContext(ctx).Error(
					"io.ReadCloser failed.",
					" err: ", _err)
			}
		}(response.Body)

		respBodyBuf, err := io.ReadAll(response.Body)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"io.ReadAll failed.",
				" err: ", err)
			return err, output
		}

		Logger.WithContext(ctx).Debug(
			"response: ", string(respBodyBuf))

		var resp *JCSBaseResponse
		err = json.Unmarshal(respBodyBuf, resp)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"json.Unmarshal failed.",
				" err: ", err)
			return err, output
		}

		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)

		return errors.New(resp.Message), output
	}

	output = new(JCSDownloadPartOutput)
	output.Body = response.Body

	Logger.WithContext(ctx).Debug(
		"JCSClient:DownloadPartWithSignedUrl finish.")
	return nil, output
}
