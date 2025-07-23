package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/go-querystring/query"
	"github.com/hashicorp/go-retryablehttp"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"time"
	// LTScow "github.com/urchinfs/LT-scow-sdk/scow"
)

type ScowClient struct {
	username        string
	password        string
	endpoint        string
	url             string
	token           string
	tokenCreateTime time.Time
	clusterId       string
	scowClient      *retryablehttp.Client
}

func (o *ScowClient) Init(
	ctx context.Context,
	username,
	password,
	endpoint,
	url,
	clusterId string,
	reqTimeout,
	maxConnection int32) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Init start.",
		zap.String("endpoint", endpoint),
		zap.String("url", url),
		zap.String("clusterId", clusterId),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

	o.username = username
	o.password = password
	o.endpoint = endpoint
	o.url = url
	o.clusterId = clusterId

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
		MaxIdleConnsPerHost: int(maxConnection),
	}
	o.scowClient = retryablehttp.NewClient()
	o.scowClient.RetryMax = 3
	o.scowClient.RetryWaitMin = 1 * time.Second
	o.scowClient.RetryWaitMax = 5 * time.Second
	o.scowClient.HTTPClient.Timeout = timeout
	o.scowClient.HTTPClient.Transport = transport

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Init finish.")
}

func (o *ScowClient) refreshToken(
	ctx context.Context) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:refreshToken start.")

	if time.Now().Sub(o.tokenCreateTime).Hours() <
		DefaultScowTokenExpireHours {

		InfoLogger.WithContext(ctx).Debug(
			"Scow token valid, no need to refresh.")
		return err
	}

	input := new(ScowGetTokenReq)
	input.Username = o.username
	input.Password = o.password

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err
	}

	url := o.url + ScowGetTokenInterface

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:refreshToken request.",
		zap.String("url", url))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	err, respBody := Do(
		ctx,
		url,
		http.MethodPost,
		header,
		reqBody,
		o.scowClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	resp := new(ScowGetTokenResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if ScowSuccessCode != resp.RespCode {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("respCode", resp.RespCode),
			zap.String("respError", resp.RespError),
			zap.String("respMessage", resp.RespMessage))
		return errors.New(resp.RespError)
	}

	o.token = resp.RespBody.Token
	o.tokenCreateTime = time.Now()

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:refreshToken finish.")
	return err
}

func (o *ScowClient) CheckExist(
	ctx context.Context,
	path string) (err error, exist bool) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:CheckExist start.",
		zap.String("path", path))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			zap.Error(err))
		return err, exist
	}

	input := new(ScowCheckExistReq)
	input.ClusterId = o.clusterId
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, exist
	}

	url := o.endpoint + ScowCheckExistInterface + "?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:CheckExist request.",
		zap.String("url", url))

	header := make(http.Header)
	header.Add(ScowHttpHeaderAuth, o.token)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	err, respBody := Do(
		ctx,
		url,
		http.MethodGet,
		header,
		nil,
		o.scowClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, exist
	}
	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:CheckExist response.",
		zap.String("path", path),
		zap.String("response", string(respBody)))

	resp := new(ScowCheckExistResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, exist
	}

	if ScowSuccessCode != resp.RespCode {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient:CheckExist response failed.",
			zap.String("path", path),
			zap.Int32("respCode", resp.RespCode),
			zap.String("respError", resp.RespError),
			zap.String("respMessage", resp.RespMessage))
		return errors.New(resp.RespError), exist
	}
	exist = resp.RespBody.Data.Exists

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:CheckExist finish.")
	return err, exist
}

func (o *ScowClient) Mkdir(
	ctx context.Context,
	path string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Mkdir start.",
		zap.String("path", path))

	err, exist := o.CheckExist(ctx, path)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.CheckExist failed.",
			zap.Error(err))
		return err
	}
	if true == exist {
		InfoLogger.WithContext(ctx).Info(
			"Path already exist, no need to mkdir.",
			zap.String("path", path))
		return err
	}

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			zap.Error(err))
		return err
	}

	input := new(ScowMkdirReq)
	input.ClusterId = o.clusterId
	input.Path = path

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err
	}

	url := o.endpoint + ScowMkdirInterface

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Mkdir request.",
		zap.String("url", url),
		zap.String("reqBody", string(reqBody)))

	header := make(http.Header)
	header.Add(ScowHttpHeaderAuth, o.token)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	err, respBody := Do(
		ctx,
		url,
		http.MethodPost,
		header,
		reqBody,
		o.scowClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Mkdir response.",
		zap.String("path", path),
		zap.String("response", string(respBody)))

	resp := new(ScowBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if ScowSuccessCode != resp.RespCode {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient:Mkdir response failed.",
			zap.String("path", path),
			zap.Int32("respCode", resp.RespCode),
			zap.String("respError", resp.RespError),
			zap.String("respMessage", resp.RespMessage))
		return errors.New(resp.RespError)
	}

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Mkdir finish.")
	return err
}

func (o *ScowClient) Delete(
	ctx context.Context,
	path,
	target string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Delete start.",
		zap.String("path", path),
		zap.String("target", target))

	err, exist := o.CheckExist(ctx, path)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.CheckExist failed.",
			zap.Error(err))
		return err
	}
	if false == exist {
		InfoLogger.WithContext(ctx).Info(
			"Path already not exist, no need to delete.",
			zap.String("path", path))
		return err
	}

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			zap.Error(err))
		return err
	}

	input := new(ScowDeleteReq)
	input.ClusterId = o.clusterId
	input.Path = path
	input.Target = target

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err
	}

	url := o.endpoint + ScowDeleteInterface

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Delete request.",
		zap.String("url", url),
		zap.String("reqBody", string(reqBody)))

	header := make(http.Header)
	header.Add(ScowHttpHeaderAuth, o.token)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	err, respBody := Do(
		ctx,
		url,
		http.MethodPost,
		header,
		reqBody,
		o.scowClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Delete response.",
		zap.String("path", path),
		zap.String("response", string(respBody)))

	resp := new(ScowBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if ScowSuccessCode != resp.RespCode {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient:Delete response failed.",
			zap.String("path", path),
			zap.Int32("respCode", resp.RespCode),
			zap.String("respError", resp.RespError),
			zap.String("respMessage", resp.RespMessage))
		return errors.New(resp.RespError)
	}

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Delete finish.")
	return err
}

func (o *ScowClient) Upload(
	ctx context.Context,
	fileName,
	path string,
	data io.Reader) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Upload start.",
		zap.String("fileName", fileName),
		zap.String("path", path))

	err, exist := o.CheckExist(ctx, path)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.CheckExist failed.",
			zap.Error(err))
		return err
	}
	if true == exist {
		InfoLogger.WithContext(ctx).Info(
			"Path already exist, no need to upload.",
			zap.String("path", path))
		return err
	}

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			zap.Error(err))
		return err
	}

	input := new(ScowUploadReq)
	input.ClusterId = o.clusterId
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err
	}

	url := o.endpoint + ScowUploadInterface + "?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Upload request.",
		zap.String("url", url))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		ScowMultiPartFormFiledFile,
		fileName)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"writer.CreateFormFile failed.",
			zap.Error(err))
		return err
	}
	_, err = io.Copy(part, data)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.Copy failed.",
			zap.Error(err))
		return err
	}

	err = writer.Close()
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"writer.Close failed.",
			zap.Error(err))
		return err
	}

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return err
	}
	reqHttp.Header.Set(ScowHttpHeaderAuth, o.token)
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err
	}

	response, err := o.scowClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.Do failed.",
			zap.Error(err))
		return err
	}

	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				zap.Error(_err))
		}
	}(response.Body)

	respBodyBuf, err := io.ReadAll(response.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Upload response.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("response", string(respBodyBuf)))

	resp := new(ScowBaseMessageResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if ScowSuccessMessage != resp.Message {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient:Upload response failed.",
			zap.String("fileName", fileName),
			zap.String("path", path),
			zap.String("message", resp.Message))
		return errors.New(resp.Message)
	}

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:Upload finish.")
	return err
}

func (o *ScowClient) UploadChunks(
	ctx context.Context,
	fileName,
	path,
	md5 string,
	partNum int32,
	data io.Reader) (err error, resp *ScowBaseMessageResponse) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks start.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("md5", md5),
		zap.Int32("partNum", partNum))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			zap.Error(err))
		return err, resp
	}

	input := new(ScowUploadChunksReq)
	input.ClusterId = o.clusterId
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, resp
	}

	url := o.endpoint + ScowUploadChunksInterface + "?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks request.",
		zap.String("url", url))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		ScowMultiPartFormFiledFile,
		fileName)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"writer.CreateFormFile failed.",
			zap.Error(err))
		return err, resp
	}
	_, err = io.Copy(part, data)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.Copy failed.",
			zap.Error(err))
		return err, resp
	}

	fileMd5Name := fmt.Sprintf("%s_%d.%s", md5, partNum, fileName)
	_ = writer.WriteField(
		ScowMultiPartFormFiledFileMd5Name,
		fileMd5Name)

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks request.",
		zap.String("fileMd5Name", fileMd5Name))

	err = writer.Close()
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"writer.Close failed.",
			zap.Error(err))
		return err, resp
	}

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return err, resp
	}
	reqHttp.Header.Set(ScowHttpHeaderAuth, o.token)
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err, resp
	}

	resp = new(ScowBaseMessageResponse)
	response, err := o.scowClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.Do failed.",
			zap.Error(err))
		return err, resp
	}

	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				zap.Error(_err))
		}
	}(response.Body)

	respBodyBuf, err := io.ReadAll(response.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks response.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("md5", md5),
		zap.Int32("partNum", partNum),
		zap.String("response", string(respBodyBuf)))

	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if ScowSuccessMessage != resp.Message &&
		ScowAlreadyExistsMessage != resp.Message {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient:UploadChunks response failed.",
			zap.String("fileName", fileName),
			zap.String("path", path),
			zap.String("md5", md5),
			zap.Int32("partNum", partNum),
			zap.String("message", resp.Message))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks finish.")
	return nil, resp
}

func (o *ScowClient) MergeChunks(
	ctx context.Context,
	fileName,
	path,
	md5 string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:MergeChunks start.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("md5", md5))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			zap.Error(err))
		return err
	}

	input := new(ScowMergeChunksReq)
	input.ClusterId = o.clusterId
	input.FileName = fileName
	input.Path = path
	input.Md5 = md5

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err
	}

	url := o.endpoint + ScowMergeChunksInterface

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:MergeChunks request.",
		zap.String("url", url),
		zap.String("reqBody", string(reqBody)))

	header := make(http.Header)
	header.Add(ScowHttpHeaderAuth, o.token)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	err, respBody := Do(
		ctx,
		url,
		http.MethodPost,
		header,
		reqBody,
		o.scowClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:MergeChunks response.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("md5", md5),
		zap.String("response", string(respBody)))

	resp := new(ScowBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if ScowSuccessCode != resp.RespCode {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient:MergeChunks response failed.",
			zap.String("fileName", fileName),
			zap.String("path", path),
			zap.String("md5", md5),
			zap.Int32("respCode", resp.RespCode),
			zap.String("respError", resp.RespError),
			zap.String("respMessage", resp.RespMessage))
		return errors.New(resp.RespError)
	}

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:MergeChunks finish.")
	return err
}

func (o *ScowClient) List(
	ctx context.Context,
	path string) (err error, output *ScowListResponseBody) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:List start.",
		zap.String("path", path))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			zap.Error(err))
		return err, output
	}

	input := new(ScowListReq)
	input.ClusterId = o.clusterId
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, output
	}

	url := o.endpoint + ScowListInterface + "?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:List request.",
		zap.String("url", url))

	header := make(http.Header)
	header.Add(ScowHttpHeaderAuth, o.token)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	err, respBody := Do(
		ctx,
		url,
		http.MethodGet,
		header,
		nil,
		o.scowClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, output
	}
	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:List response.",
		zap.String("path", path),
		zap.String("response", string(respBody)))

	resp := new(ScowListResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, output
	}

	if ScowSuccessCode != resp.RespCode {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient:List response failed.",
			zap.String("path", path),
			zap.Int32("respCode", resp.RespCode),
			zap.String("respError", resp.RespError),
			zap.String("respMessage", resp.RespMessage))
		return errors.New(resp.RespError), output
	}

	output = new(ScowListResponseBody)
	output = resp.RespBody

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:List finish.")
	return err, output
}

func (o *ScowClient) DownloadChunks(
	ctx context.Context,
	path,
	contentRange string) (err error, output *ScowDownloadPartOutput) {

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:DownloadChunks start.",
		zap.String("path", path),
		zap.String("contentRange", contentRange))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			zap.Error(err))
		return err, output
	}

	input := new(ScowDownloadReq)
	input.ClusterId = o.clusterId
	input.Path = path
	input.Download = "true"

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, output
	}

	url := o.endpoint + ScowDownloadInterface

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:DownloadChunks request.",
		zap.String("url", url),
		zap.String("query", values.Encode()))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(ScowHttpHeaderAuth, o.token)
	header.Add(HttpHeaderRange, contentRange)

	request, err := retryablehttp.NewRequest(
		http.MethodGet,
		url+"?"+values.Encode(),
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			zap.Error(err))
		return err, output
	}

	request.Header = header

	response, err := o.scowClient.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.Do failed.",
			zap.Error(err))
		return err, output
	}

	if 200 > response.StatusCode ||
		300 <= response.StatusCode {

		ErrorLogger.WithContext(ctx).Error(
			"ScowClient.DownloadChunks response failed.",
			zap.Int("statusCode", response.StatusCode),
			zap.String("status", response.Status))
		return errors.New("DownloadChunks failed"), output
	}

	output = new(ScowDownloadPartOutput)
	output.Body = response.Body

	InfoLogger.WithContext(ctx).Debug(
		"ScowClient:DownloadChunks finish.")
	return err, output
}
