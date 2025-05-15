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
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
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

	Logger.WithContext(ctx).Debug(
		"ScowClient:Init start.",
		" username: ", "***",
		" password: ", "***",
		" endpoint: ", endpoint,
		" url: ", url,
		" clusterId: ", clusterId,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

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

	Logger.WithContext(ctx).Debug(
		"ScowClient:Init finish.")
}

func (o *ScowClient) refreshToken(
	ctx context.Context) (err error) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:refreshToken start.")

	if time.Now().Sub(o.tokenCreateTime).Hours() <
		DefaultScowTokenExpireHours {

		Logger.WithContext(ctx).Debug(
			"Scow token valid, no need to refresh.")
		return err
	}

	input := new(ScowGetTokenReq)
	input.Username = o.username
	input.Password = o.password

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err
	}

	url := o.url + ScowGetTokenInterface

	Logger.WithContext(ctx).Debug(
		"ScowClient:refreshToken request.",
		" url: ", url)

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
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	resp := new(ScowGetTokenResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if ScowSuccessCode != resp.RespCode {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" RespCode: ", resp.RespCode,
			" RespError: ", resp.RespError,
			" RespMessage: ", resp.RespMessage)
		return errors.New(resp.RespError)
	}

	o.token = resp.RespBody.Token
	o.tokenCreateTime = time.Now()

	Logger.WithContext(ctx).Debug(
		"ScowClient:refreshToken finish.")
	return err
}

func (o *ScowClient) CheckExist(
	ctx context.Context,
	path string) (err error, exist bool) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:CheckExist start.",
		" path: ", path)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			" err: ", err)
		return err, exist
	}

	input := new(ScowCheckExistReq)
	input.ClusterId = o.clusterId
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, exist
	}

	url := o.endpoint + ScowCheckExistInterface + "?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"ScowClient:CheckExist request.",
		" url: ", url)

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
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err, exist
	}
	Logger.WithContext(ctx).Debug(
		"ScowClient:CheckExist response.",
		" path: ", path,
		" response: ", string(respBody))

	resp := new(ScowCheckExistResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, exist
	}

	if ScowSuccessCode != resp.RespCode {
		Logger.WithContext(ctx).Error(
			"ScowClient:CheckExist response failed.",
			" path: ", path,
			" RespCode: ", resp.RespCode,
			" RespError: ", resp.RespError,
			" RespMessage: ", resp.RespMessage)
		return errors.New(resp.RespError), exist
	}
	exist = resp.RespBody.Data.Exists

	Logger.WithContext(ctx).Debug(
		"ScowClient:CheckExist finish.")
	return err, exist
}

func (o *ScowClient) Mkdir(
	ctx context.Context,
	path string) (err error) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:Mkdir start.",
		" path: ", path)

	err, exist := o.CheckExist(ctx, path)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.CheckExist failed.",
			" err: ", err)
		return err
	}
	if true == exist {
		Logger.WithContext(ctx).Info(
			"Path already exist, no need to mkdir.",
			" path: ", path)
		return err
	}

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(ScowMkdirReq)
	input.ClusterId = o.clusterId
	input.Path = path

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + ScowMkdirInterface

	Logger.WithContext(ctx).Debug(
		"ScowClient:Mkdir request.",
		" url: ", url,
		" reqBody: ", string(reqBody))

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
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"ScowClient:Mkdir response.",
		" path: ", path,
		" response: ", string(respBody))

	resp := new(ScowBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if ScowSuccessCode != resp.RespCode {
		Logger.WithContext(ctx).Error(
			"ScowClient:Mkdir response failed.",
			" path: ", path,
			" RespCode: ", resp.RespCode,
			" RespError: ", resp.RespError,
			" RespMessage: ", resp.RespMessage)
		return errors.New(resp.RespError)
	}

	Logger.WithContext(ctx).Debug(
		"ScowClient:Mkdir finish.")
	return err
}

func (o *ScowClient) Delete(
	ctx context.Context,
	path string) (err error) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:Delete start.",
		" path: ", path)

	err, exist := o.CheckExist(ctx, path)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.CheckExist failed.",
			" err: ", err)
		return err
	}
	if false == exist {
		Logger.WithContext(ctx).Info(
			"Path already not exist, no need to delete.",
			" path: ", path)
		return err
	}

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(ScowDeleteReq)
	input.ClusterId = o.clusterId
	input.Path = path

	stat, err := os.Stat(path)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"os.Stat failed.",
			" path: ", path,
			" err: ", err)
		return err
	}
	if stat.IsDir() {
		input.Target = ScowObjectTypeFolder
	} else {
		input.Target = ScowObjectTypeFile
	}

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + ScowDeleteInterface

	Logger.WithContext(ctx).Debug(
		"ScowClient:Delete request.",
		" url: ", url,
		" reqBody: ", string(reqBody))

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
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"ScowClient:Delete response.",
		" path: ", path,
		" response: ", string(respBody))

	resp := new(ScowBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if ScowSuccessCode != resp.RespCode {
		Logger.WithContext(ctx).Error(
			"ScowClient:Delete response failed.",
			" path: ", path,
			" RespCode: ", resp.RespCode,
			" RespError: ", resp.RespError,
			" RespMessage: ", resp.RespMessage)
		return errors.New(resp.RespError)
	}

	Logger.WithContext(ctx).Debug(
		"ScowClient:Delete finish.")
	return err
}

func (o *ScowClient) Upload(
	ctx context.Context,
	fileName,
	path string,
	data io.Reader) (err error) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:Upload start.",
		" fileName: ", fileName,
		" path: ", path)

	err, exist := o.CheckExist(ctx, path)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.CheckExist failed.",
			" err: ", err)
		return err
	}
	if true == exist {
		Logger.WithContext(ctx).Info(
			"Path already exist, no need to upload.",
			" path: ", path)
		return err
	}

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(ScowUploadReq)
	input.ClusterId = o.clusterId
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + ScowUploadInterface + "?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"ScowClient:Upload request.",
		" url: ", url)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		ScowMultiPartFormFiledFile,
		fileName)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"writer.CreateFormFile failed.",
			" err: ", err)
		return err
	}
	_, err = io.Copy(part, data)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.Copy failed.",
			" err: ", err)
		return err
	}

	err = writer.Close()
	if err != nil {
		Logger.WithContext(ctx).Error(
			"writer.Close failed.",
			" err: ", err)
		return err
	}

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return err
	}
	reqHttp.Header.Set(ScowHttpHeaderAuth, o.token)
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return err
	}

	response, err := o.scowClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.Do failed.",
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

	respBodyBuf, err := io.ReadAll(response.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"ScowClient:Upload response.",
		" fileName: ", fileName,
		" path: ", path,
		" response: ", string(respBodyBuf))

	resp := new(ScowBaseMessageResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if ScowSuccessMessage != resp.Message {
		Logger.WithContext(ctx).Error(
			"ScowClient:Upload response failed.",
			" fileName: ", fileName,
			" path: ", path,
			" Message: ", resp.Message)
		return errors.New(resp.Message)
	}
	Logger.WithContext(ctx).Debug(
		"ScowClient:Upload finish.")
	return err
}

func (o *ScowClient) UploadChunks(
	ctx context.Context,
	fileName,
	path,
	md5 string,
	partNum int32,
	data io.Reader) (err error) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks start.",
		" fileName: ", fileName,
		" path: ", path,
		" md5: ", md5,
		" partNum: ", partNum)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(ScowUploadChunksReq)
	input.ClusterId = o.clusterId
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + ScowUploadChunksInterface + "?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks request.",
		" url: ", url)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		ScowMultiPartFormFiledFile,
		fileName)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"writer.CreateFormFile failed.",
			" err: ", err)
		return err
	}
	_, err = io.Copy(part, data)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.Copy failed.",
			" err: ", err)
		return err
	}

	fileMd5Name := fmt.Sprintf("%s_%d.%s", md5, partNum, fileName)
	_ = writer.WriteField(
		ScowMultiPartFormFiledFileMd5Name,
		fileMd5Name)

	Logger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks request.",
		" fileMd5Name: ", fileMd5Name)

	err = writer.Close()
	if err != nil {
		Logger.WithContext(ctx).Error(
			"writer.Close failed.",
			" err: ", err)
		return err
	}

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return err
	}
	reqHttp.Header.Set(ScowHttpHeaderAuth, o.token)
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return err
	}

	response, err := o.scowClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.Do failed.",
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

	respBodyBuf, err := io.ReadAll(response.Body)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks response.",
		" fileName: ", fileName,
		" path: ", path,
		" md5: ", md5,
		" partNum: ", partNum,
		" response: ", string(respBodyBuf))

	resp := new(ScowBaseMessageResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if ScowSuccessMessage != resp.Message &&
		ScowAlreadyExistsMessage != resp.Message {
		Logger.WithContext(ctx).Error(
			"ScowClient:UploadChunks response failed.",
			" fileName: ", fileName,
			" path: ", path,
			" md5: ", md5,
			" partNum: ", partNum,
			" Message: ", resp.Message)
		return errors.New(resp.Message)
	}
	Logger.WithContext(ctx).Debug(
		"ScowClient:UploadChunks finish.")
	return err
}

func (o *ScowClient) MergeChunks(
	ctx context.Context,
	fileName,
	path,
	md5 string) (err error) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:MergeChunks start.",
		" fileName: ", fileName,
		" path: ", path,
		" md5: ", md5)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(ScowMergeChunksReq)
	input.ClusterId = o.clusterId
	input.FileName = fileName
	input.Path = path
	input.Md5 = md5

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + ScowMergeChunksInterface

	Logger.WithContext(ctx).Debug(
		"ScowClient:MergeChunks request.",
		" url: ", url,
		" reqBody: ", string(reqBody))

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
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"ScowClient:MergeChunks response.",
		" fileName: ", fileName,
		" path: ", path,
		" md5: ", md5,
		" response: ", string(respBody))

	resp := new(ScowBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if ScowSuccessCode != resp.RespCode {
		Logger.WithContext(ctx).Error(
			"ScowClient:MergeChunks response failed.",
			" fileName: ", fileName,
			" path: ", path,
			" md5: ", md5,
			" RespCode: ", resp.RespCode,
			" RespError: ", resp.RespError,
			" RespMessage: ", resp.RespMessage)
		return errors.New(resp.RespError)
	}

	Logger.WithContext(ctx).Debug(
		"ScowClient:MergeChunks finish.")
	return err
}

func (o *ScowClient) List(
	ctx context.Context,
	path string) (err error, output *ScowListResponseBody) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:List start.",
		" path: ", path)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			" err: ", err)
		return err, output
	}

	input := new(ScowListReq)
	input.ClusterId = o.clusterId
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, output
	}

	url := o.endpoint + ScowListInterface + "?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"ScowClient:List request.",
		" url: ", url)

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
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err, output
	}
	Logger.WithContext(ctx).Debug(
		"ScowClient:List response.",
		" path: ", path,
		" response: ", string(respBody))

	resp := new(ScowListResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, output
	}

	if ScowSuccessCode != resp.RespCode {
		Logger.WithContext(ctx).Error(
			"ScowClient:List response failed.",
			" path: ", path,
			" RespCode: ", resp.RespCode,
			" RespError: ", resp.RespError,
			" RespMessage: ", resp.RespMessage)
		return errors.New(resp.RespError), output
	}
	output = resp.RespBody
	return err, output
}

func (o *ScowClient) DownloadChunks(
	ctx context.Context,
	path,
	contentRange string) (err error, output *ScowDownloadPartOutput) {

	Logger.WithContext(ctx).Debug(
		"ScowClient:DownloadChunks start.",
		" path: ", path,
		" contentRange: ", contentRange)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.refreshToken failed.",
			" err: ", err)
		return err, output
	}

	input := new(ScowDownloadReq)
	input.ClusterId = o.clusterId
	input.Path = path
	input.Download = "true"

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, output
	}

	url := o.endpoint + ScowDownloadInterface

	Logger.WithContext(ctx).Debug(
		"ScowClient:DownloadChunks request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(ScowHttpHeaderAuth, o.token)
	header.Add(HttpHeaderRange, contentRange)

	request, err := retryablehttp.NewRequest(
		http.MethodGet,
		url+"?"+values.Encode(),
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			" err: ", err)
		return err, output
	}

	request.Header = header

	response, err := o.scowClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ScowClient.Do failed.",
			" err: ", err)
		return err, output
	}

	if 200 > response.StatusCode ||
		300 <= response.StatusCode {

		Logger.WithContext(ctx).Error(
			"ScowClient.DownloadChunks response failed.",
			" StatusCode: ", response.StatusCode,
			" Status: ", response.Status)
		return errors.New("DownloadChunks failed"), output
	}

	output = new(ScowDownloadPartOutput)
	output.Body = response.Body

	Logger.WithContext(ctx).Debug(
		"ScowClient:DownloadChunks finish.")
	return err, output
}
