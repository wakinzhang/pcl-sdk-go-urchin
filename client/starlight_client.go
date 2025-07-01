package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/google/go-querystring/query"
	"github.com/hashicorp/go-retryablehttp"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"io"
	"net"
	"net/http"
	"time"
	// sl "github.com/urchinfs/starlight-sdk/starlight"
)

type SLClient struct {
	username        string
	password        string
	endpoint        string
	token           string
	tokenCreateTime time.Time
	slClient        *retryablehttp.Client
}

func (o *SLClient) Init(
	ctx context.Context,
	username,
	password,
	endpoint string,
	reqTimeout,
	maxConnection int32) {

	Logger.WithContext(ctx).Debug(
		"SLClient:Init start.",
		" username: ", "***",
		" password: ", "***",
		" endpoint: ", endpoint,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

	o.username = username
	o.password = password
	o.endpoint = endpoint

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
	o.slClient = retryablehttp.NewClient()
	o.slClient.RetryMax = 3
	o.slClient.RetryWaitMin = 1 * time.Second
	o.slClient.RetryWaitMax = 5 * time.Second
	o.slClient.HTTPClient.Timeout = timeout
	o.slClient.HTTPClient.Transport = transport
	o.slClient.Logger = Logger

	Logger.WithContext(ctx).Debug(
		"SLClient:Init finish.")
}

func (o *SLClient) refreshToken(ctx context.Context) (err error) {
	Logger.WithContext(ctx).Debug(
		"SLClient:refreshToken start.")

	if time.Now().Sub(o.tokenCreateTime).Hours() <
		DefaultStarLightTokenExpireHours {

		Logger.WithContext(ctx).Debug(
			"StarLight token valid, no need to refresh.")
		return err
	}

	input := new(SLGetTokenReq)
	input.Username = o.username
	input.Password = o.password

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + StarLightGetTokenInterface

	Logger.WithContext(ctx).Debug(
		"SLClient:refreshToken request.",
		" url: ", url,
		" reqBody: ", string(reqBody))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	err, respBody := Do(
		ctx,
		url,
		http.MethodPost,
		header,
		reqBody,
		o.slClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	resp := new(SLBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SLSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" Code: ", resp.Code,
			" Info: ", resp.Info)
		return errors.New(resp.Info)
	}

	o.token = resp.Spec
	o.tokenCreateTime = time.Now()

	Logger.WithContext(ctx).Debug(
		"SLClient:refreshToken finish.")
	return err
}

func (o *SLClient) Mkdir(
	ctx context.Context,
	target string) (err error) {

	Logger.WithContext(ctx).Debug(
		"SLClient:Mkdir start.",
		" target: ", target)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SLClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(SLStorageOperationReq)
	input.Target = target
	input.Opt = StarLightStorageOperationMkdir
	input.Force = "true"
	input.Recursive = "true"

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + StarLightStorageOperationInterface

	Logger.WithContext(ctx).Debug(
		"SLClient:Mkdir request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(StarLightHttpHeaderAuth, o.token)

	err, respBody := Do(
		ctx,
		url+"?"+values.Encode(),
		http.MethodPost,
		header,
		nil,
		o.slClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"SLClient:Mkdir response.",
		" target: ", target,
		" response: ", string(respBody))

	resp := new(SLBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SLSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SLClient:Mkdir response failed.",
			" target: ", target,
			" Code: ", resp.Code,
			" Info: ", resp.Info)
		return errors.New(resp.Info)
	}
	Logger.WithContext(ctx).Debug(
		"SLClient:Mkdir finish.")
	return err
}

func (o *SLClient) Rm(
	ctx context.Context,
	target string) (err error) {

	Logger.WithContext(ctx).Debug(
		"SLClient:Rm start.",
		" target: ", target)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SLClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(SLStorageOperationReq)
	input.Target = target
	input.Opt = StarLightStorageOperationRm
	input.Force = "true"
	input.Recursive = "true"

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + StarLightStorageOperationInterface

	Logger.WithContext(ctx).Debug(
		"SLClient:Rm request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(StarLightHttpHeaderAuth, o.token)

	err, respBody := Do(
		ctx,
		url+"?"+values.Encode(),
		http.MethodPost,
		header,
		nil,
		o.slClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"SLClient:Rm response.",
		" target: ", target,
		" response: ", string(respBody))

	resp := new(SLBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SLSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SLClient:Rm response failed.",
			" target: ", target,
			" Code: ", resp.Code,
			" Info: ", resp.Info)
		return errors.New(resp.Info)
	}
	Logger.WithContext(ctx).Debug(
		"SLClient:Rm finish.")
	return err
}

// UploadChunks Content-Range: <unit> <start>-<end>/<total>
func (o *SLClient) UploadChunks(
	ctx context.Context,
	file,
	contentRange string,
	data io.Reader) (err error, resp *SLUploadChunksResponse) {

	Logger.WithContext(ctx).Debug(
		"SLClient:UploadChunks start.",
		" file: ", file,
		" contentRange: ", contentRange)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SLClient.refreshToken failed.",
			" err: ", err)
		return err, resp
	}

	input := new(SLUploadChunksReq)
	input.File = file
	input.Overwrite = "true"

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, resp
	}

	url := o.endpoint + StarLightUploadInterface

	Logger.WithContext(ctx).Debug(
		"SLClient:UploadChunks request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeText)
	header.Add(StarLightHttpHeaderAuth, o.token)
	header.Add(HttpHeaderContentRange, contentRange)

	err, respBody := Do(
		ctx,
		url+"?"+values.Encode(),
		http.MethodPut,
		header,
		data,
		o.slClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"SLClient:UploadChunks response.",
		" file: ", file,
		" contentRange: ", contentRange,
		" response: ", string(respBody))

	resp = new(SLUploadChunksResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, resp
	}

	if SLSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SLClient:UploadChunks response failed.",
			" file: ", file,
			" contentRange: ", contentRange,
			" Code: ", resp.Code,
			" Info: ", resp.Info)
		return errors.New(resp.Info), resp
	}
	Logger.WithContext(ctx).Debug(
		"SLClient:UploadChunks finish.")
	return nil, resp
}

func (o *SLClient) List(
	ctx context.Context,
	path string) (err error, output *SLListOutput) {

	Logger.WithContext(ctx).Debug(
		"SLClient:List start.",
		" path: ", path)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SLClient.refreshToken failed.",
			" err: ", err)
		return err, output
	}

	input := new(SLListReq)
	input.Dir = path
	input.ShowHidden = "true"

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, output
	}

	url := o.endpoint + StarLightListInterface

	Logger.WithContext(ctx).Debug(
		"SLClient:List request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(StarLightHttpHeaderAuth, o.token)

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

	response, err := o.slClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"slClient.Do failed.",
			" err: ", err)
		return err, output
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
		return err, output
	}

	Logger.WithContext(ctx).Debug(
		"SLClient:List response.",
		" path: ", path,
		" response: ", string(respBodyBuf))

	output = new(SLListOutput)
	err = json.Unmarshal(respBodyBuf, output)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, output
	}

	Logger.WithContext(ctx).Debug(
		"SLClient:List finish.")
	return err, output
}

func (o *SLClient) DownloadChunks(
	ctx context.Context,
	file,
	contentRange string) (err error, output *SLDownloadPartOutput) {

	Logger.WithContext(ctx).Debug(
		"SLClient:DownloadChunks start.",
		" file: ", file,
		" contentRange: ", contentRange)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SLClient.refreshToken failed.",
			" err: ", err)
		return err, output
	}

	input := new(SLDownloadReq)
	input.File = file

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, output
	}

	url := o.endpoint + StarLightDownloadInterface

	Logger.WithContext(ctx).Debug(
		"SLClient:DownloadChunks request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(StarLightHttpHeaderAuth, o.token)
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

	response, err := o.slClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"slClient.Do failed.",
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
			"SLClient:DownloadChunks response.",
			" file: ", file,
			" contentRange: ", contentRange,
			" response: ", string(respBodyBuf))

		var resp *SLBaseResponse
		err = json.Unmarshal(respBodyBuf, resp)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"json.Unmarshal failed.",
				" err: ", err)
			return err, output
		}

		Logger.WithContext(ctx).Error(
			"SLClient:DownloadChunks response failed.",
			" file: ", file,
			" contentRange: ", contentRange,
			" Code: ", resp.Code,
			" Info: ", resp.Info)

		return errors.New(resp.Info), output
	}

	output = new(SLDownloadPartOutput)
	output.Body = response.Body

	Logger.WithContext(ctx).Debug(
		"SLClient:DownloadChunks finish.")
	return err, output
}
