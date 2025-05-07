package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/google/go-querystring/query"
	"github.com/hashicorp/go-retryablehttp"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
	// sg "github.com/urchinfs/sugon-sdk/sugon"
)

type SugonClient struct {
	user        string
	password    string
	endpoint    string
	url         string
	token       string
	orgId       string
	clusterId   string
	sugonClient *retryablehttp.Client
}

func (o *SugonClient) Init(
	ctx context.Context,
	user,
	password,
	endpoint,
	url,
	orgId,
	clusterId string,
	reqTimeout,
	maxConnection int32) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:Init start.",
		" user: ", "***",
		" password: ", "***",
		" endpoint: ", endpoint,
		" url: ", url,
		" orgId: ", orgId,
		" clusterId: ", clusterId,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

	o.user = user
	o.password = password
	o.endpoint = endpoint
	o.url = url
	o.orgId = orgId
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
	o.sugonClient = retryablehttp.NewClient()
	o.sugonClient.RetryMax = 3
	o.sugonClient.RetryWaitMin = 1 * time.Second
	o.sugonClient.RetryWaitMax = 5 * time.Second
	o.sugonClient.HTTPClient.Timeout = timeout
	o.sugonClient.HTTPClient.Transport = transport

	Logger.WithContext(ctx).Debug(
		"SugonClient:Init finish.")
}

func (o *SugonClient) checkToken(
	ctx context.Context) (valid bool) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:checkToken start.")

	valid = false

	url := o.url + SugonGetTokenInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:checkToken request.",
		" url: ", url)

	header := make(http.Header)
	header.Add(SugonHttpHeaderToken, o.token)

	err, respBody := Do(
		ctx,
		url,
		http.MethodGet,
		header,
		nil,
		o.sugonClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return valid
	}
	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	resp := new(SugonBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return valid
	}

	if SugonSuccessCode == resp.Code {
		if tokenState, ok := resp.Data.(string); ok {
			if TokenStateValid == tokenState {
				Logger.WithContext(ctx).Debug(
					"SugonClient:checkToken token state valid.")
				valid = true
			} else {
				Logger.WithContext(ctx).Debug(
					"SugonClient:checkToken token state invalid.")
				valid = false
			}
			Logger.WithContext(ctx).Debug(
				"SugonClient:checkToken finish.")
			return valid
		} else {
			Logger.WithContext(ctx).Error(
				"response data invalid.",
				" response: ", string(respBody))
			return valid
		}
	} else {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" Code: ", resp.Code,
			" Msg: ", resp.Msg)
		return valid
	}
}

func (o *SugonClient) refreshToken(
	ctx context.Context) (err error) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:refreshToken start.")

	tokenValid := false
	tokenValid = o.checkToken(ctx)
	if true == tokenValid {
		Logger.WithContext(ctx).Info(
			"token valid, no need refresh.")
		return err
	}

	url := o.url + SugonPostTokenInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:refreshToken request.",
		" url: ", url)

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(SugonHttpHeaderUser, o.user)
	header.Add(SugonHttpHeaderPassword, o.password)
	header.Add(SugonHttpHeaderOrgId, o.orgId)

	err, respBody := Do(
		ctx,
		url,
		http.MethodPost,
		header,
		nil,
		o.sugonClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	resp := new(SugonPostTokenResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SugonSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" Code: ", resp.Code,
			" Msg: ", resp.Msg)
		return errors.New(resp.Msg)
	}

	refresh := false
	for _, token := range resp.Data {
		if token.ClusterId == o.clusterId {
			o.token = token.Token
			refresh = true
			break
		}
	}

	if true != refresh {
		Logger.WithContext(ctx).Error(
			"response data can not refresh token.",
			" response: ", string(respBody))
		return errors.New("response data can not refresh token")
	}

	Logger.WithContext(ctx).Debug(
		"SugonClient:refreshToken finish.")
	return err
}

func (o *SugonClient) Mkdir(
	ctx context.Context,
	path string) (err error) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:Mkdir start.",
		" path: ", path)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(SugonMkdirReq)
	input.Path = path
	input.CreateParents = true

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + SugonMkdirInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:Mkdir request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(SugonHttpHeaderToken, o.token)

	err, respBody := Do(
		ctx,
		url+"?"+values.Encode(),
		http.MethodPost,
		header,
		nil,
		o.sugonClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"SugonClient:Mkdir response.",
		" path: ", path,
		" response: ", string(respBody))

	resp := new(SugonBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SugonErrFileExist == resp.Code {
		Logger.WithContext(ctx).Info(
			"Already exist.")
		return err
	} else if SugonSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SugonClient:Mkdir response failed.",
			" path: ", path,
			" Code: ", resp.Code,
			" Msg: ", resp.Msg)
		return errors.New(resp.Msg)
	}

	Logger.WithContext(ctx).Debug(
		"SugonClient:Mkdir finish.")
	return err
}

func (o *SugonClient) Delete(
	ctx context.Context,
	paths string) (err error) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:Delete start.",
		" paths: ", paths)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(SugonDeleteReq)
	input.Paths = paths
	input.Recursive = true

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + SugonDeleteInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:Delete request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(SugonHttpHeaderToken, o.token)

	err, respBody := Do(
		ctx,
		url+"?"+values.Encode(),
		http.MethodPost,
		header,
		nil,
		o.sugonClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"SugonClient:Delete response.",
		" paths: ", paths,
		" response: ", string(respBody))

	resp := new(SugonBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SugonErrFileNotExist == resp.Code {
		Logger.WithContext(ctx).Info(
			"Already not exist.")
		return err
	} else if SugonSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SugonClient:Delete response failed.",
			" paths: ", paths,
			" Code: ", resp.Code,
			" Msg: ", resp.Msg)
		return errors.New(resp.Msg)
	}

	Logger.WithContext(ctx).Debug(
		"SugonClient:Delete finish.")
	return err
}

func (o *SugonClient) Upload(
	ctx context.Context,
	fileName,
	path string,
	data io.Reader) (err error) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:Upload start.",
		" fileName: ", fileName,
		" path: ", path)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + SugonUploadInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:Upload request.",
		" url: ", url)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		SugonMultiPartFormFiledFile,
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

	_ = writer.WriteField(
		SugonMultiPartFormFiledCover,
		SugonMultiPartFormFiledCoverECover)

	_ = writer.WriteField(
		SugonMultiPartFormFiledFileName,
		fileName)

	_ = writer.WriteField(
		SugonMultiPartFormFiledPath,
		path)

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
	reqHttp.Header.Set(SugonHttpHeaderToken, o.token)
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return err
	}

	response, err := o.sugonClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"sugonClient.Do failed.",
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
		"SugonClient:Upload response.",
		" fileName: ", fileName,
		" path: ", path,
		" response: ", string(respBodyBuf))

	resp := new(SugonBaseResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SugonErrFileExist == resp.Code {
		Logger.WithContext(ctx).Info(
			"Already exist.")
		return err
	} else if SugonSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SugonClient:Upload response failed.",
			" fileName: ", fileName,
			" path: ", path,
			" Code: ", resp.Code,
			" Msg: ", resp.Msg)
		return errors.New(resp.Msg)
	}

	Logger.WithContext(ctx).Debug(
		"SugonClient:Upload finish.")
	return err
}

func (o *SugonClient) UploadChunks(
	ctx context.Context,
	file,
	fileName,
	path,
	relativePath string,
	chunkNumber,
	totalChunks int32,
	totalSize,
	chunkSize,
	currentChunkSize int64,
	data io.Reader) (err error) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:UploadChunks start.",
		" file: ", file,
		" fileName: ", fileName,
		" path: ", path,
		" relativePath: ", relativePath,
		" chunkNumber: ", chunkNumber,
		" totalChunks: ", totalChunks,
		" totalSize: ", totalSize,
		" chunkSize: ", chunkSize,
		" currentChunkSize: ", currentChunkSize)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + SugonUploadChunksInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:UploadChunks request.",
		" url: ", url)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		SugonMultiPartFormFiledFile,
		file)
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

	_ = writer.WriteField(
		SugonMultiPartFormFiledChunkNumber,
		strconv.FormatInt(int64(chunkNumber), 10))

	_ = writer.WriteField(
		SugonMultiPartFormFiledCover,
		SugonMultiPartFormFiledCoverECover)

	_ = writer.WriteField(
		SugonMultiPartFormFiledFileName,
		fileName)

	_ = writer.WriteField(
		SugonMultiPartFormFiledPath,
		path)

	_ = writer.WriteField(
		SugonMultiPartFormFiledRelativePath,
		relativePath)

	_ = writer.WriteField(
		SugonMultiPartFormFiledTotalChunks,
		strconv.FormatInt(int64(totalChunks), 10))

	_ = writer.WriteField(
		SugonMultiPartFormFiledTotalSize,
		strconv.FormatInt(totalSize, 10))

	_ = writer.WriteField(
		SugonMultiPartFormFiledChunkSize,
		strconv.FormatInt(chunkSize, 10))

	_ = writer.WriteField(
		SugonMultiPartFormFiledCurrentChunkSize,
		strconv.FormatInt(currentChunkSize, 10))

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
	reqHttp.Header.Set(SugonHttpHeaderToken, o.token)
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return err
	}

	response, err := o.sugonClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"sugonClient.Do failed.",
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
		"SugonClient:UploadChunks response.",
		" fileName: ", fileName,
		" path: ", path,
		" relativePath: ", relativePath,
		" chunkNumber: ", chunkNumber,
		" totalChunks: ", totalChunks,
		" totalSize: ", totalSize,
		" chunkSize: ", chunkSize,
		" currentChunkSize: ", currentChunkSize,
		"response: ", string(respBodyBuf))

	resp := new(SugonBaseResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SugonErrFileExist == resp.Code {
		Logger.WithContext(ctx).Info(
			"Already exist.")
		return err
	} else if SugonSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SugonClient:UploadChunks response failed.",
			" fileName: ", fileName,
			" path: ", path,
			" relativePath: ", relativePath,
			" chunkNumber: ", chunkNumber,
			" totalChunks: ", totalChunks,
			" totalSize: ", totalSize,
			" chunkSize: ", chunkSize,
			" currentChunkSize: ", currentChunkSize,
			" Code: ", resp.Code,
			" Msg: ", resp.Msg)
		return errors.New(resp.Msg)
	}

	Logger.WithContext(ctx).Debug(
		"SugonClient:UploadChunks finish.")
	return err
}

func (o *SugonClient) MergeChunks(
	ctx context.Context,
	fileName,
	path,
	relativePath string) (err error) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:MergeChunks start.",
		" fileName: ", fileName,
		" path: ", path,
		" relativePath: ", relativePath)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			" err: ", err)
		return err
	}

	input := new(SugonMergeChunksReq)
	input.Filename = fileName
	input.Path = path
	input.RelativePath = relativePath
	input.Cover = SugonMultiPartFormFiledCoverECover

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err
	}

	url := o.endpoint + SugonMergeChunksInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:MergeChunks request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(SugonHttpHeaderToken, o.token)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeUrlEncoded)

	err, respBody := Do(
		ctx,
		url,
		http.MethodPost,
		header,
		strings.NewReader(values.Encode()),
		o.sugonClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err
	}
	Logger.WithContext(ctx).Debug(
		"SugonClient:MergeChunks response.",
		" fileName: ", fileName,
		" path: ", path,
		" relativePath: ", relativePath,
		" response: ", string(respBody))

	resp := new(SugonBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err
	}

	if SugonErrFileExist == resp.Code {
		Logger.WithContext(ctx).Info(
			"Already exist.")
		return err
	} else if SugonSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SugonClient:MergeChunks response failed.",
			" fileName: ", fileName,
			" path: ", path,
			" relativePath: ", relativePath,
			" Code: ", resp.Code,
			" Msg: ", resp.Msg)
		return errors.New(resp.Msg)
	}

	Logger.WithContext(ctx).Debug(
		"SugonClient:MergeChunks finish.")
	return err
}

func (o *SugonClient) List(
	ctx context.Context,
	path string,
	start,
	limit int32) (err error, output *SugonListResponseData) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:List start.",
		" path: ", path,
		" start: ", start,
		" limit: ", limit)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			" err: ", err)
		return err, output
	}

	input := new(SugonListReq)
	input.Path = path
	input.Start = start
	input.Limit = limit

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, output
	}

	url := o.endpoint + SugonListInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:List request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(SugonHttpHeaderToken, o.token)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	err, respBody := Do(
		ctx,
		url+"?"+values.Encode(),
		http.MethodGet,
		header,
		nil,
		o.sugonClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
			" err: ", err)
		return err, output
	}
	Logger.WithContext(ctx).Debug(
		"SugonClient:List response.",
		" path: ", path,
		" start: ", start,
		" limit: ", limit,
		" response: ", string(respBody))

	resp := new(SugonBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return err, output
	}

	if SugonSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"SugonClient:List response failed.",
			" path: ", path,
			" start: ", start,
			" limit: ", limit,
			" Code: ", resp.Code,
			" Msg: ", resp.Msg)
		return errors.New(resp.Msg), output
	}

	if output, ok :=
		resp.Data.(*SugonListResponseData); ok {

		Logger.WithContext(ctx).Debug(
			"SugonClient:List finish.")
		return err, output
	} else {
		Logger.WithContext(ctx).Error(
			"response body invalid.",
			" response: ", string(respBody))
		return errors.New("response body invalid"), output
	}
}

func (o *SugonClient) DownloadChunks(
	ctx context.Context,
	path,
	contentRange string) (err error, output *SugonDownloadPartOutput) {

	Logger.WithContext(ctx).Debug(
		"SugonClient:DownloadChunks start.",
		" path: ", path,
		" contentRange: ", contentRange)

	err = o.refreshToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			" err: ", err)
		return err, output
	}

	input := new(SugonDownloadReq)
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, output
	}

	url := o.endpoint + SugonDownloadInterface

	Logger.WithContext(ctx).Debug(
		"SugonClient:DownloadChunks request.",
		" url: ", url,
		" query: ", values.Encode())

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(SugonHttpHeaderToken, o.token)
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

	response, err := o.sugonClient.Do(request)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"sugonClient.Do failed.",
			" err: ", err)
		return err, output
	}

	output = new(SugonDownloadPartOutput)
	output.Body = response.Body

	Logger.WithContext(ctx).Debug(
		"SugonClient:DownloadChunks finish.")
	return err, output
}
