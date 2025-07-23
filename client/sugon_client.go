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
	"go.uber.org/zap"
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

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Init start.",
		zap.String("endpoint", endpoint),
		zap.String("url", url),
		zap.String("orgId", orgId),
		zap.String("clusterId", clusterId),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

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

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Init finish.")
}

func (o *SugonClient) checkToken(
	ctx context.Context) (valid bool) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:checkToken start.")

	valid = false

	url := o.url + SugonGetTokenInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:checkToken request.",
		zap.String("url", url))

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
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return valid
	}
	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	resp := new(SugonBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return valid
	}

	if SugonSuccessCode == resp.Code {
		if tokenState, ok := resp.Data.(string); ok {
			if TokenStateValid == tokenState {
				InfoLogger.WithContext(ctx).Debug(
					"SugonClient:checkToken token state valid.")
				valid = true
			} else {
				InfoLogger.WithContext(ctx).Debug(
					"SugonClient:checkToken token state invalid.")
				valid = false
			}
			InfoLogger.WithContext(ctx).Debug(
				"SugonClient:checkToken finish.")
			return valid
		} else {
			ErrorLogger.WithContext(ctx).Error(
				"response data invalid.",
				zap.String("response", string(respBody)))
			return valid
		}
	} else {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.String("code", resp.Code),
			zap.String("msg", resp.Msg))
		return valid
	}
}

func (o *SugonClient) refreshToken(
	ctx context.Context) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:refreshToken start.")

	tokenValid := false
	tokenValid = o.checkToken(ctx)
	if true == tokenValid {
		InfoLogger.WithContext(ctx).Info(
			"token valid, no need refresh.")
		return err
	}

	url := o.url + SugonPostTokenInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:refreshToken request.",
		zap.String("url", url))

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
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	resp := new(SugonPostTokenResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if SugonSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.String("code", resp.Code),
			zap.String("msg", resp.Msg))
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
		ErrorLogger.WithContext(ctx).Error(
			"response data can not refresh token.",
			zap.String("response", string(respBody)))
		return errors.New("response data can not refresh token")
	}

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:refreshToken finish.")
	return err
}

func (o *SugonClient) Mkdir(
	ctx context.Context,
	path string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Mkdir start.",
		zap.String("path", path))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			zap.Error(err))
		return err
	}

	input := new(SugonMkdirReq)
	input.Path = path
	input.CreateParents = true

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err
	}

	url := o.endpoint + SugonMkdirInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Mkdir request.",
		zap.String("url", url),
		zap.String("query", values.Encode()))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(SugonHttpHeaderToken, o.token)

	resp := new(SugonBaseResponse)
	err, respBody := Do(
		ctx,
		url+"?"+values.Encode(),
		http.MethodPost,
		header,
		nil,
		o.sugonClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Mkdir response.",
		zap.String("path", path),
		zap.String("response", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}
	if SugonSuccessCode != resp.Code &&
		SugonErrFileExist != resp.Code {

		ErrorLogger.WithContext(ctx).Error(
			"SugonClient:Mkdir failed.",
			zap.String("path", path),
			zap.String("message", resp.Msg))
		return errors.New(resp.Msg)
	}

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Mkdir finish.")
	return err
}

func (o *SugonClient) Delete(
	ctx context.Context,
	paths string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Delete start.",
		zap.String("paths", paths))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			zap.Error(err))
		return err
	}

	input := new(SugonDeleteReq)
	input.Paths = paths
	input.Recursive = true

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err
	}

	url := o.endpoint + SugonDeleteInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Delete request.",
		zap.String("url", url),
		zap.String("query", values.Encode()))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(SugonHttpHeaderToken, o.token)

	resp := new(SugonBaseResponse)
	err, respBody := Do(
		ctx,
		url+"?"+values.Encode(),
		http.MethodPost,
		header,
		nil,
		o.sugonClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Delete response.",
		zap.String("paths", paths),
		zap.String("response", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if SugonSuccessCode != resp.Code &&
		SugonErrFileNotExist != resp.Code {

		ErrorLogger.WithContext(ctx).Error(
			"SugonClient:Delete failed.",
			zap.String("paths", paths),
			zap.String("message", resp.Msg))
		return errors.New(resp.Msg)
	}

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Delete finish.")
	return err
}

func (o *SugonClient) Upload(
	ctx context.Context,
	fileName,
	path string,
	data io.Reader) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Upload start.",
		zap.String("fileName", fileName),
		zap.String("path", path))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			zap.Error(err))
		return err
	}

	url := o.endpoint + SugonUploadInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:Upload request.",
		zap.String("url", url))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		SugonMultiPartFormFiledFile,
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
	reqHttp.Header.Set(SugonHttpHeaderToken, o.token)
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err
	}

	resp := new(SugonBaseResponse)
	response, err := o.sugonClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"sugonClient.Do failed.",
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
		"SugonClient:Upload response.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("response", string(respBodyBuf)))

	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if SugonSuccessCode != resp.Code &&
		SugonErrFileExist != resp.Code {

		ErrorLogger.WithContext(ctx).Error(
			"SugonClient:Upload failed.",
			zap.String("fileName", fileName),
			zap.String("path", path),
			zap.String("message", resp.Msg))
		return errors.New(resp.Msg)
	}

	InfoLogger.WithContext(ctx).Debug(
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
	data io.Reader) (err error, resp *SugonBaseResponse) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:UploadChunks start.",
		zap.String("file", file),
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("relativePath", relativePath),
		zap.Int32("chunkNumber", chunkNumber),
		zap.Int32("totalChunks", totalChunks),
		zap.Int64("totalSize", totalSize),
		zap.Int64("chunkSize", chunkSize),
		zap.Int64("currentChunkSize", currentChunkSize))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			zap.Error(err))
		return err, resp
	}

	url := o.endpoint + SugonUploadChunksInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:UploadChunks request.",
		zap.String("url", url))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		SugonMultiPartFormFiledFile,
		file)
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
	reqHttp.Header.Set(SugonHttpHeaderToken, o.token)
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err, resp
	}

	response, err := o.sugonClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"sugonClient.Do failed.",
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
		"SugonClient:UploadChunks response.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("relativePath", relativePath),
		zap.Int32("chunkNumber", chunkNumber),
		zap.Int32("totalChunks", totalChunks),
		zap.Int64("totalSize", totalSize),
		zap.Int64("chunkSize", chunkSize),
		zap.Int64("currentChunkSize", currentChunkSize),
		zap.String("response", string(respBodyBuf)))

	resp = new(SugonBaseResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SugonSuccessCode != resp.Code &&
		SugonErrFileExist != resp.Code {

		ErrorLogger.WithContext(ctx).Error(
			"SugonClient:UploadChunks failed.",
			zap.String("fileName", fileName),
			zap.String("path", path),
			zap.String("relativePath", relativePath),
			zap.Int32("chunkNumber", chunkNumber),
			zap.Int32("totalChunks", totalChunks),
			zap.Int64("totalSize", totalSize),
			zap.Int64("chunkSize", chunkSize),
			zap.Int64("currentChunkSize", currentChunkSize),
			zap.String("message", resp.Msg))
		return errors.New(resp.Msg), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:UploadChunks finish.")
	return nil, resp
}

func (o *SugonClient) MergeChunks(
	ctx context.Context,
	fileName,
	path,
	relativePath string) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:MergeChunks start.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("relativePath", relativePath))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			zap.Error(err))
		return err
	}

	input := new(SugonMergeChunksReq)
	input.Filename = fileName
	input.Path = path
	input.RelativePath = relativePath
	input.Cover = SugonMultiPartFormFiledCoverECover

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err
	}

	url := o.endpoint + SugonMergeChunksInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:MergeChunks request.",
		zap.String("url", url),
		zap.String("request", values.Encode()))

	header := make(http.Header)
	header.Set(HttpHeaderContentType, HttpHeaderContentTypeUrlEncoded)
	header.Add(SugonHttpHeaderToken, o.token)

	resp := new(SugonBaseResponse)
	err, respBody := Do(
		ctx,
		url,
		http.MethodPost,
		header,
		strings.NewReader(values.Encode()),
		o.sugonClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err
	}
	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:MergeChunks response.",
		zap.String("fileName", fileName),
		zap.String("path", path),
		zap.String("relativePath", relativePath),
		zap.String("response", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if SugonSuccessCode != resp.Code &&
		SugonErrFileExist != resp.Code {

		ErrorLogger.WithContext(ctx).Error(
			"SugonClient:MergeChunks failed.",
			zap.String("fileName", fileName),
			zap.String("path", path),
			zap.String("relativePath", relativePath),
			zap.String("message", resp.Msg))
		return errors.New(resp.Msg)
	}

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:MergeChunks finish.")
	return err
}

func (o *SugonClient) List(
	ctx context.Context,
	path string,
	start,
	limit int32) (err error, output *SugonListResponseData) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:List start.",
		zap.String("path", path),
		zap.Int32("start", start),
		zap.Int32("limit", limit))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			zap.Error(err))
		return err, output
	}

	input := new(SugonListReq)
	input.Path = path
	input.Start = start
	input.Limit = limit

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, output
	}

	url := o.endpoint + SugonListInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:List request.",
		zap.String("url", url),
		zap.String("query", values.Encode()))

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
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, output
	}
	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:List response.",
		zap.String("path", path),
		zap.Int32("start", start),
		zap.Int32("limit", limit),
		zap.String("response", string(respBody)))

	resp := new(SugonListResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, output
	}

	if SugonSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"SugonClient:List response failed.",
			zap.String("path", path),
			zap.Int32("start", start),
			zap.Int32("limit", limit),
			zap.String("code", resp.Code),
			zap.String("msg", resp.Msg))
		return errors.New(resp.Msg), output
	}

	output = new(SugonListResponseData)
	output = resp.Data

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:List finish.")
	return err, output
}

func (o *SugonClient) DownloadChunks(
	ctx context.Context,
	path,
	contentRange string) (err error, output *SugonDownloadPartOutput) {

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:DownloadChunks start.",
		zap.String("path", path),
		zap.String("contentRange", contentRange))

	err = o.refreshToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"SugonClient.refreshToken failed.",
			zap.Error(err))
		return err, output
	}

	input := new(SugonDownloadReq)
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, output
	}

	url := o.endpoint + SugonDownloadInterface

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:DownloadChunks request.",
		zap.String("url", url),
		zap.String("query", values.Encode()))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)
	header.Add(SugonHttpHeaderToken, o.token)
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

	response, err := o.sugonClient.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"sugonClient.Do failed.",
			zap.Error(err))
		return err, output
	}

	if HttpHeaderContentTypeJson ==
		response.Header.Get(HttpHeaderContentType) {

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
			return err, output
		}

		InfoLogger.WithContext(ctx).Debug(
			"SugonClient:DownloadChunks response.",
			zap.String("path", path),
			zap.String("contentRange", contentRange),
			zap.String("response", string(respBodyBuf)))

		var resp = new(SugonBaseResponse)
		err = json.Unmarshal(respBodyBuf, resp)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"json.Unmarshal failed.",
				zap.Error(err))
			return err, output
		}

		ErrorLogger.WithContext(ctx).Error(
			"SugonClient:DownloadChunks response failed.",
			zap.String("path", path),
			zap.String("contentRange", contentRange),
			zap.String("code", resp.Code),
			zap.String("msg", resp.Msg))

		return errors.New(resp.Msg), output
	}

	output = new(SugonDownloadPartOutput)
	output.Body = response.Body

	InfoLogger.WithContext(ctx).Debug(
		"SugonClient:DownloadChunks finish.")
	return err, output
}
