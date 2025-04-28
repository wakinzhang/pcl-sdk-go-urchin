package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	signerV4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/google/go-querystring/query"
	"github.com/hashicorp/go-retryablehttp"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"strings"
	"time"
)

type JCSProxyClient struct {
	jcsClient *retryablehttp.Client
}

func (o *JCSProxyClient) Init(
	ctx context.Context,
	reqTimeout,
	maxConnection int) {

	Logger.WithContext(ctx).Debug(
		"JCSProxyClient:Init start.",
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
		"JCSProxyClient:Init finish.")
}

func (o *JCSProxyClient) ListWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error, resp *JCSListResponse) {

	Logger.WithContext(ctx).Debug(
		"JCSProxyClient:ListWithSignedUrl start.",
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
		"JCSProxyClient:ListWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSProxyClient) UploadFileWithSignedUrl(
	ctx context.Context,
	signedUrl string,
	data io.Reader) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCSProxyClient:UploadFileWithSignedUrl start.",
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
		"JCSProxyClient:UploadFileWithSignedUrl finish.")
	return nil
}

func (o *JCSProxyClient) NewMultiPartUploadWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error,
	resp *JCSNewMultiPartUploadResponse) {

	Logger.WithContext(ctx).Debug(
		"JCSProxyClient:NewMultiPartUploadWithSignedUrl start.",
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
		"JCSProxyClient:NewMultiPartUploadWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSProxyClient) UploadPartWithSignedUrl(
	ctx context.Context,
	signedUrl string,
	data io.Reader) (err error, resp *JCSBaseResponse) {

	Logger.WithContext(ctx).Debug(
		"JCSProxyClient:UploadPartWithSignedUrl start.",
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
		"JCSProxyClient:UploadPartWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSProxyClient) CompleteMultiPartUploadWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error,
	resp *JCSCompleteMultiPartUploadResponse) {

	Logger.WithContext(ctx).Debug(
		"JCSProxyClient:CompleteMultiPartUploadWithSignedUrl start.",
		" signedUrl: ", signedUrl)

	resp = new(JCSCompleteMultiPartUploadResponse)

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
		"JCSProxyClient:CompleteMultiPartUploadWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSProxyClient) DownloadPartWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error, output *JCSDownloadPartOutput) {

	Logger.WithContext(ctx).Debug(
		"JCSProxyClient:DownloadPartWithSignedUrl start.",
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
		"JCSProxyClient:DownloadPartWithSignedUrl finish.")
	return nil, output
}

type JCSClient struct {
	accessKey   string
	secretKey   string
	endPoint    string
	authService string
	authRegion  string
	userID      int32
	bucketID    int32
	jcsClient   *retryablehttp.Client
}

func (o *JCSClient) Init(
	ctx context.Context,
	accessKey,
	secretKey,
	endPoint,
	authService,
	authRegion string,
	userID,
	bucketID,
	reqTimeout,
	maxConnection int32) {

	Logger.WithContext(ctx).Debug(
		"Function JCSClient:Init start.",
		" accessKey: ", "***",
		" secretKey: ", "***",
		" endPoint: ", endPoint,
		" authService: ", authService,
		" authRegion: ", authRegion,
		" userID: ", userID,
		" bucketID: ", bucketID,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

	o.accessKey = accessKey
	o.secretKey = secretKey
	o.endPoint = endPoint
	o.authService = authService
	o.authRegion = authRegion
	o.userID = userID
	o.bucketID = bucketID

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
	o.jcsClient = retryablehttp.NewClient()
	o.jcsClient.RetryMax = 3
	o.jcsClient.RetryWaitMin = 1 * time.Second
	o.jcsClient.RetryWaitMax = 5 * time.Second
	o.jcsClient.HTTPClient.Timeout = timeout
	o.jcsClient.HTTPClient.Transport = transport

	Logger.WithContext(ctx).Debug(
		"Function JCS:Init finish.")
}

func (o *JCSClient) sign(
	ctx context.Context,
	req *http.Request) (err error) {

	Logger.WithContext(ctx).Debug(
		"Function JCSClient:sign start.")

	prod := credentials.NewStaticCredentialsProvider(
		o.accessKey,
		o.secretKey,
		"")

	cred, err := prod.Retrieve(context.TODO())
	if nil != err {
		Logger.WithContext(ctx).Error(
			"CredentialsProvider:Retrieve failed.",
			" err: ", err)
		return err
	}

	payloadHash := ""
	if req.Body != nil {
		data, err := io.ReadAll(req.Body)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"http.Request.Body ReadAll failed.",
				" err: ", err)
			return err
		}
		_err := req.Body.Close()
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"http.Request.Body.Close failed.",
				" error:", _err)
		}
		Logger.WithContext(ctx).Debug(
			"http.Request.Body ReadAll.",
			" data: ", string(data))

		req.Body = io.NopCloser(bytes.NewReader(data))

		hash := sha256.New()
		hash.Write(data)
		payloadHash = hex.EncodeToString(hash.Sum(nil))
	} else {
		hash := sha256.Sum256([]byte(""))
		payloadHash = hex.EncodeToString(hash[:])
	}

	Logger.WithContext(ctx).Debug(
		"Signer:SignHTTP params."+
			" payloadHash: ", payloadHash)
	signer := signerV4.NewSigner()
	err = signer.SignHTTP(
		context.Background(),
		cred,
		req,
		payloadHash,
		o.authService,
		o.authRegion,
		time.Now())
	if nil != err {
		Logger.WithContext(ctx).Error(
			"Signer.SignHTTP failed.",
			" err: ", err)
		return err
	}

	Logger.WithContext(ctx).Debug(
		"Function JCSClient:sign finish.")
	return nil
}

func (o *JCSClient) preSign(
	ctx context.Context,
	req *http.Request,
	expiration int) (signedUrl string, err error) {

	Logger.WithContext(ctx).Debug(
		"Function JCSClient:preSign start.")

	urlQuery := req.URL.Query()
	urlQuery.Add("X-Expires", fmt.Sprintf("%v", expiration))
	req.URL.RawQuery = urlQuery.Encode()

	prod := credentials.NewStaticCredentialsProvider(
		o.accessKey,
		o.secretKey,
		"")

	cred, err := prod.Retrieve(context.TODO())
	if nil != err {
		Logger.WithContext(ctx).Error(
			"CredentialsProvider:Retrieve failed.",
			" err: ", err)
		return signedUrl, err
	}

	signer := signerV4.NewSigner()

	signedUrl, _, err = signer.PresignHTTP(
		context.Background(),
		cred,
		req,
		"",
		o.authService,
		o.authRegion,
		time.Now())
	if nil != err {
		Logger.WithContext(ctx).Error(
			"signerV4:PreSignHTTP failed.",
			" err: ", err)
		return signedUrl, err
	}
	Logger.WithContext(ctx).Debug(
		"Function JCSClient:preSign finish.")

	return signedUrl, err
}

func (o *JCSClient) CreateBucket(
	ctx context.Context,
	bucketName string) (
	resp *JCSCreateBucketResponse, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreateBucket start.",
		" bucketName: ", bucketName)

	input := new(JCSCreateBucketReq)
	input.UserID = o.userID
	input.Name = bucketName

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return resp, err
	}

	url := o.endPoint + JCSCreateBucketInterface

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreateBucket request.",
		" url: ", url,
		" reqBody: ", string(reqBody))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		strings.NewReader(string(reqBody)))
	if err != nil {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return resp, err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			" err: ", err)
		return resp, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return resp, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			" err: ", err)
		return resp, err
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
		return resp, err
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBodyBuf))

	resp = new(JCSCreateBucketResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return resp, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreateBucket finish.")
	return resp, err
}

func (o *JCSClient) CreatePackage(
	ctx context.Context,
	packageName string) (
	resp *JCSCreatePackageResponse, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage start.",
		" packageName: ", packageName)

	input := new(JCSCreatePackageReq)
	input.UserID = o.userID
	input.BucketID = o.bucketID
	input.Name = packageName

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return resp, err
	}

	url := o.endPoint + JCSCreatePackageInterface

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage request.",
		" url: ", url,
		" reqBody: ", string(reqBody))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		strings.NewReader(string(reqBody)))
	if err != nil {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return resp, err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			" err: ", err)
		return resp, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return resp, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			" err: ", err)
		return resp, err
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
		return resp, err
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBodyBuf))

	resp = new(JCSCreatePackageResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return resp, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage finish.")
	return resp, err
}

func (o *JCSClient) CreatePreSignedObjectListSignedUrl(
	ctx context.Context,
	packageID int32,
	path string,
	isPrefix, noRecursive bool,
	maxKeys int32,
	continuationToken string,
	expires int) (
	signedUrl string, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectListSignedUrl start.",
		" packageID: ", packageID,
		" path: ", path,
		" isPrefix: ", isPrefix,
		" noRecursive: ", noRecursive,
		" maxKeys: ", maxKeys,
		" continuationToken: ", continuationToken,
		" expires: ", expires)

	input := JCSListReq{}
	input.UserID = o.userID
	input.PackageID = packageID
	input.Path = path
	input.IsPrefix = isPrefix
	input.NoRecursive = noRecursive
	input.MaxKeys = maxKeys
	input.ContinuationToken = continuationToken

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return signedUrl, err
	}

	url := o.endPoint +
		JCSPreSignedObjectListInterface +
		"?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectListSignedUrl request.",
		" url: ", url)

	req, err := http.NewRequest(
		http.MethodGet,
		url,
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			" err: ", err)
		return signedUrl, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectListSignedUrl finish.",
		" signedUrl: ", signedUrl)
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectUploadSignedUrl(
	ctx context.Context,
	packageID int32,
	path string,
	expires int) (
	signedUrl string, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadSignedUrl start.",
		" packageID: ", packageID,
		" path: ", path,
		" expires: ", expires)

	input := JCSCreatePreSignedObjectUploadSignedUrlReq{}
	input.UserID = o.userID
	input.PackageID = packageID
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return signedUrl, err
	}

	url := o.endPoint +
		JCSPreSignedObjectUploadInterface +
		"?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadSignedUrl request.",
		" url: ", url)

	req, err := http.NewRequest(
		http.MethodPost,
		url,
		strings.NewReader(""))
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			" err: ", err)
		return signedUrl, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadSignedUrl finish.",
		" signedUrl: ", signedUrl)
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectNewMultipartUploadSignedUrl(
	ctx context.Context,
	packageID int32,
	path string,
	expires int) (
	signedUrl string, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectNewMultipartUploadSignedUrl"+
			" start.",
		" packageID: ", packageID,
		" path: ", path,
		" expires: ", expires)

	input := JCSNewMultiPartUploadReq{}
	input.UserID = o.userID
	input.PackageID = packageID
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return signedUrl, err
	}

	url := o.endPoint +
		JCSPreSignedObjectNewMultipartUploadInterface +
		"?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectNewMultipartUploadSignedUrl"+
			" request.",
		" url: ", url)

	req, err := http.NewRequest(
		http.MethodPost,
		url,
		strings.NewReader(""))
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			" err: ", err)
		return signedUrl, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectNewMultipartUploadSignedUrl"+
			" finish.",
		" signedUrl: ", signedUrl)
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectUploadPartSignedUrl(
	ctx context.Context,
	objectID, index int32,
	expires int) (
	signedUrl string, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadPartSignedUrl start.",
		" objectID: ", objectID,
		" index: ", index,
		" expires: ", expires)

	input := JCSUploadPartReqInfo{}

	input.UserID = o.userID
	input.ObjectID = objectID
	input.Index = index

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return signedUrl, err
	}

	url := o.endPoint +
		JCSPreSignedObjectUploadPartInterface +
		"?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadPartSignedUrl request.",
		" url: ", url)

	req, err := http.NewRequest(
		http.MethodPost,
		url,
		strings.NewReader(""))
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			" err: ", err)
		return signedUrl, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadPartSignedUrl finish.",
		" signedUrl: ", signedUrl)
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectCompleteMultipartUploadSignedUrl(
	ctx context.Context,
	objectID int32,
	indexes []int32,
	expires int) (
	signedUrl string, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:"+
			"CreatePreSignedObjectCompleteMultipartUploadSignedUrl start.",
		" objectID: ", objectID,
		" expires: ", expires)

	input := JCSCompleteMultiPartUploadReq{}
	input.UserID = o.userID
	input.ObjectID = objectID
	input.Indexes = indexes

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return signedUrl, err
	}

	url := o.endPoint +
		JCSPreSignedObjectCompleteMultipartUploadInterface +
		"?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"JCSClient:"+
			"CreatePreSignedObjectCompleteMultipartUploadSignedUrl request.",
		" url: ", url)

	req, err := http.NewRequest(
		http.MethodPost,
		url,
		strings.NewReader(""))
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			" err: ", err)
		return signedUrl, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:"+
			"CreatePreSignedObjectCompleteMultipartUploadSignedUrl finish.",
		" signedUrl: ", signedUrl)
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectDownloadSignedUrl(
	ctx context.Context,
	objectID int32,
	offset,
	length int64,
	expires int) (
	signedUrl string, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectDownloadSignedUrl start.",
		" objectID: ", objectID,
		" offset: ", offset,
		" length: ", length,
		" expires: ", expires)

	input := JCSDownloadReq{}
	input.UserID = o.userID
	input.ObjectID = objectID
	input.Offset = offset
	input.Length = length

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return signedUrl, err
	}

	url := o.endPoint +
		JCSPreSignedObjectDownloadInterface +
		"?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectDownloadSignedUrl request.",
		" url: ", url)

	req, err := http.NewRequest(
		http.MethodGet,
		url,
		nil)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			" err: ", err)
		return signedUrl, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectDownloadSignedUrl finish.",
		" signedUrl: ", signedUrl)
	return signedUrl, err
}

func (o *JCSClient) List(
	ctx context.Context,
	packageID int32,
	path string,
	isPrefix, noRecursive bool,
	maxKeys int32,
	continuationToken string) (
	listObjectsData *JCSListData, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:List start.",
		" packageID: ", packageID,
		" path: ", path,
		" isPrefix: ", isPrefix,
		" noRecursive: ", noRecursive,
		" maxKeys: ", maxKeys,
		" continuationToken: ", continuationToken)

	input := JCSListReq{}
	input.UserID = o.userID
	input.PackageID = packageID
	input.Path = path
	input.IsPrefix = isPrefix
	input.NoRecursive = noRecursive
	input.MaxKeys = maxKeys
	input.ContinuationToken = continuationToken

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return listObjectsData, err
	}

	url := o.endPoint +
		JCSListInterface +
		"?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"JCSClient:List request.",
		" url: ", url)

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		nil)
	if err != nil {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return listObjectsData, err
	}

	reqHttp.Header = header

	err = o.sign(ctx, reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			" err: ", err)
		return listObjectsData, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return listObjectsData, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			" err: ", err)
		return listObjectsData, err
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
		return listObjectsData, err
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBody))

	resp := new(JCSListResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return listObjectsData, err
	}

	if JCSSuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return listObjectsData, errors.New(resp.Message)
	}

	listObjectsData = new(JCSListData)
	listObjectsData = resp.Data

	Logger.WithContext(ctx).Debug(
		"JCSClient:List finish.")
	return listObjectsData, err
}

func (o *JCSClient) UploadFile(
	ctx context.Context,
	packageId int32,
	path string,
	data io.Reader) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:UploadFile start.",
		" packageId: ", packageId,
		" path: ", path)

	jCSUploadReqInfo := new(JCSUploadReqInfo)
	jCSUploadReqInfo.UserID = o.userID
	jCSUploadReqInfo.PackageID = packageId

	info, err := json.Marshal(jCSUploadReqInfo)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err
	}

	url := o.endPoint + JCSUploadInterface

	Logger.WithContext(ctx).Debug(
		"JCSClient:UploadFile request.",
		" url: ", url,
		" info: ", info)

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)
	go func() {
		defer func() {
			errMsg := pw.Close()
			if errMsg != nil {
				Logger.WithContext(ctx).Warn(
					"close io.Pipe pw failed.",
					" err: ", errMsg)
			}
			errMsg = writer.Close()
			if errMsg != nil {
				Logger.WithContext(ctx).Warn(
					"close multipart.Writer failed.",
					" err: ", errMsg)
			}
		}()
		_ = writer.WriteField(
			JCSMultiPartFormFiledInfo,
			string(info))

		part, err := writer.CreateFormFile(
			JCSMultiPartFormFiledFiles,
			path)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"writer.CreateFormFile failed.",
				" err: ", err)
			_ = pw.CloseWithError(err)
			return
		}
		_, err = io.Copy(part, data)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"io.Copy failed.",
				" err: ", err)
			_ = pw.CloseWithError(err)
			return
		}
	}()

	header := make(http.Header)
	header.Add(HttpHeaderContentType, writer.FormDataContentType())

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		pr)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return err
	}
	reqHttp.Header = header

	err = o.sign(ctx, reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			" err: ", err)
		return err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
		"response: ", string(respBodyBuf))

	resp := new(JCSBaseResponse)
	err = json.Unmarshal(respBodyBuf, resp)
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
		"JCSClient:UploadFile finish.")
	return nil
}

func (o *JCSClient) NewMultiPartUpload(
	ctx context.Context,
	packageId int32,
	path string) (
	resp *JCSNewMultiPartUploadResponse, err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:NewMultiPartUpload start.",
		" packageId: ", packageId,
		" path: ", path)

	input := new(JCSNewMultiPartUploadReq)
	input.UserID = o.userID
	input.PackageID = packageId
	input.Path = path

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return resp, err
	}

	url := o.endPoint + JCSNewMultipartUploadInterface

	Logger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage request.",
		" url: ", url,
		" reqBody: ", string(reqBody))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		strings.NewReader(string(reqBody)))
	if err != nil {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return resp, err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			" err: ", err)
		return resp, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return resp, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			" err: ", err)
		return resp, err
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
		return resp, err
	}

	Logger.WithContext(ctx).Debug(
		"response: ", string(respBodyBuf))

	resp = new(JCSNewMultiPartUploadResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		Logger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			" err: ", err)
		return resp, err
	}

	Logger.WithContext(ctx).Debug(
		"JCSClient:NewMultiPartUpload finish.")
	return resp, err
}

func (o *JCSClient) UploadPart(
	ctx context.Context,
	objectID,
	index int32,
	path string,
	data io.Reader) (err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:UploadPart start.",
		" objectID: ", objectID,
		" index: ", index,
		" path: ", path)

	jCSUploadPartReqInfo := new(JCSUploadPartReqInfo)
	jCSUploadPartReqInfo.UserID = o.userID
	jCSUploadPartReqInfo.ObjectID = objectID
	jCSUploadPartReqInfo.Index = index

	info, err := json.Marshal(jCSUploadPartReqInfo)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err
	}

	url := o.endPoint + JCSUploadPartInterface

	Logger.WithContext(ctx).Debug(
		"JCSClient:UploadPart request.",
		" url: ", url)

	/*
		pr, pw := io.Pipe()
		writer := multipart.NewWriter(pw)
		go func() {
			defer func() {
				errMsg := pw.Close()
				if errMsg != nil {
					Logger.WithContext(ctx).Warn(
						"close io.Pipe pw failed.",
						" err: ", errMsg)
				}
				errMsg = writer.Close()
				if errMsg != nil {
					Logger.WithContext(ctx).Warn(
						"close multipart.Writer failed.",
						" err: ", errMsg)
				}
			}()

			_ = writer.WriteField(
				JCSMultiPartFormFiledInfo,
				string(info))

			part, err := writer.CreateFormFile(
				JCSMultiPartFormFiledFile,
				path)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"writer.CreateFormFile failed.",
					" err: ", err)
				_ = pw.CloseWithError(err)
				return
			}
			_, err = io.Copy(part, data)
			if nil != err {
				Logger.WithContext(ctx).Error(
					"io.Copy failed.",
					" err: ", err)
				_ = pw.CloseWithError(err)
				return
			}
		}()

		header := make(http.Header)
		header.Add(HttpHeaderContentType, writer.FormDataContentType())

		reqHttp, err := http.NewRequest(
			http.MethodPost,
			url,
			pr)
		if nil != err {
			Logger.WithContext(ctx).Error(
				"http.NewRequest failed.",
				" err: ", err)
			return err
		}
		reqHttp.Header = header
	*/
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		JCSMultiPartFormFiledFile,
		path)
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
		JCSMultiPartFormFiledInfo,
		string(info))

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
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	err = o.sign(ctx, reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			" err: ", err)
		return err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
		"response: ", string(respBodyBuf))

	resp := new(JCSBaseResponse)
	err = json.Unmarshal(respBodyBuf, resp)
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
		"JCSClient:UploadPart finish.")
	return err
}

func (o *JCSClient) CompleteMultiPartUpload(
	ctx context.Context,
	objectID int32,
	indexes []int32) (
	err error) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:CompleteMultiPartUpload start.",
		" objectID: ", objectID)

	input := new(JCSCompleteMultiPartUploadReq)
	input.UserID = o.userID
	input.ObjectID = objectID
	input.Indexes = indexes

	reqBody, err := json.Marshal(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err
	}

	url := o.endPoint + JCSCompleteMultipartUploadInterface

	Logger.WithContext(ctx).Debug(
		"JCSClient:CompleteMultiPartUpload request.",
		" url: ", url,
		" reqBody: ", string(reqBody))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		strings.NewReader(string(reqBody)))
	if err != nil {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			" err: ", err)
		return err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
		"response: ", string(respBodyBuf))

	resp := new(JCSCompleteMultiPartUploadResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
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
		"JCSClient:CompleteMultiPartUpload finish.")
	return err
}

func (o *JCSClient) DownloadPart(
	ctx context.Context,
	objectID int32,
	offset,
	length int64) (err error, output *JCSDownloadPartOutput) {

	Logger.WithContext(ctx).Debug(
		"JCSClient:DownloadPart start.",
		" objectID: ", objectID,
		" offset: ", offset,
		" length: ", length)

	input := new(JCSDownloadReq)
	input.UserID = o.userID
	input.ObjectID = objectID
	input.Offset = offset
	input.Length = length

	values, err := query.Values(input)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, output
	}

	url := o.endPoint +
		JCSDownloadInterface +
		"?" + values.Encode()

	Logger.WithContext(ctx).Debug(
		"JCSClient:DownloadPart request.",
		" url: ", url)

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		url,
		nil)
	if err != nil {
		Logger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			" err: ", err)
		return err, output
	}

	reqHttp.Header = header

	err = o.sign(ctx, reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			" err: ", err)
		return err, output
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			" err: ", err)
		return err, output
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
		"JCSClient:DownloadPart finish.")
	return err, output
}
