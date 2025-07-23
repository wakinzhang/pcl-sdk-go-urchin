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
	"go.uber.org/zap"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
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

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:Init start.",
		zap.Int("reqTimeout", reqTimeout),
		zap.Int("maxConnection", maxConnection))

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

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:Init finish.")
}

func (o *JCSProxyClient) ListWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error, resp *JCSListResponse) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:ListWithSignedUrl start.",
		zap.String("signedUrl", signedUrl))

	resp = new(JCSListResponse)

	request, err := retryablehttp.NewRequest(
		http.MethodGet,
		signedUrl,
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			zap.Error(err))
		return err, resp
	}

	response, err := o.jcsClient.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Do failed.",
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

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:ListWithSignedUrl response.",
		zap.String("signedUrl", signedUrl),
		zap.String("response", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxyClient:ListWithSignedUrl response failed.",
			zap.String("signedUrl", signedUrl),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:ListWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSProxyClient) UploadFileWithSignedUrl(
	ctx context.Context,
	signedUrl string,
	data io.Reader) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:UploadFileWithSignedUrl start.",
		zap.String("signedUrl", signedUrl))

	request, err := retryablehttp.NewRequest(
		http.MethodPost,
		signedUrl,
		data)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			zap.Error(err))
		return err
	}

	request.Header.Set(HttpHeaderContentType, HttpHeaderContentTypeStream)

	response, err := o.jcsClient.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Do failed.",
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

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:UploadFileWithSignedUrl response.",
		zap.String("signedUrl", signedUrl),
		zap.String("response", string(respBody)))

	resp := new(JCSBaseResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxyClient:UploadFileWithSignedUrl response failed.",
			zap.String("signedUrl", signedUrl),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message)
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:UploadFileWithSignedUrl finish.")
	return nil
}

func (o *JCSProxyClient) NewMultiPartUploadWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error,
	resp *JCSNewMultiPartUploadResponse) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:NewMultiPartUploadWithSignedUrl start.",
		zap.String("signedUrl", signedUrl))

	resp = new(JCSNewMultiPartUploadResponse)

	request, err := retryablehttp.NewRequest(
		http.MethodPost,
		signedUrl,
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			zap.Error(err))
		return err, resp
	}

	response, err := o.jcsClient.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Do failed.",
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

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:NewMultiPartUploadWithSignedUrl response.",
		zap.String("signedUrl", signedUrl),
		zap.String("response", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxyClient:NewMultiPartUploadWithSignedUrl response"+
				" failed.",
			zap.String("signedUrl", signedUrl),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:NewMultiPartUploadWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSProxyClient) UploadPartWithSignedUrl(
	ctx context.Context,
	signedUrl string,
	data io.Reader) (err error, resp *JCSBaseResponse) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:UploadPartWithSignedUrl start.",
		zap.String("signedUrl", signedUrl))

	resp = new(JCSBaseResponse)

	request, err := retryablehttp.NewRequest(
		http.MethodPost,
		signedUrl,
		data)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			zap.Error(err))
		return err, resp
	}

	request.Header.Set(HttpHeaderContentType, HttpHeaderContentTypeStream)

	response, err := o.jcsClient.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Do failed.",
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

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:UploadPartWithSignedUrl response.",
		zap.String("signedUrl", signedUrl),
		zap.String("response", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxyClient:UploadPartWithSignedUrl response failed.",
			zap.String("signedUrl", signedUrl),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:UploadPartWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSProxyClient) CompleteMultiPartUploadWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error,
	resp *JCSCompleteMultiPartUploadResponse) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:CompleteMultiPartUploadWithSignedUrl start.",
		zap.String("signedUrl", signedUrl))

	resp = new(JCSCompleteMultiPartUploadResponse)

	request, err := retryablehttp.NewRequest(
		http.MethodPost,
		signedUrl,
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			zap.Error(err))
		return err, resp
	}

	response, err := o.jcsClient.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Do failed.",
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

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:CompleteMultiPartUploadWithSignedUrl"+
			" response.",
		zap.String("signedUrl", signedUrl),
		zap.String("response", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSProxyClient:CompleteMultiPartUploadWithSignedUrl"+
				" response failed.",
			zap.String("signedUrl", signedUrl),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:CompleteMultiPartUploadWithSignedUrl finish.")
	return nil, resp
}

func (o *JCSProxyClient) DownloadPartWithSignedUrl(
	ctx context.Context,
	signedUrl string) (err error, output *JCSDownloadPartOutput) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSProxyClient:DownloadPartWithSignedUrl start.",
		zap.String("signedUrl", signedUrl))

	output = new(JCSDownloadPartOutput)

	request, err := retryablehttp.NewRequest(
		http.MethodGet,
		signedUrl,
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.NewRequest failed.",
			zap.Error(err))
		return err, output
	}

	response, err := o.jcsClient.Do(request)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"client.Do failed.",
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
			"JCSProxyClient:DownloadPartWithSignedUrl response.",
			zap.String("signedUrl", signedUrl),
			zap.String("response", string(respBodyBuf)))

		var resp = new(JCSBaseResponse)
		err = json.Unmarshal(respBodyBuf, resp)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"json.Unmarshal failed.",
				zap.Error(err))
			return err, output
		}

		ErrorLogger.WithContext(ctx).Error(
			"JCSProxyClient:DownloadPartWithSignedUrl response failed.",
			zap.String("signedUrl", signedUrl),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))

		return errors.New(resp.Message), output
	}

	output = new(JCSDownloadPartOutput)
	output.Body = response.Body

	InfoLogger.WithContext(ctx).Debug(
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
	bucketName  string
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
	bucketID int32,
	bucketName string,
	reqTimeout,
	maxConnection int32) {

	InfoLogger.WithContext(ctx).Debug(
		"Function JCSClient:Init start.",
		zap.String("endPoint", endPoint),
		zap.String("authService", authService),
		zap.String("authRegion", authRegion),
		zap.Int32("userId", userID),
		zap.Int32("bucketID", bucketID),
		zap.String("bucketName", bucketName),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))

	o.accessKey = accessKey
	o.secretKey = secretKey
	o.endPoint = endPoint
	o.authService = authService
	o.authRegion = authRegion
	o.userID = userID
	o.bucketID = bucketID
	o.bucketName = bucketName

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

	InfoLogger.WithContext(ctx).Debug(
		"Function JCS:Init finish.")
}

func (o *JCSClient) sign(
	ctx context.Context,
	req *http.Request) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Function JCSClient:sign start.")

	prod := credentials.NewStaticCredentialsProvider(
		o.accessKey,
		o.secretKey,
		"")

	cred, err := prod.Retrieve(context.TODO())
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"CredentialsProvider:Retrieve failed.",
			zap.Error(err))
		return err
	}

	payloadHash := ""
	if req.Body != nil {
		data, err := io.ReadAll(req.Body)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"http.Request.Body ReadAll failed.",
				zap.Error(err))
			return err
		}
		_err := req.Body.Close()
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"http.Request.Body.Close failed.",
				zap.Error(_err))
		}

		req.Body = io.NopCloser(bytes.NewReader(data))

		hash := sha256.New()
		hash.Write(data)
		payloadHash = hex.EncodeToString(hash.Sum(nil))
	} else {
		hash := sha256.Sum256([]byte(""))
		payloadHash = hex.EncodeToString(hash[:])
	}

	InfoLogger.WithContext(ctx).Debug(
		"Signer:SignHTTP params.",
		zap.String("payloadHash", payloadHash))
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
		ErrorLogger.WithContext(ctx).Error(
			"Signer.SignHTTP failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Function JCSClient:sign finish.")
	return nil
}

func (o *JCSClient) signWithoutBody(
	ctx context.Context,
	req *http.Request) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Function JCSClient:signWithoutBody start.")

	prod := credentials.NewStaticCredentialsProvider(
		o.accessKey,
		o.secretKey,
		"")

	cred, err := prod.Retrieve(context.TODO())
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"CredentialsProvider:Retrieve failed.",
			zap.Error(err))
		return err
	}

	signer := signerV4.NewSigner()
	err = signer.SignHTTP(
		context.Background(),
		cred,
		req,
		"",
		o.authService,
		o.authRegion,
		time.Now())
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"Signer.SignHTTP failed.",
			zap.Error(err))
		return err
	}

	InfoLogger.WithContext(ctx).Debug(
		"Function JCSClient:signWithoutBody finish.")
	return nil
}

func (o *JCSClient) preSign(
	ctx context.Context,
	req *http.Request,
	expiration int) (signedUrl string, err error) {

	InfoLogger.WithContext(ctx).Debug(
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
		ErrorLogger.WithContext(ctx).Error(
			"CredentialsProvider:Retrieve failed.",
			zap.Error(err))
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
		ErrorLogger.WithContext(ctx).Error(
			"signerV4:PreSignHTTP failed.",
			zap.Error(err))
		return signedUrl, err
	}
	InfoLogger.WithContext(ctx).Debug(
		"Function JCSClient:preSign finish.")

	return signedUrl, err
}

func (o *JCSClient) CreateBucket(
	ctx context.Context,
	bucketName string) (
	resp *JCSCreateBucketResponse, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreateBucket start.",
		zap.String("bucketName", bucketName))

	input := new(JCSCreateBucketReq)
	input.UserID = o.userID
	input.Name = bucketName

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return resp, err
	}

	reqUrl := o.endPoint + JCSCreateBucketInterface

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreateBucket request.",
		zap.String("reqUrl", reqUrl),
		zap.String("reqBody", string(reqBody)))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(string(reqBody)))
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return resp, err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			zap.Error(err))
		return resp, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return resp, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			zap.Error(err))
		return resp, err
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
		return resp, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreateBucket response.",
		zap.String("bucketName", bucketName),
		zap.String("response", string(respBodyBuf)))

	resp = new(JCSCreateBucketResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return resp, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreateBucket finish.")
	return resp, err
}

func (o *JCSClient) CreatePackage(
	ctx context.Context,
	packageName string) (
	resp *JCSCreatePackageResponse, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage start.",
		zap.String("packageName", packageName))

	input := new(JCSCreatePackageReq)
	input.UserID = o.userID
	input.BucketID = o.bucketID
	input.Name = packageName

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return resp, err
	}

	reqUrl := o.endPoint + JCSCreatePackageInterface

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage request.",
		zap.String("reqUrl", reqUrl),
		zap.String("reqBody", string(reqBody)))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(string(reqBody)))
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return resp, err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			zap.Error(err))
		return resp, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return resp, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			zap.Error(err))
		return resp, err
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
		return resp, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage response.",
		zap.String("packageName", packageName),
		zap.String("response", string(respBodyBuf)))

	resp = new(JCSCreatePackageResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return resp, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage finish.")
	return resp, err
}

func (o *JCSClient) GetPackage(
	ctx context.Context,
	packageName string) (
	resp *JCSGetPackageResponse, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:GetPackage start.",
		zap.String("packageName", packageName))

	input := new(JCSGetPackageReq)
	input.UserID = o.userID
	input.BucketName = o.bucketName
	input.PackageName = packageName

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return resp, err
	}

	reqUrl := o.endPoint + JCSGetPackageInterface + "?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:GetPackage request.",
		zap.String("reqUrl", reqUrl))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	reqHttp, err := http.NewRequest(
		http.MethodGet,
		reqUrl,
		nil)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return resp, err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			zap.Error(err))
		return resp, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return resp, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			zap.Error(err))
		return resp, err
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
		return resp, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:GetPackage response.",
		zap.String("packageName", packageName),
		zap.String("response", string(respBodyBuf)))

	resp = new(JCSGetPackageResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return resp, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:GetPackage finish.")
	return resp, err
}

func (o *JCSClient) DeletePackage(
	ctx context.Context,
	packageId int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:DeletePackage start.",
		zap.Int32("packageId", packageId))

	input := new(JCSDeletePackageReq)
	input.UserID = o.userID
	input.PackageID = packageId

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err
	}

	reqUrl := o.endPoint + JCSDeletePackageInterface

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:DeletePackage request.",
		zap.String("reqUrl", reqUrl),
		zap.String("reqBody", string(reqBody)))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(string(reqBody)))
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			zap.Error(err))
		return err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
		"JCSClient:DeletePackage response.",
		zap.Int32("packageId", packageId),
		zap.String("response", string(respBodyBuf)))

	resp := new(JCSBaseResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient:DeletePackage response failed.",
			zap.Int32("packageId", packageId),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message)
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:DeletePackage finish.")
	return err
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

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectListSignedUrl start.",
		zap.Int32("packageID", packageID),
		zap.String("path", path),
		zap.Bool("isPrefix", isPrefix),
		zap.Bool("noRecursive", noRecursive),
		zap.Int32("maxKeys", maxKeys),
		zap.String("continuationToken", continuationToken),
		zap.Int("expires", expires))

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
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return signedUrl, err
	}

	reqUrl := o.endPoint +
		JCSPreSignedObjectListInterface +
		"?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectListSignedUrl request.",
		zap.String("reqUrl", reqUrl))

	req, err := http.NewRequest(
		http.MethodGet,
		reqUrl,
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			zap.Error(err))
		return signedUrl, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectListSignedUrl finish.",
		zap.String("signedUrl", signedUrl))
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectUploadSignedUrl(
	ctx context.Context,
	packageID int32,
	path string,
	expires int) (
	signedUrl string, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadSignedUrl start.",
		zap.Int32("packageID", packageID),
		zap.String("path", path),
		zap.Int("expires", expires))

	input := JCSCreatePreSignedObjectUploadSignedUrlReq{}
	input.UserID = o.userID
	input.PackageID = packageID
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return signedUrl, err
	}

	reqUrl := o.endPoint +
		JCSPreSignedObjectUploadInterface +
		"?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadSignedUrl request.",
		zap.String("reqUrl", reqUrl))

	req, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(""))
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			zap.Error(err))
		return signedUrl, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadSignedUrl finish.",
		zap.String("signedUrl", signedUrl))
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectNewMultipartUploadSignedUrl(
	ctx context.Context,
	packageID int32,
	path string,
	expires int) (
	signedUrl string, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectNewMultipartUploadSignedUrl"+
			" start.",
		zap.Int32("packageID", packageID),
		zap.String("path", path),
		zap.Int("expires", expires))

	input := JCSNewMultiPartUploadReq{}
	input.UserID = o.userID
	input.PackageID = packageID
	input.Path = path

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return signedUrl, err
	}

	reqUrl := o.endPoint +
		JCSPreSignedObjectNewMultipartUploadInterface +
		"?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectNewMultipartUploadSignedUrl"+
			" request.",
		zap.String("reqUrl", reqUrl))

	req, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(""))
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			zap.Error(err))
		return signedUrl, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectNewMultipartUploadSignedUrl"+
			" finish.",
		zap.String("signedUrl", signedUrl))
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectUploadPartSignedUrl(
	ctx context.Context,
	objectID, index int32,
	expires int) (
	signedUrl string, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadPartSignedUrl start.",
		zap.Int32("objectID", objectID),
		zap.Int32("index", index),
		zap.Int("expires", expires))

	input := JCSUploadPartReqInfo{}

	input.UserID = o.userID
	input.ObjectID = objectID
	input.Index = index

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return signedUrl, err
	}

	reqUrl := o.endPoint +
		JCSPreSignedObjectUploadPartInterface +
		"?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadPartSignedUrl request.",
		zap.String("reqUrl", reqUrl))

	req, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(""))
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			zap.Error(err))
		return signedUrl, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectUploadPartSignedUrl finish.",
		zap.String("signedUrl", signedUrl))
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectCompleteMultipartUploadSignedUrl(
	ctx context.Context,
	objectID int32,
	indexes []int32,
	expires int) (
	signedUrl string, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:"+
			"CreatePreSignedObjectCompleteMultipartUploadSignedUrl start.",
		zap.Int32("objectID", objectID),
		zap.Int("expires", expires))

	input := JCSCompleteMultiPartUploadReq{}
	input.UserID = o.userID
	input.ObjectID = objectID
	input.Indexes = indexes

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return signedUrl, err
	}

	reqUrl := o.endPoint +
		JCSPreSignedObjectCompleteMultipartUploadInterface +
		"?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:"+
			"CreatePreSignedObjectCompleteMultipartUploadSignedUrl request.",
		zap.String("reqUrl", reqUrl))

	req, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(""))
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			zap.Error(err))
		return signedUrl, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:"+
			"CreatePreSignedObjectCompleteMultipartUploadSignedUrl finish.",
		zap.String("signedUrl", signedUrl))
	return signedUrl, err
}

func (o *JCSClient) CreatePreSignedObjectDownloadSignedUrl(
	ctx context.Context,
	objectID int32,
	offset,
	length int64,
	expires int) (
	signedUrl string, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectDownloadSignedUrl start.",
		zap.Int32("objectID", objectID),
		zap.Int64("offset", offset),
		zap.Int64("length", length),
		zap.Int("expires", expires))

	input := JCSDownloadReq{}
	input.UserID = o.userID
	input.ObjectID = objectID
	input.Offset = offset
	input.Length = length

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return signedUrl, err
	}

	reqUrl := o.endPoint +
		JCSPreSignedObjectDownloadInterface +
		"?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectDownloadSignedUrl request.",
		zap.String("reqUrl", reqUrl))

	req, err := http.NewRequest(
		http.MethodGet,
		reqUrl,
		nil)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return signedUrl, err
	}

	signedUrl, err = o.preSign(ctx, req, expires)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.PreSign failed.",
			zap.Error(err))
		return signedUrl, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePreSignedObjectDownloadSignedUrl finish.",
		zap.String("signedUrl", signedUrl))
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

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:List start.",
		zap.Int32("packageID", packageID),
		zap.String("path", path),
		zap.Bool("isPrefix", isPrefix),
		zap.Bool("noRecursive", noRecursive),
		zap.Int32("maxKeys", maxKeys),
		zap.String("continuationToken", continuationToken))

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
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return listObjectsData, err
	}

	reqUrl := o.endPoint + JCSListInterface + "?" + values.Encode()
	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:List request.",
		zap.String("reqUrl", reqUrl))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	reqHttp, err := http.NewRequest(
		http.MethodGet,
		reqUrl,
		nil)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return listObjectsData, err
	}

	reqHttp.Header = header

	err = o.sign(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			zap.Error(err))
		return listObjectsData, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return listObjectsData, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			zap.Error(err))
		return listObjectsData, err
	}

	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"io.ReadCloser failed.",
				zap.Error(_err))
		}
	}(response.Body)

	respBody, err := io.ReadAll(response.Body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"io.ReadAll failed.",
			zap.Error(err))
		return listObjectsData, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:List response.",
		zap.Int32("packageID", packageID),
		zap.String("path", path),
		zap.Bool("isPrefix", isPrefix),
		zap.Bool("noRecursive", noRecursive),
		zap.Int32("maxKeys", maxKeys),
		zap.String("continuationToken", continuationToken),
		zap.String("response", string(respBody)))

	resp := new(JCSListResponse)
	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return listObjectsData, err
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient:List response failed.",
			zap.Int32("packageID", packageID),
			zap.String("path", path),
			zap.Bool("isPrefix", isPrefix),
			zap.Bool("noRecursive", noRecursive),
			zap.Int32("maxKeys", maxKeys),
			zap.String("continuationToken", continuationToken),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return listObjectsData, errors.New(resp.Message)
	}

	listObjectsData = new(JCSListData)
	listObjectsData = resp.Data

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:List finish.")
	return listObjectsData, err
}

func (o *JCSClient) UploadFile(
	ctx context.Context,
	packageId int32,
	path string,
	data io.Reader) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:UploadFile start.",
		zap.Int32("packageId", packageId),
		zap.String("path", path))

	jCSUploadReqInfo := new(JCSUploadReqInfo)
	jCSUploadReqInfo.UserID = o.userID
	jCSUploadReqInfo.PackageID = packageId

	info, err := json.Marshal(jCSUploadReqInfo)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err
	}

	reqUrl := o.endPoint + JCSUploadInterface

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:UploadFile request.",
		zap.String("reqUrl", reqUrl),
		zap.String("info", string(info)))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		JCSMultiPartFormFiledFiles,
		url.PathEscape(path))
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"writer.CreateFormFile failed.",
			zap.Error(err))
		return err
	}

	if nil != data {
		_, err = io.Copy(part, data)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"io.Copy failed.",
				zap.Error(err))
			return err
		}
	}
	_ = writer.WriteField(
		JCSMultiPartFormFiledInfo,
		string(info))

	err = writer.Close()
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"writer.Close failed.",
			zap.Error(err))
		return err
	}

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return err
	}
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	err = o.signWithoutBody(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.signWithoutBody failed.",
			zap.Error(err))
		return err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
		"JCSClient:UploadFile response.",
		zap.Int32("packageId", packageId),
		zap.String("path", path),
		zap.String("response", string(respBodyBuf)))

	resp := new(JCSBaseResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient:UploadFile response failed.",
			zap.Int32("packageId", packageId),
			zap.String("path", path),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message)
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:UploadFile finish.")
	return nil
}

func (o *JCSClient) NewMultiPartUpload(
	ctx context.Context,
	packageId int32,
	path string) (
	resp *JCSNewMultiPartUploadResponse, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:NewMultiPartUpload start.",
		zap.Int32("packageId", packageId),
		zap.String("path", path))

	input := new(JCSNewMultiPartUploadReq)
	input.UserID = o.userID
	input.PackageID = packageId
	input.Path = path

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return resp, err
	}

	reqUrl := o.endPoint + JCSNewMultipartUploadInterface

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CreatePackage request.",
		zap.String("reqUrl", reqUrl),
		zap.String("reqBody", string(reqBody)))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(string(reqBody)))
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return resp, err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			zap.Error(err))
		return resp, err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return resp, err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
			zap.Error(err))
		return resp, err
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
		return resp, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:NewMultiPartUpload response.",
		zap.Int32("packageId", packageId),
		zap.String("path", path),
		zap.String("response", string(respBodyBuf)))

	resp = new(JCSNewMultiPartUploadResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return resp, err
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:NewMultiPartUpload finish.")
	return resp, err
}

func (o *JCSClient) UploadPart(
	ctx context.Context,
	objectID,
	index int32,
	path string,
	data io.Reader) (err error, resp *JCSBaseResponse) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:UploadPart start.",
		zap.Int32("objectID", objectID),
		zap.Int32("index", index),
		zap.String("path", path))

	jCSUploadPartReqInfo := new(JCSUploadPartReqInfo)
	jCSUploadPartReqInfo.UserID = o.userID
	jCSUploadPartReqInfo.ObjectID = objectID
	jCSUploadPartReqInfo.Index = index

	info, err := json.Marshal(jCSUploadPartReqInfo)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}

	reqUrl := o.endPoint + JCSUploadPartInterface

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:UploadPart request.",
		zap.String("reqUrl", reqUrl),
		zap.String("info", string(info)))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		JCSMultiPartFormFiledFile,
		url.PathEscape(path))
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
		JCSMultiPartFormFiledInfo,
		string(info))

	err = writer.Close()
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"writer.Close failed.",
			zap.Error(err))
		return err, resp
	}

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		body)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return err, resp
	}
	reqHttp.Header.Set(HttpHeaderContentType, writer.FormDataContentType())

	err = o.signWithoutBody(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.signWithoutBody failed.",
			zap.Error(err))
		return err, resp
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err, resp
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
		"JCSClient:UploadPart response.",
		zap.Int32("objectID", objectID),
		zap.Int32("index", index),
		zap.String("path", path),
		zap.String("response", string(respBodyBuf)))

	resp = new(JCSBaseResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient:UploadPart response failed.",
			zap.Int32("objectID", objectID),
			zap.Int32("index", index),
			zap.String("path", path),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:UploadPart finish.")
	return nil, resp
}

func (o *JCSClient) CompleteMultiPartUpload(
	ctx context.Context,
	objectID int32,
	indexes []int32) (
	err error) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CompleteMultiPartUpload start.",
		zap.Int32("objectID", objectID))

	input := new(JCSCompleteMultiPartUploadReq)
	input.UserID = o.userID
	input.ObjectID = objectID
	input.Indexes = indexes

	reqBody, err := json.Marshal(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err
	}

	reqUrl := o.endPoint + JCSCompleteMultipartUploadInterface

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CompleteMultiPartUpload request.",
		zap.String("reqUrl", reqUrl),
		zap.String("reqBody", string(reqBody)))

	reqHttp, err := http.NewRequest(
		http.MethodPost,
		reqUrl,
		strings.NewReader(string(reqBody)))
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return err
	}

	err = o.sign(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			zap.Error(err))
		return err
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
		"JCSClient:CompleteMultiPartUpload response.",
		zap.Int32("objectID", objectID),
		zap.String("response", string(respBodyBuf)))

	resp := new(JCSCompleteMultiPartUploadResponse)
	err = json.Unmarshal(respBodyBuf, resp)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err
	}

	if JCSSuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient:CompleteMultiPartUpload response failed.",
			zap.Int32("objectID", objectID),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))
		return errors.New(resp.Message)
	}

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:CompleteMultiPartUpload finish.")
	return err
}

func (o *JCSClient) DownloadPart(
	ctx context.Context,
	objectID int32,
	offset,
	length int64) (err error, output *JCSDownloadPartOutput) {

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:DownloadPart start.",
		zap.Int32("objectID", objectID),
		zap.Int64("offset", offset),
		zap.Int64("length", length))

	input := new(JCSDownloadReq)
	input.UserID = o.userID
	input.ObjectID = objectID
	input.Offset = offset
	input.Length = length

	values, err := query.Values(input)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, output
	}

	reqUrl := o.endPoint + JCSDownloadInterface + "?" + values.Encode()

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:DownloadPart request.",
		zap.String("reqUrl", reqUrl))

	header := make(http.Header)
	header.Add(HttpHeaderContentType, HttpHeaderContentTypeJson)

	reqHttp, err := http.NewRequest(
		http.MethodGet,
		reqUrl,
		nil)
	if err != nil {
		ErrorLogger.WithContext(ctx).Error(
			"http.NewRequest failed.",
			zap.Error(err))
		return err, output
	}

	reqHttp.Header = header

	err = o.sign(ctx, reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"JCSClient.sign failed.",
			zap.Error(err))
		return err, output
	}

	reqRetryableHttp, err := retryablehttp.FromRequest(reqHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"retryablehttp.FromRequest failed.",
			zap.Error(err))
		return err, output
	}

	response, err := o.jcsClient.Do(reqRetryableHttp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"jcsClient.Do failed.",
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
			"JCSClient:DownloadPart response.",
			zap.Int32("objectID", objectID),
			zap.Int64("offset", offset),
			zap.Int64("length", length),
			zap.String("response", string(respBodyBuf)))

		var resp = new(JCSBaseResponse)
		err = json.Unmarshal(respBodyBuf, resp)
		if nil != err {
			ErrorLogger.WithContext(ctx).Error(
				"json.Unmarshal failed.",
				zap.Error(err))
			return err, output
		}

		ErrorLogger.WithContext(ctx).Error(
			"JCSClient:DownloadPart response failed.",
			zap.Int32("objectID", objectID),
			zap.Int64("offset", offset),
			zap.Int64("length", length),
			zap.String("errCode", resp.Code),
			zap.String("errMessage", resp.Message))

		return errors.New(resp.Message), output
	}

	output = new(JCSDownloadPartOutput)
	output.Body = response.Body

	InfoLogger.WithContext(ctx).Debug(
		"JCSClient:DownloadPart finish.")
	return err, output
}
