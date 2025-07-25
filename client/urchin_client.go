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
	"go.uber.org/zap"
	"net"
	"net/http"
	"time"
)

var UClient UrchinClient

type UrchinClient struct {
	addr         string
	header       http.Header
	urchinClient *retryablehttp.Client
}

func (u *UrchinClient) Init(
	ctx context.Context,
	userId string,
	token string,
	address string,
	reqTimeout int64,
	maxConnection int) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:Init start.",
		zap.String("userId", userId),
		zap.String("token", "***"),
		zap.String("address", address),
		zap.Int64("reqTimeout", reqTimeout),
		zap.Int("maxConnection", maxConnection))

	u.addr = address

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

	u.urchinClient = retryablehttp.NewClient()
	u.urchinClient.RetryMax = 3
	u.urchinClient.RetryWaitMin = 1 * time.Second
	u.urchinClient.RetryWaitMax = 5 * time.Second
	u.urchinClient.HTTPClient.Transport = transport
	u.urchinClient.HTTPClient.Timeout = timeout

	requestId := ctx.Value("X-Request-Id").(string)
	u.header = make(http.Header)
	u.header.Add(UrchinClientHeaderUserId, userId)
	u.header.Add(UrchinClientHeaderToken, token)
	u.header.Add(UrchinClientHeaderRequestId, requestId)

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:Init finish.")
}

func (u *UrchinClient) CreateInitiateMultipartUploadSignedUrl(
	ctx context.Context,
	req *CreateInitiateMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateInitiateMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateInitiateMultipartUploadSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateInitiateMultipartUploadSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateUploadPartSignedUrl(
	ctx context.Context,
	req *CreateUploadPartSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateUploadPartSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateUploadPartSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateUploadPartSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateListPartsSignedUrl(
	ctx context.Context,
	req *CreateListPartsSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateListPartsSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateListPartsSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateListPartsSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateCompleteMultipartUploadSignedUrl(
	ctx context.Context,
	req *CreateCompleteMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateCompleteMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateCompleteMultipartUploadSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateCompleteMultipartUploadSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateAbortMultipartUploadSignedUrl(
	ctx context.Context,
	req *CreateAbortMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateAbortMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateAbortMultipartUploadSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateAbortMultipartUploadSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreatePutObjectSignedUrl(
	ctx context.Context,
	req *CreatePutObjectSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreatePutObjectSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreatePutObjectSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreatePutObjectSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateGetObjectMetadataSignedUrl(
	ctx context.Context,
	req *CreateGetObjectMetadataSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateGetObjectMetadataSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateGetObjectMetadataSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateGetObjectMetadataSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateGetObjectSignedUrl(
	ctx context.Context,
	req *CreateGetObjectSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateGetObjectSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateGetObjectSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateGetObjectSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateListObjectsSignedUrl(
	ctx context.Context,
	req *CreateListObjectsSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateListObjectsSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateListObjectsSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateListObjectsSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) GetIpfsToken(
	ctx context.Context,
	req *GetIpfsTokenReq) (
	err error, resp *GetIpfsTokenResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:GetIpfsToken start.")

	values, err := query.Values(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", values.Encode()))

	resp = new(GetIpfsTokenResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientGetIpfsTokenInterface+"?"+values.Encode(),
		http.MethodGet,
		u.header,
		nil,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:GetIpfsToken finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectUpload(
	ctx context.Context,
	req *CreateJCSPreSignedObjectUploadReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectUpload" +
			" start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectUploadInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectUpload" +
			" finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectNewMultipartUpload(
	ctx context.Context,
	req *CreateJCSPreSignedObjectNewMultipartUploadReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectNewMultipartUpload" +
			" start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectNewMultipartUploadInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectNewMultipartUpload" +
			" finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectUploadPart(
	ctx context.Context,
	req *CreateJCSPreSignedObjectUploadPartReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectUploadPart start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectUploadPartInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectUploadPart finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectCompleteMultipartUpload(
	ctx context.Context,
	req *CreateJCSPreSignedObjectCompleteMultipartUploadReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectCompleteMultipartUpload" +
			" start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectCompleteMultipartUploadInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectCompleteMultipartUpload" +
			" finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectDownload(
	ctx context.Context,
	req *CreateJCSPreSignedObjectDownloadReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectDownload start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectDownloadInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectDownload finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectList(
	ctx context.Context,
	req *CreateJCSPreSignedObjectListReq) (
	err error, resp *CreateSignedUrlResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectList start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectListInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectList finish.")
	return nil, resp
}

func (u *UrchinClient) UploadObject(
	ctx context.Context,
	req *UploadObjectReq) (
	err error, resp *UploadObjectResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:UploadObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(UploadObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientUploadObjectInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:UploadObject finish.")
	return nil, resp
}

func (u *UrchinClient) UploadFile(
	ctx context.Context,
	req *UploadFileReq) (
	err error, resp *UploadFileResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:UploadFile start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(UploadFileResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientUploadFileInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:UploadFile finish.")
	return nil, resp
}

func (u *UrchinClient) DownloadObject(
	ctx context.Context,
	req *DownloadObjectReq) (
	err error, resp *DownloadObjectResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DownloadObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(DownloadObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientDownloadObjectInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DownloadObject finish.")
	return nil, resp
}

func (u *UrchinClient) DownloadFile(
	ctx context.Context,
	req *DownloadFileReq) (
	err error, resp *DownloadFileResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DownloadFile start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(DownloadFileResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientDownloadFileInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DownloadFile finish.")
	return nil, resp
}

func (u *UrchinClient) LoadObject(
	ctx context.Context,
	req *LoadObjectReq) (
	err error, resp *LoadObjectResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:LoadObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(LoadObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientLoadObjectInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:LoadObject finish.")
	return nil, resp
}

func (u *UrchinClient) MigrateObject(
	ctx context.Context,
	req *MigrateObjectReq) (
	err error, resp *MigrateObjectResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:MigrateObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(MigrateObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientMigrateObjectInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:MigrateObject finish.")
	return nil, resp
}

func (u *UrchinClient) CopyObject(
	ctx context.Context,
	req *CopyObjectReq) (
	err error, resp *CopyObjectResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CopyObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CopyObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCopyObjectInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CopyObject finish.")
	return nil, resp
}

func (u *UrchinClient) GetObject(
	ctx context.Context,
	req *GetObjectReq) (
	err error, resp *GetObjectResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:GetObject start.")

	values, err := query.Values(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", values.Encode()))

	resp = new(GetObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientGetObjectInterface+"?"+values.Encode(),
		http.MethodGet,
		u.header,
		nil,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:GetObject finish.")
	return nil, resp
}

func (u *UrchinClient) DeleteObject(
	ctx context.Context,
	req *DeleteObjectReq) (
	err error, resp *BaseResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DeleteObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientDeleteObjectInterface,
		http.MethodDelete,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DeleteObject finish.")
	return nil, resp
}

func (u *UrchinClient) DeleteObjectDeployment(
	ctx context.Context,
	req *DeleteObjectDeploymentReq) (
	err error, resp *BaseResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DeleteObjectDeployment start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientDeleteObjectDeploymentInterface,
		http.MethodDelete,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DeleteObjectDeployment finish.")
	return nil, resp
}

func (u *UrchinClient) DeleteFile(
	ctx context.Context,
	req *DeleteFileReq) (
	err error, resp *BaseResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DeleteFile start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientDeleteFileInterface,
		http.MethodDelete,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:DeleteFile finish.")
	return nil, resp
}

func (u *UrchinClient) CreateObject(
	ctx context.Context,
	req *CreateObjectReq) (
	err error, resp *CreateObjectResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(CreateObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateObjectInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:CreateObject finish.")
	return nil, resp
}

func (u *UrchinClient) PutObjectDeployment(
	ctx context.Context,
	req *PutObjectDeploymentReq) (
	err error, resp *BaseResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:PutObjectDeployment start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientPutObjectDeploymentInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:PutObjectDeployment finish.")
	return nil, resp
}

func (u *UrchinClient) ListObjects(
	ctx context.Context,
	req *ListObjectsReq) (
	err error, resp *ListObjectsResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:ListObjects start.")

	values, err := query.Values(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", values.Encode()))

	resp = new(ListObjectsResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientListObjectsInterface+"?"+values.Encode(),
		http.MethodGet,
		u.header,
		nil,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:ListObjects finish.")
	return nil, resp
}

func (u *UrchinClient) ListParts(
	ctx context.Context,
	req *ListPartsReq) (
	err error, resp *ListPartsResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:ListParts start.")

	values, err := query.Values(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", values.Encode()))

	resp = new(ListPartsResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientListPartsInterface+"?"+values.Encode(),
		http.MethodGet,
		u.header,
		nil,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:ListParts finish.")
	return nil, resp
}

func (u *UrchinClient) GetTask(
	ctx context.Context,
	req *GetTaskReq) (
	err error, resp *GetTaskResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:GetTask start.")

	values, err := query.Values(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"query.Values failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", values.Encode()))

	resp = new(GetTaskResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientGetTaskInterface+"?"+values.Encode(),
		http.MethodGet,
		u.header,
		nil,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:GetTask finish.")
	return nil, resp
}

func (u *UrchinClient) FinishTask(
	ctx context.Context,
	req *FinishTaskReq) (
	err error, resp *BaseResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:FinishTask start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientFinishTaskInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:FinishTask finish.")
	return nil, resp
}

func (u *UrchinClient) RetryTask(
	ctx context.Context,
	req *RetryTaskReq) (
	err error, resp *BaseResp) {

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:RetryTask start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Marshal failed.",
			zap.Error(err))
		return err, resp
	}
	InfoLogger.WithContext(ctx).Debug(
		"request info.",
		zap.String("request", string(reqBody)))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientRetryTaskInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"http.Do failed.",
			zap.Error(err))
		return err, resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"response info.",
		zap.String("respBody", string(respBody)))

	err = json.Unmarshal(respBody, resp)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"json.Unmarshal failed.",
			zap.Error(err))
		return err, resp
	}

	if SuccessCode != resp.Code {
		ErrorLogger.WithContext(ctx).Error(
			"response failed.",
			zap.Int32("errCode", resp.Code),
			zap.String("errMessage", resp.Message),
			zap.String("requestId", resp.RequestId))
		return errors.New(resp.Message), resp
	}

	InfoLogger.WithContext(ctx).Debug(
		"UrchinClient:RetryTask finish.")
	return nil, resp
}
