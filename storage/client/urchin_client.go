package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/google/go-querystring/query"
	"github.com/hashicorp/go-retryablehttp"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
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
	address string,
	reqTimeout int64,
	maxConnection int) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:Init start.",
		" address: ", address,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)

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

	userId := "test"
	token := "test"
	requestId := ctx.Value("X-Request-Id").(string)
	u.header = make(http.Header)
	u.header.Add(UrchinClientHeaderUserId, userId)
	u.header.Add(UrchinClientHeaderToken, token)
	u.header.Add(UrchinClientHeaderRequestId, requestId)

	Logger.WithContext(ctx).Debug(
		"UrchinClient:Init finish.")
}

func (u *UrchinClient) CreateInitiateMultipartUploadSignedUrl(
	ctx context.Context,
	req *CreateInitiateMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateInitiateMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateInitiateMultipartUploadSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateInitiateMultipartUploadSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateUploadPartSignedUrl(
	ctx context.Context,
	req *CreateUploadPartSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateUploadPartSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateUploadPartSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateUploadPartSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateCompleteMultipartUploadSignedUrl(
	ctx context.Context,
	req *CreateCompleteMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateCompleteMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateCompleteMultipartUploadSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateCompleteMultipartUploadSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateAbortMultipartUploadSignedUrl(
	ctx context.Context,
	req *CreateAbortMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateAbortMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateAbortMultipartUploadSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateAbortMultipartUploadSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreatePutObjectSignedUrl(
	ctx context.Context,
	req *CreatePutObjectSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreatePutObjectSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreatePutObjectSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreatePutObjectSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateGetObjectMetadataSignedUrl(
	ctx context.Context,
	req *CreateGetObjectMetadataSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateGetObjectMetadataSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateGetObjectMetadataSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateGetObjectMetadataSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateGetObjectSignedUrl(
	ctx context.Context,
	req *CreateGetObjectSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateGetObjectSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateGetObjectSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateGetObjectSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) CreateListObjectsSignedUrl(
	ctx context.Context,
	req *CreateListObjectsSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateListObjectsSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateListObjectsSignedUrlInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateListObjectsSignedUrl finish.")
	return nil, resp
}

func (u *UrchinClient) GetIpfsToken(
	ctx context.Context,
	req *GetIpfsTokenReq) (
	err error, resp *GetIpfsTokenResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:GetIpfsToken start.")

	values, err := query.Values(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", values.Encode())

	resp = new(GetIpfsTokenResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientGetIpfsTokenInterface+"?"+values.Encode(),
		http.MethodGet,
		u.header,
		nil,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:GetIpfsToken finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectList(
	ctx context.Context,
	req *CreateJCSPreSignedObjectListReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectList start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectListInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectList finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectUpload(
	ctx context.Context,
	req *CreateJCSPreSignedObjectUploadReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectUpload" +
			" start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectUploadInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectUpload" +
			" finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectNewMultipartUpload(
	ctx context.Context,
	req *CreateJCSPreSignedObjectNewMultipartUploadReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectNewMultipartUpload" +
			" start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectNewMultipartUploadInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectNewMultipartUpload" +
			" finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectUploadPart(
	ctx context.Context,
	req *CreateJCSPreSignedObjectUploadPartReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectUploadPart start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectUploadPartInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectUploadPart finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectCompleteMultipartUpload(
	ctx context.Context,
	req *CreateJCSPreSignedObjectCompleteMultipartUploadReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectCompleteMultipartUpload" +
			" start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}

	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectCompleteMultipartUploadInterface,
		http.MethodPost,
		u.header,
		reqBody, u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectCompleteMultipartUpload" +
			" finish.")
	return nil, resp
}

func (u *UrchinClient) CreateJCSPreSignedObjectDownload(
	ctx context.Context,
	req *CreateJCSPreSignedObjectDownloadReq) (
	err error, resp *CreateSignedUrlResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectDownload start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientCreateJCSPreSignedObjectDownloadInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:CreateJCSPreSignedObjectDownload finish.")
	return nil, resp
}

func (u *UrchinClient) UploadObject(
	ctx context.Context,
	req *UploadObjectReq) (
	err error, resp *UploadObjectResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:UploadObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(UploadObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientUploadObjectInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:UploadObject finish.")
	return nil, resp
}

func (u *UrchinClient) UploadFile(
	ctx context.Context,
	req *UploadFileReq) (
	err error, resp *UploadFileResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:UploadFile start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(UploadFileResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientUploadFileInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:UploadFile finish.")
	return nil, resp
}

func (u *UrchinClient) GetObject(
	ctx context.Context,
	req *GetObjectReq) (
	err error, resp *GetObjectResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:GetObject start.")

	values, err := query.Values(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", values.Encode())

	resp = new(GetObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientGetObjectInterface+"?"+values.Encode(),
		http.MethodGet,
		u.header,
		nil,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:GetObject finish.")
	return nil, resp
}

func (u *UrchinClient) DownloadObject(
	ctx context.Context,
	req *DownloadObjectReq) (
	err error, resp *DownloadObjectResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:DownloadObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(DownloadObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientDownloadObjectInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:DownloadObject finish.")
	return nil, resp
}

func (u *UrchinClient) DownloadFile(
	ctx context.Context,
	req *DownloadFileReq) (
	err error, resp *DownloadFileResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:DownloadFile start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(DownloadFileResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientDownloadFileInterface,
		http.MethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:DownloadFile finish.")
	return nil, resp
}

func (u *UrchinClient) MigrateObject(
	ctx context.Context,
	req *MigrateObjectReq) (
	err error, resp *MigrateObjectResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:MigrateObject start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(MigrateObjectResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientMigrateObjectInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:MigrateObject finish.")
	return nil, resp
}

func (u *UrchinClient) PutObjectDeployment(
	ctx context.Context,
	req *PutObjectDeploymentReq) (
	err error, resp *BaseResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:PutObjectDeployment start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientPutObjectDeploymentInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:PutObjectDeployment finish.")
	return nil, resp
}

func (u *UrchinClient) GetTask(
	ctx context.Context,
	req *GetTaskReq) (
	err error, resp *GetTaskResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:GetTask start.")

	values, err := query.Values(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"query.Values failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", values.Encode())

	resp = new(GetTaskResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientGetTaskInterface+"?"+values.Encode(),
		http.MethodGet,
		u.header,
		nil,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:GetTask finish.")
	return nil, resp
}

func (u *UrchinClient) FinishTask(
	ctx context.Context,
	req *FinishTaskReq) (
	err error, resp *BaseResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:FinishTask start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientFinishTaskInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:FinishTask finish.")
	return nil, resp
}

func (u *UrchinClient) RetryTask(
	ctx context.Context,
	req *RetryTaskReq) (
	err error, resp *BaseResp) {

	Logger.WithContext(ctx).Debug(
		"UrchinClient:RetryTask start.")

	reqBody, err := json.Marshal(req)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"json.Marshal failed.",
			" err: ", err)
		return err, resp
	}
	Logger.WithContext(ctx).Debug(
		"request: ", string(reqBody))

	resp = new(BaseResp)
	err, respBody := Do(
		ctx,
		u.addr+UrchinClientRetryTaskInterface,
		http.MethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"http.Do failed.",
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

	if SuccessCode != resp.Code {
		Logger.WithContext(ctx).Error(
			"response failed.",
			" errCode: ", resp.Code,
			" errMessage: ", resp.Message)
		return errors.New(resp.Message), resp
	}

	Logger.WithContext(ctx).Debug(
		"UrchinClient:RetryTask finish.")
	return nil, resp
}
