package service

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/google/go-querystring/query"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	"net"
	"net/http"
	"time"
)

type UrchinService struct {
	addr         string
	urchinClient *http.Client
	header       http.Header
}

func (u *UrchinService) Init(
	address string,
	reqTimeout int64,
	maxConnection int) {

	obs.DoLog(obs.LEVEL_DEBUG, "Function UrchinService:Init start.")

	u.addr = address

	timeout := time.Duration(reqTimeout) * time.Second

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     60 * time.Second,
		MaxIdleConnsPerHost: maxConnection,
	}

	u.urchinClient = &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}

	userId := "test"
	token := "test"
	u.header = make(http.Header)
	u.header.Add(UrchinServiceHeaderUserId, userId)
	u.header.Add(UrchinServiceHeaderToken, token)

	obs.DoLog(obs.LEVEL_DEBUG, "Function UrchinService:Init finish.")
}

func (u *UrchinService) CreateInitiateMultipartUploadSignedUrl(
	req *CreateInitiateMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateInitiateMultipartUploadSignedUrl start."+
			" interface: %s",
		UrchinServiceCreateInitiateMultipartUploadSignedUrlInterface)

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateInitiateMultipartUploadSignedUrlReq failed."+
				" error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		u.addr+UrchinServiceCreateInitiateMultipartUploadSignedUrlInterface,
		HttpMethodPost,
		u.header,
		reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateInitiateMultipartUploadSignedUrl failed."+
				" errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateInitiateMultipartUploadSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateUploadPartSignedUrl(req *CreateUploadPartSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateUploadPartSignedUrl start.")
	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateUploadPartSignedUrlReq failed. error: ", err)
		return err, resp
	}
	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		u.addr+UrchinServiceCreateUploadPartSignedUrlInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateUploadPartSignedUrl failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateUploadPartSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateCompleteMultipartUploadSignedUrl(
	req *CreateCompleteMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateCompleteMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateCompleteMultipartUploadSignedUrlReq failed."+
				" error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		u.addr+UrchinServiceCreateCompleteMultipartUploadSignedUrlInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateCompleteMultipartUploadSignedUrl failed."+
				" errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateCompleteMultipartUploadSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateAbortMultipartUploadSignedUrl(
	req *CreateAbortMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateAbortMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateAbortMultipartUploadSignedUrlReq failed."+
				" error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		u.addr+UrchinServiceCreateAbortMultipartUploadSignedUrlInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateAbortMultipartUploadSignedUrl failed."+
				" errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateAbortMultipartUploadSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateNewFolderSignedUrl(req *CreateNewFolderSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateNewFolderSignedUrl start. interface: %s",
		UrchinServiceCreateNewFolderSignedUrlInterface)

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateNewFolderSignedUrlReq failed. error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		u.addr+UrchinServiceCreateNewFolderSignedUrlInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateNewFolderSignedUrl failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateNewFolderSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateGetObjectMetadataSignedUrl(
	req *CreateGetObjectMetadataSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateGetObjectMetadataSignedUrl start. interface: %s",
		UrchinServiceCreateGetObjectMetadataSignedUrlInterface)

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateGetObjectMetadataSignedUrlReq failed. error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		u.addr+UrchinServiceCreateGetObjectMetadataSignedUrlInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateGetObjectMetadataSignedUrl failed."+
				" errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateGetObjectMetadataSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateGetObjectSignedUrl(req *CreateGetObjectSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateGetObjectSignedUrl start. interface: %s",
		UrchinServiceCreateGetObjectSignedUrlInterface)

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateGetObjectSignedUrlReq failed. error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		u.addr+UrchinServiceCreateGetObjectSignedUrlInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateGetObjectSignedUrl failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateGetObjectSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateListObjectsSignedUrl(req *CreateListObjectsSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateListObjectsSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateListObjectsSignedUrlReq failed. error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Do(
		u.addr+UrchinServiceCreateListObjectsSignedUrlInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateListObjectsSignedUrl failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateListObjectsSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) GetIpfsToken(req *GetIpfsTokenReq) (
	err error, resp *GetIpfsTokenResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: GetIpfsToken start.")
	values, err := query.Values(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "query.Values failed. error: ", err)
		return err, resp
	}
	resp = new(GetIpfsTokenResp)
	err, respBody := Do(
		u.addr+UrchinServiceGetIpfsTokenInterface+"?"+values.Encode(),
		HttpMethodGet,
		u.header,
		nil,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "HttpDo failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"GetIpfsToken failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: GetIpfsToken end.")
	return nil, resp
}

func (u *UrchinService) UploadObject(req *UploadObjectReq) (
	err error, resp *UploadObjectResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: UploadObject start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal UploadObjectReq failed. error: ", err)
		return err, resp
	}

	resp = new(UploadObjectResp)
	err, respBody := Do(
		u.addr+UrchinServiceUploadObjectInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"UploadObject failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: UploadObject end.")
	return nil, resp
}

func (u *UrchinService) UploadFile(req *UploadFileReq) (
	err error, resp *UploadFileResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: UploadFile start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal UploadFileReq failed. error: ", err)
		return err, resp
	}

	resp = new(UploadFileResp)
	err, respBody := Do(
		u.addr+UrchinServiceUploadFileInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"UploadFile failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: UploadFile end.")
	return nil, resp
}

func (u *UrchinService) GetObject(req *GetObjectReq) (
	err error, resp *GetObjectResp) {
	values, err := query.Values(req)
	obs.DoLog(obs.LEVEL_DEBUG, "Func: GetObject start.")
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "query.Values failed. error: ", err)
		return err, resp
	}
	resp = new(GetObjectResp)
	err, respBody := Do(
		u.addr+UrchinServiceGetObjectInterface+"?"+values.Encode(),
		HttpMethodGet,
		u.header,
		nil,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "HttpDo failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"GetObject failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: GetObject end.")
	return nil, resp
}

func (u *UrchinService) DownloadObject(req *DownloadObjectReq) (
	err error, resp *DownloadObjectResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: DownloadObject start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal DownloadObjectReq failed. error: ", err)
		return err, resp
	}

	resp = new(DownloadObjectResp)
	err, respBody := Do(
		u.addr+UrchinServiceDownloadObjectInterface,
		HttpMethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"DownloadObject failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: DownloadObject end.")
	return nil, resp
}

func (u *UrchinService) DownloadFile(req *DownloadFileReq) (
	err error, resp *DownloadFileResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: DownloadFile start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal DownloadFileReq failed. error: ", err)
		return err, resp
	}

	resp = new(DownloadFileResp)
	err, respBody := Do(
		u.addr+UrchinServiceDownloadFileInterface,
		HttpMethodPost,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"DownloadFile failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: DownloadFile end.")
	return nil, resp
}

func (u *UrchinService) MigrateObject(req *MigrateObjectReq) (
	err error, resp *MigrateObjectResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: MigrateObject start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal MigrateObjectReq failed. error: ", err)
		return err, resp
	}

	resp = new(MigrateObjectResp)
	err, respBody := Do(
		u.addr+UrchinServiceMigrateObjectInterface,
		HttpMethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"MigrateObject failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: MigrateObject end.")
	return nil, resp
}

func (u *UrchinService) PutObjectDeployment(req *PutObjectDeploymentReq) (
	err error, resp *BaseResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: PutObjectDeployment start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal PutObjectDeploymentReq failed. error: ", err)
		return err, resp
	}

	resp = new(BaseResp)
	err, respBody := Do(
		u.addr+UrchinServicePutObjectDeploymentInterface,
		HttpMethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "HttpDo failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"PutObjectDeployment failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: PutObjectDeployment end.")
	return nil, resp
}

func (u *UrchinService) GetTask(req *GetTaskReq) (
	err error, resp *GetTaskResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: GetTask start.")
	values, err := query.Values(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "query.Values failed. error: ", err)
		return err, resp
	}
	resp = new(GetTaskResp)
	err, respBody := Do(
		u.addr+UrchinServiceGetTaskInterface+"?"+values.Encode(),
		HttpMethodGet,
		u.header,
		nil,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "HttpDo failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: GetTask end.")
	return nil, resp
}

func (u *UrchinService) FinishTask(req *FinishTaskReq) (
	err error, resp *BaseResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: FinishTask start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Marshal FinishTaskReq failed. error: ", err)
		return err, resp
	}

	resp = new(BaseResp)
	err, respBody := Do(
		u.addr+UrchinServiceFinishTaskInterface,
		HttpMethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"FinishTask failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: FinishTask end.")
	return nil, resp
}

func (u *UrchinService) RetryTask(req *RetryTaskReq) (
	err error, resp *BaseResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: RetryTask start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Marshal RetryTaskReq failed. error: ", err)
		return err, resp
	}

	resp = new(BaseResp)
	err, respBody := Do(
		u.addr+UrchinServiceRetryTaskInterface,
		HttpMethodPut,
		u.header,
		reqBody,
		u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"RetryTask failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: RetryTask end.")
	return nil, resp
}
