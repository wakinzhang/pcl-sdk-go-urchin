package service

import (
	"crypto/tls"
	"encoding/json"
	"errors"
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
}

func (u *UrchinService) Init(addr string, reqTimeout int64, maxConnection int) {
	obs.DoLog(obs.LEVEL_DEBUG, "Function UrchinService:Init start.")

	u.addr = addr

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

	obs.DoLog(obs.LEVEL_DEBUG, "Function UrchinService:Init finish.")
}

func (u *UrchinService) CreateInitiateMultipartUploadSignedUrl(
	interf string,
	req *CreateInitiateMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateInitiateMultipartUploadSignedUrl start. interface: %s", interf)

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateInitiateMultipartUploadSignedUrlReq failed. error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateInitiateMultipartUploadSignedUrl failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateInitiateMultipartUploadSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateUploadPartSignedUrl(
	interf string, req *CreateUploadPartSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateUploadPartSignedUrl start.")
	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateUploadPartSignedUrlReq failed. error: ", err)
		return err, resp
	}
	resp = new(CreateSignedUrlResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
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
	interf string, req *CreateCompleteMultipartUploadSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateCompleteMultipartUploadSignedUrl start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateCompleteMultipartUploadSignedUrlReq failed. error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
		return err, resp
	}

	if SuccessCode != resp.Code {
		obs.DoLog(obs.LEVEL_ERROR,
			"CreateCompleteMultipartUploadSignedUrl failed. errCode: %d, errMessage: %s",
			resp.Code, resp.Message)
		return errors.New(resp.Message), resp
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateCompleteMultipartUploadSignedUrl end.")
	return nil, resp
}

func (u *UrchinService) CreateNewFolderSignedUrl(
	interf string,
	req *CreateNewFolderSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateNewFolderSignedUrl start. interface: %s", interf)

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal CreateCompleteMultipartUploadSignedUrlReq failed. error: ", err)
		return err, resp
	}

	resp = new(CreateSignedUrlResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
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

func (u *UrchinService) CreateGetObjectSignedUrl(interf string) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: CreateGetObjectSignedUrl start. interface: %s", interf)

	resp = new(CreateSignedUrlResp)
	err, respBody := Post(u.addr+interf, nil, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
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

func (u *UrchinService) CreateListObjectsSignedUrl(
	interf string, req *CreateListObjectsSignedUrlReq) (
	err error, resp *CreateSignedUrlResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: CreateListObjectsSignedUrl start.")
	reqBody, err := json.Marshal(req)
	resp = new(CreateSignedUrlResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
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

func (u *UrchinService) UploadObject(
	interf string, req *UploadObjectReq) (
	err error, resp *UploadObjectResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: UploadObject start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal UploadObjectReq failed. error: ", err)
		return err, resp
	}

	resp = new(UploadObjectResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
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

func (u *UrchinService) DownloadObject(
	interf string, req *DownloadObjectReq) (
	err error, resp *DownloadObjectResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: DownloadObject start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal DownloadObjectReq failed. error: ", err)
		return err, resp
	}

	resp = new(DownloadObjectResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
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

func (u *UrchinService) MigrateObject(
	interf string, req *MigrateObjectReq) (
	err error, resp *MigrateObjectResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: MigrateObject start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal MigrateObjectReq failed. error: ", err)
		return err, resp
	}

	resp = new(MigrateObjectResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
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

func (u *UrchinService) FinishTask(
	interf string, req *FinishTaskReq) (
	err error, resp *BaseResp) {

	obs.DoLog(obs.LEVEL_DEBUG, "Func: FinishTask start.")

	reqBody, err := json.Marshal(req)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"Marshal FinishTaskReq failed. error: ", err)
		return err, resp
	}

	resp = new(BaseResp)
	err, respBody := Post(u.addr+interf, reqBody, u.urchinClient)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "Post failed. error: ", err)
		return err, resp
	}

	err = json.Unmarshal(respBody, resp)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"json.Unmarshal failed. error: ", err)
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
