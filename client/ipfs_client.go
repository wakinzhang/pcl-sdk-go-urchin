package client

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/urchinfs/go-urchin2-sdk/ipfs_api"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"go.uber.org/zap"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type IPFSClient struct {
	user       string
	pass       string
	passMagic  string
	endPoint   string
	ipfsSDK    *ipfs_api.HttpClient
	ipfsClient *retryablehttp.Client
	token      string
}

func (o *IPFSClient) Init(
	ctx context.Context,
	user,
	pass,
	passMagic,
	endPoint string,
	reqTimeout,
	maxConnection int32) (err error) {

	InfoLogger.WithContext(ctx).Debug(
		"Function IPFSClient:Init start.",
		zap.String("endPoint", endPoint),
		zap.Int32("reqTimeout", reqTimeout),
		zap.Int32("maxConnection", maxConnection))
	o.user = user
	o.pass = pass
	o.passMagic = passMagic
	o.endPoint = endPoint

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
	o.ipfsClient = retryablehttp.NewClient()
	o.ipfsClient.RetryMax = 3
	o.ipfsClient.RetryWaitMin = 1 * time.Second
	o.ipfsClient.RetryWaitMax = 5 * time.Second
	o.ipfsClient.HTTPClient.Timeout = timeout
	o.ipfsClient.HTTPClient.Transport = transport

	InfoLogger.WithContext(ctx).Debug(
		"Function IPFSClient:Init finish.")
	return nil
}

func (o *IPFSClient) GetToken(
	ctx context.Context) (
	err error, token string) {

	InfoLogger.WithContext(ctx).Debug(
		"Func: GetToken start.")
	t := time.Now()
	timeStamp := fmt.Sprintf("%d%d%d", t.Year(), t.Month(), t.Day())
	rawPass := o.pass + ":" + o.passMagic + ":" + timeStamp

	InfoLogger.WithContext(ctx).Debug(
		"Func: GetToken",
		zap.String("rawPass", rawPass))

	hash := sha256.Sum256([]byte(rawPass))
	reqPass := hex.EncodeToString(hash[:])

	req := new(IPFSGetTokenReq)
	req.Pass = reqPass
	req.User = o.user
	req.ExpireTime = 3600
	reqBody, _ := json.Marshal(req)

	resp, err := o.ipfsClient.Post(
		o.endPoint+IPFSAuthInterface,
		"application/json; charset=UTF-8",
		strings.NewReader(string(reqBody)))
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"GetToken request failed.",
			zap.String("url", o.endPoint+IPFSAuthInterface),
			zap.String("request", string(reqBody)),
			zap.Error(err))
		return err, token
	}
	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"io.ReadCloser.Close failed.",
				zap.Error(_err))
		}
	}(resp.Body)

	if resp.StatusCode == http.StatusOK {
		token = resp.Header.Get(IPFSAuthInterfaceResponseHeaderTokenKey)
	} else {
		respBody, _err := io.ReadAll(resp.Body)
		if nil != _err {
			ErrorLogger.WithContext(ctx).Error(
				"read response body failed.",
				zap.String("url", o.endPoint+IPFSAuthInterface),
				zap.Error(_err))
		}
		ErrorLogger.WithContext(ctx).Error(
			"GetToken response failed.",
			zap.String("url", o.endPoint+IPFSAuthInterface),
			zap.String("request", string(reqBody)),
			zap.Int("statusCode", resp.StatusCode),
			zap.String("respBody", string(respBody)))
		return errors.New("ipfs response failed status code"), token
	}

	InfoLogger.WithContext(ctx).Debug(
		"Func: GetToken end.")
	return nil, token
}

func (o *IPFSClient) ListObjects(
	ctx context.Context,
	path string) (output []*ipfs_api.LsLink, err error) {

	InfoLogger.WithContext(ctx).Debug(
		"IPFSClient:ListObjects start.",
		zap.String("path", path))
	err, o.token = o.GetToken(ctx)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"IPFSClient.GetToken failed.",
			zap.Error(err))
		return output, err
	}
	o.ipfsSDK = ipfs_api.NewClient(o.endPoint, o.token)
	output, err = o.ipfsSDK.List(path)
	if nil != err {
		ErrorLogger.WithContext(ctx).Error(
			"ipfsSDK.List failed.",
			zap.Error(err))
		return output, err
	}
	InfoLogger.WithContext(ctx).Debug(
		"IPFSClient:ListObjects finish.")
	return output, nil
}
