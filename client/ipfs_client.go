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

	Logger.WithContext(ctx).Debug(
		"Function IPFSClient:Init start.",
		" user: ", "***",
		" pass: ", "***",
		" passMagic: ", "***",
		" endPoint: ", endPoint,
		" reqTimeout: ", reqTimeout,
		" maxConnection: ", maxConnection)
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

	Logger.WithContext(ctx).Debug(
		"Function IPFSClient:Init finish.")
	return nil
}

func (o *IPFSClient) GetToken(
	ctx context.Context) (
	err error, token string) {

	Logger.WithContext(ctx).Debug(
		"Func: GetToken start.")
	t := time.Now()
	timeStamp := fmt.Sprintf("%d%d%d", t.Year(), t.Month(), t.Day())
	rawPass := o.pass + ":" + o.passMagic + ":" + timeStamp
	Logger.Debug("Func: GetToken rawPass: ", rawPass)
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
		Logger.WithContext(ctx).Error(
			"GetToken request failed.",
			" url:", o.endPoint+IPFSAuthInterface,
			" request: ", string(reqBody),
			" error: ", err)
		return err, token
	}
	defer func(body io.ReadCloser) {
		_err := body.Close()
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"io.ReadCloser.Close failed. error:", _err)
		}
	}(resp.Body)

	if resp.StatusCode == http.StatusOK {
		token = resp.Header.Get(IPFSAuthInterfaceResponseHeaderTokenKey)
	} else {
		respBody, _err := io.ReadAll(resp.Body)
		if nil != _err {
			Logger.WithContext(ctx).Error(
				"read response body failed.",
				" url: %s, error: %v", o.endPoint+IPFSAuthInterface, _err)
		}
		Logger.WithContext(ctx).Error(
			"GetToken response failed.",
			" url:", o.endPoint+IPFSAuthInterface,
			" request: ", string(reqBody),
			" StatusCode: ", resp.StatusCode,
			" respBody: ", string(respBody))
		return errors.New("ipfs response failed status code"), token
	}

	Logger.WithContext(ctx).Debug(
		"Func: GetToken end.")
	return nil, token
}

func (o *IPFSClient) ListObjects(
	ctx context.Context,
	path string) (output []*ipfs_api.LsLink, err error) {

	Logger.WithContext(ctx).Debug(
		"IPFSClient:ListObjects start.",
		" path: ", path)
	err, o.token = o.GetToken(ctx)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"IPFSClient.GetToken failed. error: ", err)
		return output, err
	}
	o.ipfsSDK = ipfs_api.NewClient(o.endPoint, o.token)
	output, err = o.ipfsSDK.List(path)
	if nil != err {
		Logger.WithContext(ctx).Error(
			"ipfsSDK.List failed. error: ", err)
		return output, err
	}
	Logger.WithContext(ctx).Debug(
		"IPFSClient:ListObjects finish.")
	return output, nil
}
