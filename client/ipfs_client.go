package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/urchinfs/go-urchin2-sdk/ipfs_api"
	"io"
	"net"
	"net/http"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/module"
	"strings"
	"time"
)

type IPFS struct {
	user       string
	pass       string
	passMagic  string
	endPoint   string
	ipfsClient *ipfs_api.HttpClient
	token      string
}

func (o *IPFS) Init(
	ctx context.Context,
	user, pass, passMagic, endPoint string) (err error) {

	Logger.WithContext(ctx).Debug(
		"Function IPFS:Init start.",
		" user: ", "***",
		" pass: ", "***",
		" passMagic: ", "***",
		" endPoint: ", endPoint)
	o.user = user
	o.pass = pass
	o.passMagic = passMagic
	o.endPoint = endPoint
	Logger.WithContext(ctx).Debug(
		"Function IPFS:Init finish.")
	return nil
}

func (o *IPFS) GetToken(ctx context.Context) (err error, token string) {
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

	timeout := time.Duration(10) * time.Second
	IPFSTransport := &http.Transport{
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
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     60 * time.Second,
		MaxIdleConnsPerHost: 10,
	}
	client := &http.Client{
		Timeout:   timeout,
		Transport: IPFSTransport,
	}
	resp, err := client.Post(
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
