package common

import (
	. "github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

func Post(
	url string,
	reqBody []byte,
	timeout time.Duration,
	transport *http.Transport) (err error, respBody []byte) {

	DoLog(LEVEL_DEBUG, "Func: Post start. req: ", string(reqBody))
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
	respBuf, err := client.Post(
		url,
		"application/json; charset=UTF-8",
		strings.NewReader(string(reqBody)))
	if err != nil {
		DoLog(LEVEL_ERROR, "request failed."+
			" url:", url,
			" reqBody: ", string(reqBody),
			" error:", err)
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {
			DoLog(LEVEL_ERROR, "io.ReadCloser.Close failed. error:", err)
		}
	}(respBuf.Body)

	respBody, err = ioutil.ReadAll(respBuf.Body)
	if err != nil {
		DoLog(LEVEL_ERROR, "response failed. url:", url, " err:", err)
		return err, respBody
	}
	DoLog(LEVEL_DEBUG, "Func: Post end. respBody: ", string(respBody))
	return nil, respBody
}

func Get(
	url string,
	timeout time.Duration,
	transport *http.Transport) (err error, respBody []byte) {

	DoLog(LEVEL_DEBUG, "Func: Get start. url: ", url)
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
	respBuf, err := client.Get(url)
	if err != nil {
		DoLog(LEVEL_ERROR, "request failed. url:", url, " error:", err)
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {
			DoLog(LEVEL_ERROR, "io.ReadCloser.Close failed. error:", err)
		}
	}(respBuf.Body)

	respBody, err = ioutil.ReadAll(respBuf.Body)
	if err != nil {
		DoLog(LEVEL_ERROR, "response failed. url:", url, " err:", err)
		return err, respBody
	}
	DoLog(LEVEL_DEBUG, "Func: Get end. respBody: ", string(respBody))
	return nil, respBody
}
