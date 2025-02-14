package common

import (
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

func Post(url string, reqBody []byte, client *http.Client) (err error, respBody []byte) {
	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: Post start. url: %s, reqBody: %s", url, string(reqBody))

	respBuf, err := client.Post(
		url,
		"application/json; charset=UTF-8",
		strings.NewReader(string(reqBody)))
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"request failed.  url: %s, reqBody: %s, error: %v",
			url, string(reqBody), err)
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "io.ReadCloser.Close failed. error: %v", err)
		}
	}(respBuf.Body)

	respBody, err = ioutil.ReadAll(respBuf.Body)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "response failed. url: %s, error: %v", url, err)
		return err, respBody
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Func: Post end. respBody: %s", string(respBody))
	return nil, respBody
}

func Get(url string, client *http.Client) (err error, respBody []byte) {
	obs.DoLog(obs.LEVEL_DEBUG, "Func: Get start. url: %s", url)
	respBuf, err := client.Get(url)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "request failed. url: %s, error: %v", url, err)
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "io.ReadCloser.Close failed. error: %v", err)
		}
	}(respBuf.Body)

	respBody, err = ioutil.ReadAll(respBuf.Body)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "response failed. url: %s, error: %v", err)
		return err, respBody
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Func: Get end. respBody: %s", string(respBody))
	return nil, respBody
}

func Do(url, method string, reqBody []byte, client *http.Client) (err error, respBody []byte) {
	obs.DoLog(obs.LEVEL_DEBUG,
		"Func: GetWithBody start. url: %s, reqBody: %s", url, string(reqBody))

	request, _ := http.NewRequest(method, url, strings.NewReader(string(reqBody)))
	resp, err := client.Do(request)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"request failed. url: %s, reqBody: %s, error: %v",
			url, string(reqBody), err)
		return err, respBody
	}
	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR, "io.ReadCloser.Close failed. error: %v", err)
		}
	}(resp.Body)

	respBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "response failed. url: %s, error: %v", url, err)
		return err, respBody
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Func: GetWithBody end. respBody: %s", string(respBody))
	return nil, respBody
}
